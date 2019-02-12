import getpass
import logging
import typing
import uuid

import aiomysql
from sqlalchemy.dialects.mysql import pymysql
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import ClauseElement
from sqlalchemy.types import TypeEngine

from databases.core import DatabaseURL
from databases.interfaces import ConnectionBackend, DatabaseBackend, TransactionBackend

logger = logging.getLogger("databases")

_result_processors = {}  # type: dict


class MySQLBackend(DatabaseBackend):
    def __init__(self, database_url: typing.Union[str, DatabaseURL]) -> None:
        self._database_url = DatabaseURL(database_url)
        self._dialect = pymysql.dialect(paramstyle="pyformat")
        self._pool = None

    async def connect(self) -> None:
        assert self._pool is None, "DatabaseBackend is already running"
        self._pool = await aiomysql.create_pool(
            host=self._database_url.hostname,
            port=self._database_url.port or 3306,
            user=self._database_url.username or getpass.getuser(),
            password=self._database_url.password,
            db=self._database_url.database,
            autocommit=True,
        )

    async def disconnect(self) -> None:
        assert self._pool is not None, "DatabaseBackend is not running"
        self._pool.close()
        await self._pool.wait_closed()
        self._pool = None

    def connection(self) -> "MySQLConnection":
        assert self._pool is not None, "DatabaseBackend is not running"
        return MySQLConnection(self._pool, self._dialect)


class Record:
    def __init__(self, row: tuple, result_columns: tuple, dialect: Dialect) -> None:
        self._row = row
        self._result_columns = result_columns
        self._dialect = dialect
        self._column_map = {
            column_name: (idx, datatype)
            for idx, (column_name, _, _, datatype) in enumerate(self._result_columns)
        }

    def __getitem__(self, key: str) -> typing.Any:
        idx, datatype = self._column_map[key]
        raw = self._row[idx]
        try:
            processor = _result_processors[datatype]
        except KeyError:
            processor = datatype.result_processor(self._dialect, None)
            _result_processors[datatype] = processor

        if processor is not None:
            return processor(raw)
        return raw


class MySQLConnection(ConnectionBackend):
    def __init__(self, pool: aiomysql.pool.Pool, dialect: Dialect):
        self._pool = pool
        self._dialect = dialect
        self._connection = None  # type: typing.Optional[aiomysql.Connection]

    async def acquire(self) -> None:
        assert self._connection is None, "Connection is already acquired"
        self._connection = await self._pool.acquire()

    async def release(self) -> None:
        assert self._connection is not None, "Connection is not acquired"
        await self._pool.release(self._connection)
        self._connection = None

    async def fetch_all(self, query: ClauseElement) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        query, args, result_columns = self._compile(query)
        cursor = await self._connection.cursor()
        try:
            await cursor.execute(query, args)
            rows = await cursor.fetchall()
            return [Record(row, result_columns, self._dialect) for row in rows]
        finally:
            await cursor.close()

    async def fetch_one(self, query: ClauseElement) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        query, args, result_columns = self._compile(query)
        cursor = await self._connection.cursor()
        try:
            await cursor.execute(query, args)
            row = await cursor.fetchone()
            return Record(row, result_columns, self._dialect)
        finally:
            await cursor.close()

    async def execute(self, query: ClauseElement, values: dict = None) -> None:
        assert self._connection is not None, "Connection is not acquired"
        if values is not None:
            query = query.values(values)
        query, args, result_columns = self._compile(query)
        cursor = await self._connection.cursor()
        try:
            await cursor.execute(query, args)
        finally:
            await cursor.close()

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        assert self._connection is not None, "Connection is not acquired"
        cursor = await self._connection.cursor()
        try:
            for item in values:
                single_query = query.values(item)
                single_query, args, result_columns = self._compile(single_query)
                await cursor.execute(single_query, args)
        finally:
            await cursor.close()

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        assert self._connection is not None, "Connection is not acquired"
        query, args, result_columns = self._compile(query)
        cursor = await self._connection.cursor()
        try:
            await cursor.execute(query, args)
            async for row in cursor:
                yield Record(row, result_columns, self._dialect)
        finally:
            await cursor.close()

    def transaction(self) -> TransactionBackend:
        return MySQLTransaction(self)

    def _compile(self, query: ClauseElement) -> typing.Tuple[str, list, tuple]:
        compiled = query.compile(dialect=self._dialect)
        args = compiled.construct_params()
        logger.debug(compiled.string, args)
        for key, val in args.items():
            if key in compiled._bind_processors:
                args[key] = compiled._bind_processors[key](val)
        return compiled.string, args, compiled._result_columns


class MySQLTransaction(TransactionBackend):
    def __init__(self, connection: MySQLConnection):
        self._connection = connection
        self._is_root = False
        self._savepoint_name = ""

    async def start(self, is_root: bool) -> None:
        self._is_root = is_root
        if self._is_root:
            await self._connection._connection.begin()
        else:
            id = str(uuid.uuid4()).replace("-", "_")
            self._savepoint_name = f"STARLETTE_SAVEPOINT_{id}"
            await self._connection.execute(f"SAVEPOINT {self._savepoint_name}")

    async def commit(self) -> None:
        if self._is_root:  # pragma: no cover
            # In test cases the root transaction is never committed,
            # since we *always* wrap the test case up in a transaction
            # and rollback to a clean state at the end.
            await self._connection._connection.commit()
        else:
            await self._connection.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")

    async def rollback(self) -> None:
        if self._is_root:
            await self._connection._connection.rollback()
        else:
            await self._connection.execute(
                f"ROLLBACK TO SAVEPOINT {self._savepoint_name}"
            )
