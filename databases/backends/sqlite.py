import getpass
import logging
import typing
import uuid

import aiosqlite
from sqlalchemy.dialects.sqlite import pysqlite
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import ClauseElement
from sqlalchemy.types import TypeEngine

from databases.core import DatabaseURL
from databases.interfaces import ConnectionBackend, DatabaseBackend, TransactionBackend

logger = logging.getLogger("databases")

_result_processors = {}  # type: dict


class SQLiteBackend(DatabaseBackend):
    def __init__(self, database_url: typing.Union[str, DatabaseURL]) -> None:
        self._database_url = DatabaseURL(database_url)
        self._dialect = pysqlite.dialect(paramstyle="qmark")
        self._pool = SQLitePool(database_url)

    async def connect(self) -> None:
        pass
        # assert self._pool is None, "DatabaseBackend is already running"
        # self._pool = await aiomysql.create_pool(
        #     host=self._database_url.hostname,
        #     port=self._database_url.port or 3306,
        #     user=self._database_url.username or getpass.getuser(),
        #     password=self._database_url.password,
        #     db=self._database_url.database,
        #     autocommit=True,
        # )

    async def disconnect(self) -> None:
        pass
        #assert self._pool is not None, "DatabaseBackend is not running"
        #self._pool.close()
        #await self._pool.wait_closed()
        #self._pool = None

    def connection(self) -> "SQLiteConnection":
        return SQLiteConnection(self._pool, self._dialect)


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


class SQLitePool:
    def __init__(self, url):
        self._url = url

    async def acquire(self):
        connection = aiosqlite.connect(
            database=self._url.database,
            isolation_level=None,
        )
        await connection.__aenter__()
        return connection

    async def release(self, connection):
        await connection.__aexit__(None, None, None)


class SQLiteConnection(ConnectionBackend):
    def __init__(self, pool: SQLitePool, dialect: Dialect):
        self._pool = pool
        self._dialect = dialect
        self._connection = None

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

        async with self._connection.execute(query, args) as cursor:
            rows = await cursor.fetchall()
            return [Record(row, result_columns, self._dialect) for row in rows]

    async def fetch_one(self, query: ClauseElement) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        query, args, result_columns = self._compile(query)

        async with self._connection.execute(query, args) as cursor:
            row = await cursor.fetchone()
            return Record(row, result_columns, self._dialect)

    async def execute(self, query: ClauseElement, values: dict = None) -> None:
        assert self._connection is not None, "Connection is not acquired"
        if values is not None:
            query = query.values(values)
        query, args, result_columns = self._compile(query)
        cursor = await self._connection.execute(query, args)
        await cursor.close()

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        assert self._connection is not None, "Connection is not acquired"
        for value in values:
            await self.execute(query, value)

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        assert self._connection is not None, "Connection is not acquired"
        query, args, result_columns = self._compile(query)
        cursor = await self._connection.cursor()
        async with self._connection.execute(query, args) as cursor:
            async for row in cursor:
                yield Record(row, result_columns, self._dialect)

    def transaction(self) -> TransactionBackend:
        return SQLiteTransaction(self)

    def _compile(self, query: ClauseElement) -> typing.Tuple[str, list, tuple]:
        compiled = query.compile(dialect=self._dialect)
        args = []
        for key, raw_val in compiled.construct_params().items():
            if key in compiled._bind_processors:
                val = compiled._bind_processors[key](raw_val)
            else:
                val = raw_val
            args.append(val)
        logger.debug(compiled.string, args)
        return compiled.string, args, compiled._result_columns


class SQLiteTransaction(TransactionBackend):
    def __init__(self, connection: SQLiteConnection):
        self._connection = connection
        self._is_root = False
        self._savepoint_name = ""

    async def start(self, is_root: bool) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        self._is_root = is_root
        if self._is_root:
            cursor = await self._connection._connection.execute("BEGIN")
            await cursor.close()
        else:
            id = str(uuid.uuid4()).replace("-", "_")
            self._savepoint_name = f"STARLETTE_SAVEPOINT_{id}"
            cursor = await self._connection._connection.execute(f"SAVEPOINT {self._savepoint_name}")
            await cursor.close()

    async def commit(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        if self._is_root:
            cursor = await self._connection._connection.execute("COMMIT")
            await cursor.close()
        else:
            cursor = await self._connection._connection.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")
            await cursor.close()

    async def rollback(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        if self._is_root:
            cursor = await self._connection._connection.execute("ROLLBACK")
            await cursor.close()
        else:
            cursor = await self._connection._connection.execute(f"ROLLBACK TO SAVEPOINT {self._savepoint_name}")
            await cursor.close()
