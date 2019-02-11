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
from databases.interfaces import DatabaseBackend, DatabaseSession, DatabaseTransaction

logger = logging.getLogger("databases")

_result_processors = {}  # type: dict


class MySQLBackend(DatabaseBackend):
    def __init__(self, database_url: typing.Union[str, DatabaseURL]) -> None:
        self.database_url = DatabaseURL(database_url)
        self.dialect = self._get_dialect()
        self.pool = None

    def _get_dialect(self) -> Dialect:
        return pymysql.dialect(paramstyle="pyformat")

    async def connect(self) -> None:
        db = self.database_url
        self.pool = await aiomysql.create_pool(
            host=db.hostname,
            port=db.port or 3306,
            user=db.username or getpass.getuser(),
            password=db.password,
            db=db.database,
            autocommit=True,
        )

    async def disconnect(self) -> None:
        assert self.pool is not None, "DatabaseBackend is not running"
        self.pool.close()
        await self.pool.wait_closed()
        self.pool = None

    def session(self) -> "MySQLSession":
        assert self.pool is not None, "DatabaseBackend is not running"
        return MySQLSession(self.pool, self.dialect)


class Record:
    def __init__(self, row: dict, result_columns: tuple, dialect: Dialect) -> None:
        self._row = row
        self._result_columns = result_columns
        self._dialect = dialect
        self._column_map = {
            column_name: datatype
            for column_name, _, _, datatype in self._result_columns
        }

    def __getitem__(self, key: str) -> typing.Any:
        raw = self._row[key]

        try:
            datatype = self._column_map[key]
        except KeyError:
            return raw

        try:
            processor = _result_processors[datatype]
        except KeyError:
            processor = datatype.result_processor(self._dialect, None)
            _result_processors[datatype] = processor

        if processor is not None:
            return processor(raw)
        return raw


class MySQLSession(DatabaseSession):
    def __init__(self, pool: aiomysql.pool.Pool, dialect: Dialect):
        self.pool = pool
        self.dialect = dialect
        self.conn = None
        self.connection_holders = 0
        self.has_root_transaction = False

    def _compile(self, query: ClauseElement) -> typing.Tuple[str, list, tuple]:
        compiled = query.compile(dialect=self.dialect)
        args = compiled.construct_params()
        logger.debug(compiled.string, args)
        for key, val in args.items():
            if key in compiled._bind_processors:
                args[key] = compiled._bind_processors[key](val)
        return compiled.string, args, compiled._result_columns

    async def fetch_all(self, query: ClauseElement) -> typing.Any:
        query, args, result_columns = self._compile(query)

        conn = await self.acquire_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        try:
            await cursor.execute(query, args)
            rows = await cursor.fetchall()
            return [Record(row, result_columns, self.dialect) for row in rows]
        finally:
            await cursor.close()
            await self.release_connection()

    async def fetch_one(self, query: ClauseElement) -> typing.Any:
        query, args, result_columns = self._compile(query)

        conn = await self.acquire_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        try:
            await cursor.execute(query, args)
            row = await cursor.fetchone()
            return Record(row, result_columns, self.dialect)
        finally:
            await cursor.close()
            await self.release_connection()

    async def execute(self, query: ClauseElement, values: dict = None) -> None:
        if values is not None:
            query = query.values(values)
        query, args, result_columns = self._compile(query)

        conn = await self.acquire_connection()
        cursor = await conn.cursor()
        try:
            await cursor.execute(query, args)
        finally:
            await cursor.close()
            await self.release_connection()

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        conn = await self.acquire_connection()
        cursor = await conn.cursor()
        try:
            for item in values:
                single_query = query.values(item)
                single_query, args, result_columns = self._compile(single_query)
                await cursor.execute(single_query, args)
        finally:
            await cursor.close()
            await self.release_connection()

    def transaction(self) -> DatabaseTransaction:
        return MySQLTransaction(self)

    async def acquire_connection(self) -> aiomysql.Connection:
        """
        Either acquire a connection from the pool, or return the
        existing connection. Must be followed by a corresponding
        call to `release_connection`.
        """
        self.connection_holders += 1
        if self.conn is None:
            self.conn = await self.pool.acquire()
        return self.conn

    async def release_connection(self) -> None:
        self.connection_holders -= 1
        if self.connection_holders == 0:
            await self.pool.release(self.conn)
            self.conn = None

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        query, args, result_columns = self._compile(query)

        conn = await self.acquire_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)

        try:
            await cursor.execute(query, args)
            async for row in cursor:
                yield Record(row, result_columns, self.dialect)
        finally:
            await cursor.close()
            await self.release_connection()

    async def raw(
        self, query: ClauseElement, **kwargs: typing.Any
    ) -> typing.AsyncGenerator[typing.Any, None]:
        query, args, result_columns = self._compile(query.bindparams(**kwargs))

        conn = await self.acquire_connection()
        cursor = await conn.cursor(aiomysql.DictCursor)
        try:
            await cursor.execute(query, args)
            async for row in cursor:
                yield Record(row, result_columns, self.dialect)
        finally:
            await cursor.close()
            await self.release_connection()


class MySQLTransaction(DatabaseTransaction):
    def __init__(self, session: MySQLSession):
        self.session = session
        self.is_root = False

    async def start(self) -> None:
        if self.session.has_root_transaction is False:
            self.session.has_root_transaction = True
            self.is_root = True

        self.conn = await self.session.acquire_connection()
        if self.is_root:
            await self.conn.begin()
        else:
            id = str(uuid.uuid4()).replace("-", "_")
            self.savepoint_name = f"STARLETTE_SAVEPOINT_{id}"
            cursor = await self.conn.cursor()
            try:
                await cursor.execute(f"SAVEPOINT {self.savepoint_name}")
            finally:
                await cursor.close()

    async def commit(self) -> None:
        if self.is_root:  # pragma: no cover
            # In test cases the root transaction is never committed,
            # since we *always* wrap the test case up in a transaction
            # and rollback to a clean state at the end.
            await self.conn.commit()
            self.session.has_root_transaction = False
        else:
            cursor = await self.conn.cursor()
            try:
                await cursor.execute(f"RELEASE SAVEPOINT {self.savepoint_name}")
            finally:
                await cursor.close()
        await self.session.release_connection()

    async def rollback(self) -> None:
        if self.is_root:
            await self.conn.rollback()
            self.session.has_root_transaction = False
        else:
            cursor = await self.conn.cursor()
            try:
                await cursor.execute(f"ROLLBACK TO SAVEPOINT {self.savepoint_name}")
            finally:
                await cursor.close()
        await self.session.release_connection()
