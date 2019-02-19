import logging
import typing
import uuid

import aiosqlite
from sqlalchemy.dialects.sqlite import pysqlite
from sqlalchemy.engine.interfaces import Dialect, ExecutionContext
from sqlalchemy.engine.result import ResultMetaData, RowProxy
from sqlalchemy.sql import ClauseElement
from sqlalchemy.types import TypeEngine

from databases.core import DatabaseURL
from databases.interfaces import ConnectionBackend, DatabaseBackend, TransactionBackend

logger = logging.getLogger("databases")


class SQLiteBackend(DatabaseBackend):
    def __init__(self, database_url: DatabaseURL) -> None:
        self._database_url = database_url
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
        # assert self._pool is not None, "DatabaseBackend is not running"
        # self._pool.close()
        # await self._pool.wait_closed()
        # self._pool = None

    def connection(self) -> "SQLiteConnection":
        return SQLiteConnection(self._pool, self._dialect)


class SQLitePool:
    def __init__(self, url: DatabaseURL) -> None:
        self._url = url

    async def acquire(self) -> aiosqlite.Connection:
        connection = aiosqlite.connect(
            database=self._url.database, isolation_level=None
        )
        await connection.__aenter__()
        return connection

    async def release(self, connection: aiosqlite.Connection) -> None:
        await connection.__aexit__(None, None, None)


class CompilationContext:
    def __init__(self, context: ExecutionContext):
        self.context = context


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

    async def fetch_all(self, query: ClauseElement) -> typing.List[RowProxy]:
        assert self._connection is not None, "Connection is not acquired"
        query, args, context = self._compile(query)

        async with self._connection.execute(query, args) as cursor:
            rows = await cursor.fetchall()
            metadata = ResultMetaData(context, cursor.description)
            return [
                RowProxy(metadata, row, metadata._processors, metadata._keymap)
                for row in rows
            ]

    async def fetch_one(self, query: ClauseElement) -> RowProxy:
        assert self._connection is not None, "Connection is not acquired"
        query, args, context = self._compile(query)

        async with self._connection.execute(query, args) as cursor:
            row = await cursor.fetchone()
            metadata = ResultMetaData(context, cursor.description)
            return RowProxy(metadata, row, metadata._processors, metadata._keymap)

    async def execute(self, query: ClauseElement, values: dict = None) -> None:
        assert self._connection is not None, "Connection is not acquired"
        if values is not None:
            query = query.values(values)
        query, args, context = self._compile(query)
        cursor = await self._connection.execute(query, args)
        await cursor.close()
        return cursor.lastrowid

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        assert self._connection is not None, "Connection is not acquired"
        for value in values:
            await self.execute(query, value)

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        assert self._connection is not None, "Connection is not acquired"
        query, args, context = self._compile(query)
        cursor = await self._connection.cursor()
        async with self._connection.execute(query, args) as cursor:
            metadata = ResultMetaData(context, cursor.description)
            async for row in cursor:
                yield RowProxy(metadata, row, metadata._processors, metadata._keymap)

    def transaction(self) -> TransactionBackend:
        return SQLiteTransaction(self)

    def _compile(
        self, query: ClauseElement
    ) -> typing.Tuple[str, list, CompilationContext]:
        compiled = query.compile(dialect=self._dialect)
        args = []
        for key, raw_val in compiled.construct_params().items():
            if key in compiled._bind_processors:
                val = compiled._bind_processors[key](raw_val)
            else:
                val = raw_val
            args.append(val)

        execution_context = self._dialect.execution_ctx_cls()
        execution_context.dialect = self._dialect
        execution_context.result_column_struct = (
            compiled._result_columns,
            compiled._ordered_columns,
            compiled._textual_ordered_columns,
        )

        logger.debug(compiled.string, args)
        return compiled.string, args, CompilationContext(execution_context)


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
            cursor = await self._connection._connection.execute(
                f"SAVEPOINT {self._savepoint_name}"
            )
            await cursor.close()

    async def commit(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        if self._is_root:
            cursor = await self._connection._connection.execute("COMMIT")
            await cursor.close()
        else:
            cursor = await self._connection._connection.execute(
                f"RELEASE SAVEPOINT {self._savepoint_name}"
            )
            await cursor.close()

    async def rollback(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        if self._is_root:
            cursor = await self._connection._connection.execute("ROLLBACK")
            await cursor.close()
        else:
            cursor = await self._connection._connection.execute(
                f"ROLLBACK TO SAVEPOINT {self._savepoint_name}"
            )
            await cursor.close()
