import getpass
import logging
import typing
import uuid

import aiomysql
from sqlalchemy.dialects.mysql import pymysql
from sqlalchemy.engine.interfaces import Dialect, ExecutionContext
from sqlalchemy.engine.result import ResultMetaData, RowProxy
from sqlalchemy.sql import ClauseElement
from sqlalchemy.types import TypeEngine

from databases.core import DatabaseURL
from databases.interfaces import ConnectionBackend, DatabaseBackend, TransactionBackend

logger = logging.getLogger("databases")


class MySQLBackend(DatabaseBackend):
    def __init__(self, database_url: DatabaseURL) -> None:
        self._database_url = database_url
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
        return MySQLConnection(self, self._dialect)


class CompilationContext:
    def __init__(self, context: ExecutionContext):
        self.context = context


class MySQLConnection(ConnectionBackend):
    def __init__(self, database: MySQLBackend, dialect: Dialect):
        self._database = database
        self._dialect = dialect
        self._connection = None  # type: typing.Optional[aiomysql.Connection]

    async def acquire(self) -> None:
        assert self._connection is None, "Connection is already acquired"
        assert self._database._pool is not None, "DatabaseBackend is not running"
        self._connection = await self._database._pool.acquire()

    async def release(self) -> None:
        assert self._connection is not None, "Connection is not acquired"
        assert self._database._pool is not None, "DatabaseBackend is not running"
        await self._database._pool.release(self._connection)
        self._connection = None

    async def fetch_all(self, query: ClauseElement) -> typing.List[RowProxy]:
        assert self._connection is not None, "Connection is not acquired"
        query, args, context = self._compile(query)
        cursor = await self._connection.cursor()
        try:
            await cursor.execute(query, args)
            rows = await cursor.fetchall()
            metadata = ResultMetaData(context, cursor.description)
            return [
                RowProxy(metadata, row, metadata._processors, metadata._keymap)
                for row in rows
            ]
        finally:
            await cursor.close()

    async def fetch_one(self, query: ClauseElement) -> RowProxy:
        assert self._connection is not None, "Connection is not acquired"
        query, args, context = self._compile(query)
        cursor = await self._connection.cursor()
        try:
            await cursor.execute(query, args)
            row = await cursor.fetchone()
            metadata = ResultMetaData(context, cursor.description)
            return RowProxy(metadata, row, metadata._processors, metadata._keymap)
        finally:
            await cursor.close()

    async def execute(self, query: ClauseElement, values: dict = None) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        if values is not None:
            query = query.values(values)
        query, args, context = self._compile(query)
        cursor = await self._connection.cursor()
        try:
            await cursor.execute(query, args)
            return cursor.lastrowid
        finally:
            await cursor.close()

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        assert self._connection is not None, "Connection is not acquired"
        cursor = await self._connection.cursor()
        try:
            for item in values:
                single_query = query.values(item)
                single_query, args, context = self._compile(single_query)
                await cursor.execute(single_query, args)
        finally:
            await cursor.close()

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        assert self._connection is not None, "Connection is not acquired"
        query, args, context = self._compile(query)
        cursor = await self._connection.cursor()
        try:
            await cursor.execute(query, args)
            metadata = ResultMetaData(context, cursor.description)
            async for row in cursor:
                yield RowProxy(metadata, row, metadata._processors, metadata._keymap)
        finally:
            await cursor.close()

    def transaction(self) -> TransactionBackend:
        return MySQLTransaction(self)

    def _compile(
        self, query: ClauseElement
    ) -> typing.Tuple[str, dict, CompilationContext]:
        compiled = query.compile(dialect=self._dialect)
        args = compiled.construct_params()
        for key, val in args.items():
            if key in compiled._bind_processors:
                args[key] = compiled._bind_processors[key](val)

        execution_context = self._dialect.execution_ctx_cls()
        execution_context.dialect = self._dialect
        execution_context.result_column_struct = (
            compiled._result_columns,
            compiled._ordered_columns,
            compiled._textual_ordered_columns,
        )

        logger.debug(compiled.string, args)
        return compiled.string, args, CompilationContext(execution_context)


class MySQLTransaction(TransactionBackend):
    def __init__(self, connection: MySQLConnection):
        self._connection = connection
        self._is_root = False
        self._savepoint_name = ""

    async def start(self, is_root: bool) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        self._is_root = is_root
        if self._is_root:
            await self._connection._connection.begin()
        else:
            id = str(uuid.uuid4()).replace("-", "_")
            self._savepoint_name = f"STARLETTE_SAVEPOINT_{id}"
            cursor = await self._connection._connection.cursor()
            try:
                await cursor.execute(f"SAVEPOINT {self._savepoint_name}")
            finally:
                await cursor.close()

    async def commit(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        if self._is_root:
            await self._connection._connection.commit()
        else:
            cursor = await self._connection._connection.cursor()
            try:
                await cursor.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")
            finally:
                await cursor.close()

    async def rollback(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        if self._is_root:
            await self._connection._connection.rollback()
        else:
            cursor = await self._connection._connection.cursor()
            try:
                await cursor.execute(f"ROLLBACK TO SAVEPOINT {self._savepoint_name}")
            finally:
                await cursor.close()
