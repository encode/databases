import getpass
import logging
import typing
import uuid

import aioodbc
from sqlalchemy.dialects.mssql import pyodbc
from sqlalchemy.engine.cursor import CursorResultMetaData
from sqlalchemy.engine.interfaces import Dialect, ExecutionContext
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.ddl import DDLElement

from databases.backends.common.records import Record, Row, create_column_maps
from databases.core import LOG_EXTRA, DatabaseURL
from databases.interfaces import (
    ConnectionBackend,
    DatabaseBackend,
    Record as RecordInterface,
    TransactionBackend,
)

logger = logging.getLogger("databases")


class MSSQLBackend(DatabaseBackend):
    def __init__(
        self, database_url: typing.Union[DatabaseURL, str], **options: typing.Any
    ) -> None:
        self._database_url = DatabaseURL(database_url)
        self._options = options
        self._dialect = pyodbc.dialect(paramstyle="pyformat")
        self._dialect.supports_native_decimal = True
        self._pool: aioodbc.Pool = None

    def _get_connection_kwargs(self) -> dict:
        url_options = self._database_url.options

        kwargs = {}
        min_size = url_options.get("min_size")
        max_size = url_options.get("max_size")
        pool_recycle = url_options.get("pool_recycle")
        ssl = url_options.get("ssl")
        driver = url_options.get("driver")
        trusted_connection = url_options.get("trusted_connection", "no")

        assert driver is not None, "The driver must be specified"

        if min_size is not None:
            kwargs["minsize"] = int(min_size)
        if max_size is not None:
            kwargs["maxsize"] = int(max_size)
        if pool_recycle is not None:
            kwargs["pool_recycle"] = int(pool_recycle)
        if ssl is not None:
            kwargs["ssl"] = {"true": True, "false": False}[ssl.lower()]

        kwargs["trusted_connection"] = trusted_connection.lower()
        kwargs["driver"] = driver

        for key, value in self._options.items():
            # Coerce 'min_size' and 'max_size' for consistency.
            if key == "min_size":
                key = "minsize"
            elif key == "max_size":
                key = "maxsize"
            kwargs[key] = value

        return kwargs

    async def connect(self) -> None:
        assert self._pool is None, "DatabaseBackend is already running"
        kwargs = self._get_connection_kwargs()

        driver = kwargs["driver"]
        database = self._database_url.database
        hostname = self._database_url.hostname
        port = self._database_url.port or 1433
        user = self._database_url.username or getpass.getuser()
        password = self._database_url.password

        dsn = f"Driver={driver};Database={database};Server={hostname};UID={user};PWD={password};Port={port}"

        self._pool = await aioodbc.create_pool(
            dsn=dsn,
            autocommit=True,
            **kwargs,
        )

    async def disconnect(self) -> None:
        assert self._pool is not None, "DatabaseBackend is not running"
        self._pool.close()
        await self._pool.wait_closed()
        self._pool = None

    def connection(self) -> "MSSQLConnection":
        return MSSQLConnection(self, self._dialect)


class CompilationContext:
    def __init__(self, context: ExecutionContext):
        self.context = context


class MSSQLConnection(ConnectionBackend):
    def __init__(self, database: MSSQLBackend, dialect: Dialect) -> None:
        self._database = database
        self._dialect = dialect
        self._connection: typing.Optional[aioodbc.Connection] = None

    async def acquire(self) -> None:
        assert self._connection is None, "Connection is already acquired"
        assert self._database._pool is not None, "DatabaseBackend is not running"
        self._connection = await self._database._pool.acquire()

    async def release(self) -> None:
        assert self._connection is not None, "Connection is not acquired"
        assert self._database._pool is not None, "DatabaseBackend is not running"
        await self._database._pool.release(self._connection)
        self._connection = None

    async def fetch_all(self, query: ClauseElement) -> typing.List["RecordInterface"]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns, context = self._compile(query)
        column_maps = create_column_maps(result_columns)
        dialect = self._dialect
        cursor = await self._connection.cursor()
        try:
            await cursor.execute(query_str, args)
            rows = await cursor.fetchall()
            metadata = CursorResultMetaData(context, cursor.description)
            rows = [
                Row(
                    metadata,
                    metadata._processors,
                    metadata._keymap,
                    Row._default_key_style,
                    row,
                )
                for row in rows
            ]
            return [Record(row, result_columns, dialect, column_maps) for row in rows]
        finally:
            await cursor.close()

    async def fetch_one(self, query: ClauseElement) -> typing.Optional[RecordInterface]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns, context = self._compile(query)
        column_maps = create_column_maps(result_columns)
        dialect = self._dialect
        cursor = await self._connection.cursor()
        try:
            await cursor.execute(query_str, args)
            row = await cursor.fetchone()
            if row is None:
                return None
            metadata = CursorResultMetaData(context, cursor.description)
            row = Row(
                metadata,
                metadata._processors,
                metadata._keymap,
                Row._default_key_style,
                row,
            )
            return Record(row, result_columns, dialect, column_maps)
        finally:
            await cursor.close()

    async def execute(self, query: ClauseElement) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, _, _ = self._compile(query)
        cursor = await self._connection.cursor()
        try:
            values = await cursor.execute(query_str, args)
            try:
                values = await values.fetchone()
                return values[0]
            except Exception:
                ...
        finally:
            await cursor.close()

    async def execute_many(self, queries: typing.List[ClauseElement]) -> None:
        assert self._connection is not None, "Connection is not acquired"
        cursor = await self._connection.cursor()
        try:
            for single_query in queries:
                single_query, args, _, _ = self._compile(single_query)
                await cursor.execute(single_query, args)
        finally:
            await cursor.close()

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns, context = self._compile(query)
        column_maps = create_column_maps(result_columns)
        dialect = self._dialect
        cursor = await self._connection.cursor()
        try:
            await cursor.execute(query_str, args)
            metadata = CursorResultMetaData(context, cursor.description)
            async for row in cursor:
                record = Row(
                    metadata,
                    metadata._processors,
                    metadata._keymap,
                    Row._default_key_style,
                    row,
                )
                yield Record(record, result_columns, dialect, column_maps)
        finally:
            await cursor.close()

    def transaction(self) -> TransactionBackend:
        return MSSQLTransaction(self)

    def _compile(self, query: ClauseElement) -> typing.Tuple[str, list, tuple]:
        compiled = query.compile(
            dialect=self._dialect, compile_kwargs={"render_postcompile": True}
        )

        execution_context = self._dialect.execution_ctx_cls()
        execution_context.dialect = self._dialect

        if not isinstance(query, DDLElement):
            compiled_params = compiled.params.items()

            mapping = {key: "?" for _, (key, _) in enumerate(compiled_params, start=1)}
            compiled_query = compiled.string % mapping

            processors = compiled._bind_processors
            args = [
                processors[key](val) if key in processors else val
                for key, val in compiled_params
            ]

            execution_context.result_column_struct = (
                compiled._result_columns,
                compiled._ordered_columns,
                compiled._textual_ordered_columns,
                compiled._ad_hoc_textual,
                compiled._loose_column_name_matching,
            )

            result_map = compiled._result_columns
        else:
            compiled_query = compiled.string
            args = []
            result_map = None

        query_message = compiled_query.replace(" \n", " ").replace("\n", " ")
        logger.debug(
            "Query: %s Args: %s", query_message, repr(tuple(args)), extra=LOG_EXTRA
        )
        return compiled_query, args, result_map, CompilationContext(execution_context)

    @property
    def raw_connection(self) -> aioodbc.connection.Connection:
        assert self._connection is not None, "Connection is not acquired"
        return self._connection


class MSSQLTransaction(TransactionBackend):
    def __init__(self, connection: MSSQLConnection):
        self._connection = connection
        self._is_root = False
        self._savepoint_name = ""

    async def start(
        self, is_root: bool, extra_options: typing.Dict[typing.Any, typing.Any]
    ) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        self._is_root = is_root
        cursor = await self._connection._connection.cursor()
        if self._is_root:
            await cursor.execute("BEGIN TRANSACTION")
        else:
            id = str(uuid.uuid4()).replace("-", "_")[:12]
            self._savepoint_name = f"STARLETTE_SAVEPOINT_{id}"
            try:
                await cursor.execute(f"SAVE TRANSACTION {self._savepoint_name}")
            finally:
                cursor.close()

    async def commit(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        cursor = await self._connection._connection.cursor()
        if self._is_root:
            await cursor.execute("COMMIT TRANSACTION")
        else:
            try:
                await cursor.execute(f"COMMIT TRANSACTION {self._savepoint_name}")
            finally:
                cursor.close()

    async def rollback(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        cursor = await self._connection._connection.cursor()
        if self._is_root:
            await cursor.execute("BEGIN TRANSACTION")
            await cursor.execute("ROLLBACK TRANSACTION")
        else:
            try:
                await cursor.execute(f"ROLLBACK TRANSACTION {self._savepoint_name}")
            finally:
                cursor.close()
