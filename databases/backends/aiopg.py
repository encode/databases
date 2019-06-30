import logging
import typing
import uuid

import aiopg

from sqlalchemy.dialects.postgresql import pypostgresql
from sqlalchemy.engine.interfaces import Dialect, ExecutionContext
from sqlalchemy.engine.result import ResultMetaData, RowProxy
from sqlalchemy.sql import ClauseElement
from sqlalchemy.types import TypeEngine

from databases.core import DatabaseURL
from databases.interfaces import ConnectionBackend, DatabaseBackend, TransactionBackend

logger = logging.getLogger("databases")


class AiopgBackend(DatabaseBackend):
    def __init__(
        self, database_url: typing.Union[DatabaseURL, str], **options: typing.Any
    ) -> None:
        self._database_url = DatabaseURL(database_url)
        self._options = options
        self._dialect = self._get_dialect()
        self._pool = None

    def _get_dialect(self) -> Dialect:
        dialect = pypostgresql.dialect(paramstyle="pyformat")

        dialect.implicit_returning = True
        dialect.supports_native_enum = True
        dialect.supports_smallserial = True  # 9.2+
        dialect._backslash_escapes = False
        dialect.supports_sane_multi_rowcount = True  # psycopg 2.0.9+
        dialect._has_native_hstore = True
        dialect.supports_native_decimal = True

        return dialect

    def _get_connection_kwargs(self) -> dict:  # TODO move to `core.py`
        url_options = self._database_url.options

        kwargs = {}
        min_size = url_options.get("min_size")
        max_size = url_options.get("max_size")
        ssl = url_options.get("ssl")

        if min_size is not None:
            kwargs["minsize"] = int(min_size)
        if max_size is not None:
            kwargs["maxsize"] = int(max_size)
        if ssl is not None:
            kwargs["ssl"] = {"true": True, "false": False}[ssl.lower()]

        for key, value in self._options.items():
            # Coerce 'min_size' and 'max_size' for consistency.
            if key == "min_size":
                key = "minsize"
            elif key == "max_size":
                key = "maxsize"
            kwargs[key] = value

        return kwargs

    async def connect(self) -> None:  # TODO as MySQL one?
        assert self._pool is None, "DatabaseBackend is already running"
        kwargs = self._get_connection_kwargs()
        self._pool = await aiopg.create_pool(
            host=self._database_url.hostname,
            port=self._database_url.port,
            user=self._database_url.username or getpass.getuser(),
            password=self._database_url.password,
            database=self._database_url.database,
            # autocommit=True,
            **kwargs,
        )

    async def disconnect(self) -> None:
        assert self._pool is not None, "DatabaseBackend is not running"
        self._pool.close()
        await self._pool.wait_closed()
        self._pool = None

    def connection(self) -> "AiopgConnection":
        return AiopgConnection(self, self._dialect)


class CompilationContext:
    def __init__(self, context: ExecutionContext):
        self.context = context


class AiopgConnection(ConnectionBackend):
    def __init__(self, database: AiopgBackend, dialect: Dialect):
        self._database = database
        self._dialect = dialect
        self._connection = None  # type: typing.Optional[aiopg.Connection]

    async def acquire(self) -> None:
        assert self._connection is None, "Connection is already acquired"
        assert self._database._pool is not None, "DatabaseBackend is not running"
        self._connection = await self._database._pool.acquire()

    async def release(self) -> None:
        assert self._connection is not None, "Connection is not acquired"
        assert self._database._pool is not None, "DatabaseBackend is not running"
        await self._database._pool.release(self._connection)
        self._connection = None

    async def fetch_all(self, query: ClauseElement) -> typing.List[typing.Mapping]:
        assert self._connection is not None, "Connection is not acquired"
        query, args, context = self._compile(query)
        cursor = await self._connection.cursor()
        # TODO
        import pdb; pdb.set_trace()
        try:
            await cursor.execute(query, args)
            rows = await cursor.fetchall()
            metadata = ResultMetaData(context, cursor.description)
            return [
                RowProxy(metadata, row, metadata._processors, metadata._keymap)
                for row in rows
            ]
        finally:
            cursor.close()

    async def fetch_one(self, query: ClauseElement) -> typing.Optional[typing.Mapping]:
        assert self._connection is not None, "Connection is not acquired"
        query, args, context = self._compile(query)
        cursor = await self._connection.cursor()
        # TODO
        import pdb; pdb.set_trace()
        try:
            await cursor.execute(query, args)
            row = await cursor.fetchone()
            if row is None:
                return None
            metadata = ResultMetaData(context, cursor.description)
            return RowProxy(metadata, row, metadata._processors, metadata._keymap)
        finally:
            cursor.close()

    async def execute(self, query: ClauseElement) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        query, args, context = self._compile(query)
        cursor = await self._connection.cursor()
        # TODO
        import pdb; pdb.set_trace()
        try:
            await cursor.execute(query, args)
            return cursor.lastrowid
        finally:
            cursor.close()

    async def execute_many(self, queries: typing.List[ClauseElement]) -> None:
        assert self._connection is not None, "Connection is not acquired"
        cursor = await self._connection.cursor()
        # TODO
        import pdb; pdb.set_trace()
        try:
            for single_query in queries:
                single_query, args, context = self._compile(single_query)
                await cursor.execute(single_query, args)
        finally:
            cursor.close()

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        assert self._connection is not None, "Connection is not acquired"
        query, args, context = self._compile(query)
        cursor = await self._connection.cursor()
        # TODO
        import pdb; pdb.set_trace()
        try:
            await cursor.execute(query, args)
            metadata = ResultMetaData(context, cursor.description)
            async for row in cursor:
                yield RowProxy(metadata, row, metadata._processors, metadata._keymap)
        finally:
            cursor.close()

    def transaction(self) -> TransactionBackend:
        return AiopgTransaction(self)

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

        logger.debug("Query: %s\nArgs: %s", compiled.string, args)
        return compiled.string, args, CompilationContext(execution_context)

    @property
    def raw_connection(self) -> aiopg.connection.Connection:
        assert self._connection is not None, "Connection is not acquired"
        return self._connection


class AiopgTransaction(TransactionBackend):
    def __init__(self, connection: AiopgConnection):
        self._connection = connection
        self._is_root = False
        self._savepoint_name = ""

    async def start(self, is_root: bool) -> None:
        import pdb; pdb.set_trace()
        assert self._connection._connection is not None, "Connection is not acquired"
        self._is_root = is_root
        cursor = await self._connection._connection.cursor()
        if self._is_root:
            # await self._connection._connection.begin()
            await cursor.execute("BEGIN")
        else:
            id = str(uuid.uuid4()).replace("-", "_")
            self._savepoint_name = f"STARLETTE_SAVEPOINT_{id}"
            # cursor = await self._connection._connection.cursor()
            try:
                await cursor.execute(f"SAVEPOINT {self._savepoint_name}")
            finally:
                cursor.close()

    async def commit(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        cursor = await self._connection._connection.cursor()
        if self._is_root:
            # await self._connection._connection.commit()
            await cursor.execute("COMMIT")
        else:
            # cursor = await self._connection._connection.cursor()
            try:
                await cursor.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")
            finally:
                cursor.close()

    async def rollback(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        cursor = await self._connection._connection.cursor()
        if self._is_root:
            # await self._connection._connection.rollback()
            await cursor.execute("ROLLBACK")
        else:
            # cursor = await self._connection._connection.cursor()
            try:
                await cursor.execute(f"ROLLBACK TO SAVEPOINT {self._savepoint_name}")
            finally:
                cursor.close()