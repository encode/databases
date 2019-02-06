import logging
import typing
from types import TracebackType

import asyncpg
from sqlalchemy.dialects.postgresql import pypostgresql
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import ClauseElement

from databases.core import DatabaseURL
from databases.interfaces import DatabaseBackend, DatabaseSession, DatabaseTransaction

logger = logging.getLogger("databases")


class PostgresBackend(DatabaseBackend):
    def __init__(self, database_url: typing.Union[str, DatabaseURL]) -> None:
        self.database_url = DatabaseURL(database_url)
        self.dialect = self._get_dialect()
        self.pool = None

    def _get_dialect(self) -> Dialect:
        dialect = pypostgresql.dialect(paramstyle="pyformat")

        dialect.implicit_returning = True
        dialect.supports_native_enum = True
        dialect.supports_smallserial = True  # 9.2+
        dialect._backslash_escapes = False
        dialect.supports_sane_multi_rowcount = True  # psycopg 2.0.9+
        dialect._has_native_hstore = True

        return dialect

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(str(self.database_url))

    async def disconnect(self) -> None:
        assert self.pool is not None, "DatabaseBackend is not running"
        await self.pool.close()
        self.pool = None

    def session(self, rollback_isolation: bool = False) -> "PostgresSession":
        assert self.pool is not None, "DatabaseBackend is not running"
        return PostgresSession(
            self.pool, self.dialect, rollback_isolation=rollback_isolation
        )


class PostgresSession(DatabaseSession):
    def __init__(
        self,
        pool: asyncpg.pool.Pool,
        dialect: Dialect,
        rollback_isolation: bool = False,
    ):
        self.pool = pool
        self.dialect = dialect
        self.conn = None
        self.connection_holders = 0
        self._rollback_transaction = None
        if rollback_isolation:
            self._rollback_transaction = self.transaction()

    async def __aenter__(self) -> DatabaseSession:
        if self._rollback_transaction is not None:
            await self._rollback_transaction.start()
        return self

    async def __aexit__(
        self,
        exc_type: typing.Type[BaseException] = None,
        exc_value: BaseException = None,
        traceback: TracebackType = None,
    ) -> None:
        if self._rollback_transaction is not None:
            await self._rollback_transaction.rollback()

    def _compile(self, query: ClauseElement) -> typing.Tuple[str, list]:
        compiled = query.compile(dialect=self.dialect)
        compiled_params = sorted(compiled.params.items())

        mapping = {
            key: "$" + str(i) for i, (key, _) in enumerate(compiled_params, start=1)
        }
        compiled_query = compiled.string % mapping

        processors = compiled._bind_processors
        args = [
            processors[key](val) if key in processors else val
            for key, val in compiled_params
        ]

        logger.debug(compiled_query, args)
        return compiled_query, args

    async def fetchall(self, query: ClauseElement) -> typing.Any:
        query, args = self._compile(query)

        conn = await self.acquire_connection()
        try:
            return await conn.fetch(query, *args)
        finally:
            await self.release_connection()

    async def fetchone(self, query: ClauseElement) -> typing.Any:
        query, args = self._compile(query)

        conn = await self.acquire_connection()
        try:
            return await conn.fetchrow(query, *args)
        finally:
            await self.release_connection()

    async def execute(self, query: ClauseElement) -> None:
        query, args = self._compile(query)

        conn = await self.acquire_connection()
        try:
            await conn.execute(query, *args)
        finally:
            await self.release_connection()

    async def executemany(self, query: ClauseElement, values: list) -> None:
        conn = await self.acquire_connection()
        try:
            # asyncpg uses prepared statements under the hood, so we just
            # loop through multiple executes here, which should all end up
            # using the same prepared statement.
            for item in values:
                single_query = query.values(item)
                single_query, args = self._compile(single_query)
                await conn.execute(single_query, *args)
        finally:
            await self.release_connection()

    def transaction(self) -> DatabaseTransaction:
        return PostgresTransaction(session=self)

    async def acquire_connection(self) -> asyncpg.Connection:
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


class PostgresTransaction(DatabaseTransaction):
    def __init__(self, session: PostgresSession):
        self.session = session

    async def __aenter__(self) -> None:
        await self.start()

    async def __aexit__(
        self,
        exc_type: typing.Type[BaseException] = None,
        exc_value: BaseException = None,
        traceback: TracebackType = None,
    ) -> None:
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()

    async def start(self) -> None:
        conn = await self.session.acquire_connection()
        self.transaction = conn.transaction()
        await self.transaction.start()

    async def commit(self) -> None:
        await self.transaction.commit()
        await self.session.release_connection()

    async def rollback(self) -> None:
        await self.transaction.rollback()
        await self.session.release_connection()
