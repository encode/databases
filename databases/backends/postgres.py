import logging
import typing

import asyncpg
from sqlalchemy.dialects.postgresql import pypostgresql
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import ClauseElement

from databases.core import DatabaseURL
from databases.interfaces import DatabaseBackend, DatabaseSession, DatabaseTransaction

logger = logging.getLogger("databases")


_result_processors = {}  # type: dict


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

    def session(self) -> "PostgresSession":
        assert self.pool is not None, "DatabaseBackend is not running"
        return PostgresSession(self.pool, self.dialect)


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


class PostgresSession(DatabaseSession):
    def __init__(self, pool: asyncpg.pool.Pool, dialect: Dialect):
        self.pool = pool
        self.dialect = dialect
        self.conn = None
        self.connection_holders = 0

    def _compile(self, query: ClauseElement) -> typing.Tuple[str, list, tuple]:
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
        return compiled_query, args, compiled._result_columns

    async def fetch_all(self, query: ClauseElement) -> typing.Any:
        query, args, result_columns = self._compile(query)

        conn = await self.acquire_connection()
        try:
            rows = await conn.fetch(query, *args)
        finally:
            await self.release_connection()
        return [Record(row, result_columns, self.dialect) for row in rows]

    async def fetch_one(self, query: ClauseElement) -> typing.Any:
        query, args, result_columns = self._compile(query)

        conn = await self.acquire_connection()
        try:
            row = await conn.fetchrow(query, *args)
        finally:
            await self.release_connection()
        return Record(row, result_columns, self.dialect)

    async def execute(self, query: ClauseElement, values: dict = None) -> None:
        if values is not None:
            query = query.values(values)
        query, args, result_columns = self._compile(query)

        conn = await self.acquire_connection()
        try:
            await conn.execute(query, *args)
        finally:
            await self.release_connection()

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        conn = await self.acquire_connection()
        try:
            # asyncpg uses prepared statements under the hood, so we just
            # loop through multiple executes here, which should all end up
            # using the same prepared statement.
            for item in values:
                single_query = query.values(item)
                single_query, args, result_columns = self._compile(single_query)
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

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        query, args, result_columns = self._compile(query)

        conn = await self.acquire_connection()
        try:
            async for row in conn.cursor(query, *args):
                yield Record(row, result_columns, self.dialect)
        finally:
            await self.release_connection()

    async def raw(
        self, query: ClauseElement, **kwargs: typing.Any
    ) -> typing.AsyncGenerator[typing.Any, None]:
        query, args, result_columns = self._compile(query.bindparams(**kwargs))
        conn = await self.acquire_connection()
        try:
            async for row in conn.cursor(query, *args):
                yield Record(row, result_columns, self.dialect)
        finally:
            await self.release_connection()


class PostgresTransaction(DatabaseTransaction):
    def __init__(self, session: PostgresSession):
        self.session = session

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
