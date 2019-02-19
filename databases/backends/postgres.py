import logging
import typing

import asyncpg
from sqlalchemy.dialects.postgresql import pypostgresql
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import ClauseElement

from databases.core import DatabaseURL
from databases.interfaces import ConnectionBackend, DatabaseBackend, TransactionBackend

logger = logging.getLogger("databases")


_result_processors = {}  # type: dict


class PostgresBackend(DatabaseBackend):
    def __init__(self, database_url: DatabaseURL) -> None:
        self._database_url = database_url
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

        return dialect

    async def connect(self) -> None:
        assert self._pool is None, "DatabaseBackend is already running"
        self._pool = await asyncpg.create_pool(str(self._database_url))

    async def disconnect(self) -> None:
        assert self._pool is not None, "DatabaseBackend is not running"
        await self._pool.close()
        self._pool = None

    def connection(self) -> "PostgresConnection":
        return PostgresConnection(self, self._dialect)


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


class PostgresConnection(ConnectionBackend):
    def __init__(self, database: PostgresBackend, dialect: Dialect):
        self._database = database
        self._dialect = dialect
        self._connection = None  # type: typing.Optional[asyncpg.connection.Connection]

    async def acquire(self) -> None:
        assert self._connection is None, "Connection is already acquired"
        assert self._database._pool is not None, "DatabaseBackend is not running"
        self._connection = await self._database._pool.acquire()

    async def release(self) -> None:
        assert self._connection is not None, "Connection is not acquired"
        assert self._database._pool is not None, "DatabaseBackend is not running"
        self._connection = await self._database._pool.release(self._connection)
        self._connection = None

    async def fetch_all(self, query: ClauseElement) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        query, args, result_columns = self._compile(query)
        rows = await self._connection.fetch(query, *args)
        return [Record(row, result_columns, self._dialect) for row in rows]

    async def fetch_one(self, query: ClauseElement) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        query, args, result_columns = self._compile(query)
        row = await self._connection.fetchrow(query, *args)
        return Record(row, result_columns, self._dialect)

    async def execute(self, query: ClauseElement, values: dict = None) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        if values is not None:
            query = query.values(values)
        query, args, result_columns = self._compile(query)
        return await self._connection.fetchval(query, *args)

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        assert self._connection is not None, "Connection is not acquired"
        # asyncpg uses prepared statements under the hood, so we just
        # loop through multiple executes here, which should all end up
        # using the same prepared statement.
        for item in values:
            single_query = query.values(item)
            single_query, args, result_columns = self._compile(single_query)
            await self._connection.execute(single_query, *args)

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        assert self._connection is not None, "Connection is not acquired"
        query, args, result_columns = self._compile(query)
        async for row in self._connection.cursor(query, *args):
            yield Record(row, result_columns, self._dialect)

    def transaction(self) -> TransactionBackend:
        return PostgresTransaction(connection=self)

    def _compile(self, query: ClauseElement) -> typing.Tuple[str, list, tuple]:
        compiled = query.compile(dialect=self._dialect)
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


class PostgresTransaction(TransactionBackend):
    def __init__(self, connection: PostgresConnection):
        self._connection = connection
        self._transaction = (
            None
        )  # type: typing.Optional[asyncpg.transaction.Transaction]

    async def start(self, is_root: bool) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        self._transaction = self._connection._connection.transaction()
        await self._transaction.start()

    async def commit(self) -> None:
        assert self._transaction is not None
        await self._transaction.commit()

    async def rollback(self) -> None:
        assert self._transaction is not None
        await self._transaction.rollback()
