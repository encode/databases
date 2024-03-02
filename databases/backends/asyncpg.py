import logging
import typing

import asyncpg
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import ClauseElement

from databases.backends.common.records import Record, create_column_maps
from databases.backends.dialects.psycopg import compile_query, get_dialect
from databases.core import DatabaseURL
from databases.interfaces import (
    ConnectionBackend,
    DatabaseBackend,
    Record as RecordInterface,
    TransactionBackend,
)

logger = logging.getLogger("databases")


class AsyncpgBackend(DatabaseBackend):
    def __init__(
        self, database_url: typing.Union[DatabaseURL, str], **options: typing.Any
    ) -> None:
        self._database_url = DatabaseURL(database_url)
        self._options = options
        self._dialect = get_dialect()
        self._pool = None

    def _get_connection_kwargs(self) -> dict:
        url_options = self._database_url.options

        kwargs: typing.Dict[str, typing.Any] = {}
        min_size = url_options.get("min_size")
        max_size = url_options.get("max_size")
        ssl = url_options.get("ssl")

        if min_size is not None:
            kwargs["min_size"] = int(min_size)
        if max_size is not None:
            kwargs["max_size"] = int(max_size)
        if ssl is not None:
            ssl = ssl.lower()
            kwargs["ssl"] = {"true": True, "false": False}.get(ssl, ssl)

        kwargs.update(self._options)

        return kwargs

    async def connect(self) -> None:
        assert self._pool is None, "DatabaseBackend is already running"
        kwargs = dict(
            host=self._database_url.hostname,
            port=self._database_url.port,
            user=self._database_url.username,
            password=self._database_url.password,
            database=self._database_url.database,
        )
        kwargs.update(self._get_connection_kwargs())
        self._pool = await asyncpg.create_pool(**kwargs)

    async def disconnect(self) -> None:
        assert self._pool is not None, "DatabaseBackend is not running"
        await self._pool.close()
        self._pool = None

    def connection(self) -> "AsyncpgConnection":
        return AsyncpgConnection(self, self._dialect)


class AsyncpgConnection(ConnectionBackend):
    def __init__(self, database: AsyncpgBackend, dialect: Dialect):
        self._database = database
        self._dialect = dialect
        self._connection: typing.Optional[asyncpg.connection.Connection] = None

    async def acquire(self) -> None:
        assert self._connection is None, "Connection is already acquired"
        assert self._database._pool is not None, "DatabaseBackend is not running"
        self._connection = await self._database._pool.acquire()

    async def release(self) -> None:
        assert self._connection is not None, "Connection is not acquired"
        assert self._database._pool is not None, "DatabaseBackend is not running"
        self._connection = await self._database._pool.release(self._connection)
        self._connection = None

    async def fetch_all(self, query: ClauseElement) -> typing.List[RecordInterface]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns = compile_query(query, self._dialect)
        rows = await self._connection.fetch(query_str, *args)
        dialect = self._dialect
        column_maps = create_column_maps(result_columns)
        return [Record(row, result_columns, dialect, column_maps) for row in rows]

    async def fetch_one(self, query: ClauseElement) -> typing.Optional[RecordInterface]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns = compile_query(query, self._dialect)
        row = await self._connection.fetchrow(query_str, *args)
        if row is None:
            return None
        return Record(
            row,
            result_columns,
            self._dialect,
            create_column_maps(result_columns),
        )

    async def fetch_val(
        self, query: ClauseElement, column: typing.Any = 0
    ) -> typing.Any:
        # we are not calling self._connection.fetchval here because
        # it does not convert all the types, e.g. JSON stays string
        # instead of an object
        # see also:
        # https://github.com/encode/databases/pull/131
        # https://github.com/encode/databases/pull/132
        # https://github.com/encode/databases/pull/246
        row = await self.fetch_one(query)
        if row is None:
            return None
        return row[column]

    async def execute(self, query: ClauseElement) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, _ = compile_query(query, self._dialect)
        return await self._connection.fetchval(query_str, *args)

    async def execute_many(self, queries: typing.List[ClauseElement]) -> None:
        assert self._connection is not None, "Connection is not acquired"
        # asyncpg uses prepared statements under the hood, so we just
        # loop through multiple executes here, which should all end up
        # using the same prepared statement.
        for single_query in queries:
            single_query, args, _ = compile_query(single_query, self._dialect)
            await self._connection.execute(single_query, *args)

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, result_columns = compile_query(query, self._dialect)
        column_maps = create_column_maps(result_columns)
        async for row in self._connection.cursor(query_str, *args):
            yield Record(row, result_columns, self._dialect, column_maps)

    def transaction(self) -> TransactionBackend:
        return AsyncpgTransaction(connection=self)

    @property
    def raw_connection(self) -> asyncpg.connection.Connection:
        assert self._connection is not None, "Connection is not acquired"
        return self._connection


class AsyncpgTransaction(TransactionBackend):
    def __init__(self, connection: AsyncpgConnection):
        self._connection = connection
        self._transaction: typing.Optional[asyncpg.transaction.Transaction] = None

    async def start(
        self, is_root: bool, extra_options: typing.Dict[typing.Any, typing.Any]
    ) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        self._transaction = self._connection._connection.transaction(**extra_options)
        await self._transaction.start()

    async def commit(self) -> None:
        assert self._transaction is not None
        await self._transaction.commit()

    async def rollback(self) -> None:
        assert self._transaction is not None
        await self._transaction.rollback()
