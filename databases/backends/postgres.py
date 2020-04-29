import logging
import typing
from collections.abc import Mapping

import asyncpg
from sqlalchemy.dialects.postgresql import pypostgresql
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.schema import Column
from sqlalchemy.types import TypeEngine

from databases.core import LOG_EXTRA, DatabaseURL
from databases.interfaces import ConnectionBackend, DatabaseBackend, TransactionBackend

logger = logging.getLogger("databases")


_result_processors = {}  # type: dict


class PostgresBackend(DatabaseBackend):
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

    def _get_connection_kwargs(self) -> dict:
        url_options = self._database_url.options

        kwargs = {}
        min_size = url_options.get("min_size")
        max_size = url_options.get("max_size")
        ssl = url_options.get("ssl")

        if min_size is not None:
            kwargs["min_size"] = int(min_size)
        if max_size is not None:
            kwargs["max_size"] = int(max_size)
        if ssl is not None:
            kwargs["ssl"] = {"true": True, "false": False}[ssl.lower()]

        kwargs.update(self._options)

        return kwargs

    async def connect(self) -> None:
        assert self._pool is None, "DatabaseBackend is already running"
        kwargs = self._get_connection_kwargs()
        self._pool = await asyncpg.create_pool(str(self._database_url), **kwargs)

    async def disconnect(self) -> None:
        assert self._pool is not None, "DatabaseBackend is not running"
        await self._pool.close()
        self._pool = None

    def connection(self) -> "PostgresConnection":
        return PostgresConnection(self, self._dialect)


class Record(Mapping):
    def __init__(
        self, row: asyncpg.Record, result_columns: tuple, dialect: Dialect
    ) -> None:
        self._row = row
        self._result_columns = result_columns
        self._dialect = dialect
        self._column_map = (
            {}
        )  # type: typing.Mapping[str, typing.Tuple[int, TypeEngine]]
        self._column_map_int = (
            {}
        )  # type: typing.Mapping[int, typing.Tuple[int, TypeEngine]]
        self._column_map_full = (
            {}
        )  # type: typing.Mapping[str, typing.Tuple[int, TypeEngine]]
        for idx, (column_name, _, column, datatype) in enumerate(self._result_columns):
            self._column_map[column_name] = (idx, datatype)
            self._column_map_int[idx] = (idx, datatype)
            self._column_map_full[str(column[0])] = (idx, datatype)

    def values(self) -> typing.ValuesView:
        return self._row.values()

    def __getitem__(self, key: typing.Any) -> typing.Any:
        if len(self._column_map) == 0:  # raw query
            return self._row[tuple(self._row.keys()).index(key)]
        elif type(key) is Column:
            idx, datatype = self._column_map_full[str(key)]
        elif type(key) is int:
            idx, datatype = self._column_map_int[key]
        else:
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

    def __iter__(self) -> typing.Iterator:
        return iter(self._row.keys())

    def __len__(self) -> int:
        return len(self._row)


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

    async def fetch_all(self, query: ClauseElement) -> typing.List[typing.Mapping]:
        assert self._connection is not None, "Connection is not acquired"
        query, args, result_columns = self._compile(query)
        rows = await self._connection.fetch(query, *args)
        return [Record(row, result_columns, self._dialect) for row in rows]

    async def fetch_one(self, query: ClauseElement) -> typing.Optional[typing.Mapping]:
        assert self._connection is not None, "Connection is not acquired"
        query, args, result_columns = self._compile(query)
        row = await self._connection.fetchrow(query, *args)
        if row is None:
            return None
        return Record(row, result_columns, self._dialect)

    async def execute(self, query: ClauseElement) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        query, args, result_columns = self._compile(query)
        return await self._connection.fetchval(query, *args)

    async def execute_many(self, queries: typing.List[ClauseElement]) -> None:
        assert self._connection is not None, "Connection is not acquired"
        # asyncpg uses prepared statements under the hood, so we just
        # loop through multiple executes here, which should all end up
        # using the same prepared statement.
        for single_query in queries:
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

        query_message = compiled_query.replace(" \n", " ").replace("\n", " ")
        logger.debug(
            "Query: %s Args: %s", query_message, repr(tuple(args)), extra=LOG_EXTRA
        )
        return compiled_query, args, compiled._result_columns

    @property
    def raw_connection(self) -> asyncpg.connection.Connection:
        assert self._connection is not None, "Connection is not acquired"
        return self._connection


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
