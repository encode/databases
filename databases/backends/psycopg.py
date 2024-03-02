import typing

import psycopg
import psycopg_pool
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


class PsycopgBackend(DatabaseBackend):
    _database_url: DatabaseURL
    _options: typing.Dict[str, typing.Any]
    _dialect: Dialect
    _pool: typing.Optional[psycopg_pool.AsyncConnectionPool]

    def __init__(
        self,
        database_url: typing.Union[DatabaseURL, str],
        **options: typing.Dict[str, typing.Any],
    ) -> None:
        self._database_url = DatabaseURL(database_url)
        self._options = options
        self._dialect = get_dialect()
        self._pool = None

    async def connect(self) -> None:
        if self._pool is not None:
            return

        self._pool = psycopg_pool.AsyncConnectionPool(
            self._database_url._url, open=False, **self._options
        )

        # TODO: Add configurable timeouts
        await self._pool.open()

    async def disconnect(self) -> None:
        if self._pool is None:
            return

        # TODO: Add configurable timeouts
        await self._pool.close()
        self._pool = None

    def connection(self) -> "PsycopgConnection":
        return PsycopgConnection(self, self._dialect)


class PsycopgConnection(ConnectionBackend):
    _database: PsycopgBackend
    _dialect: Dialect
    _connection: typing.Optional[psycopg.AsyncConnection]

    def __init__(self, database: PsycopgBackend, dialect: Dialect) -> None:
        self._database = database
        self._dialect = dialect
        self._connection = None

    async def acquire(self) -> None:
        if self._connection is not None:
            return

        if self._database._pool is None:
            raise RuntimeError("PsycopgBackend is not running")

        # TODO: Add configurable timeouts
        self._connection = await self._database._pool.getconn()

    async def release(self) -> None:
        if self._connection is None:
            return

        await self._database._pool.putconn(self._connection)
        self._connection = None

    async def fetch_all(self, query: ClauseElement) -> typing.List[RecordInterface]:
        if self._connection is None:
            raise RuntimeError("Connection is not acquired")

        query_str, args, result_columns = compile_query(query, self._dialect)

        async with self._connection.cursor() as cursor:
            await cursor.execute(query_str, args)
            rows = await cursor.fetchall()

        column_maps = create_column_maps(result_columns)
        return [Record(row, result_columns, self._dialect, column_maps) for row in rows]

    async def fetch_one(self, query: ClauseElement) -> typing.Optional[RecordInterface]:
        if self._connection is None:
            raise RuntimeError("Connection is not acquired")

        query_str, args, result_columns = compile_query(query, self._dialect)

        async with self._connection.cursor() as cursor:
            await cursor.execute(query_str, args)
            row = await cursor.fetchone()

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
        row = await self.fetch_one(query)
        return None if row is None else row[column]

    async def execute(self, query: ClauseElement) -> typing.Any:
        if self._connection is None:
            raise RuntimeError("Connection is not acquired")

        query_str, args, _ = compile_query(query, self._dialect)

        async with self._connection.cursor() as cursor:
            await cursor.execute(query_str, args)

    async def execute_many(self, queries: typing.List[ClauseElement]) -> None:
        # TODO: Find a way to use psycopg's executemany
        for query in queries:
            await self.execute(query)

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Mapping, None]:
        if self._connection is None:
            raise RuntimeError("Connection is not acquired")

        query_str, args, result_columns = compile_query(query, self._dialect)
        column_maps = create_column_maps(result_columns)

        async with self._connection.cursor() as cursor:
            await cursor.execute(query_str, args)

            while True:
                row = await cursor.fetchone()

                if row is None:
                    break

                yield Record(row, result_columns, self._dialect, column_maps)

    def transaction(self) -> "TransactionBackend":
        raise NotImplementedError()  # pragma: no cover

    @property
    def raw_connection(self) -> typing.Any:
        if self._connection is None:
            raise RuntimeError("Connection is not acquired")
        return self._connection


class PsycopgTransaction(TransactionBackend):
    async def start(
        self, is_root: bool, extra_options: typing.Dict[typing.Any, typing.Any]
    ) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def commit(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def rollback(self) -> None:
        raise NotImplementedError()  # pragma: no cover
