import typing

import psycopg
import psycopg_pool
from psycopg.rows import namedtuple_row
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.schema import Column

from databases.backends.common.records import Record, create_column_maps
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
        self._dialect = PGDialect_psycopg()
        self._dialect.implicit_returning = True
        self._pool = None

    async def connect(self) -> None:
        if self._pool is not None:
            return

        url = self._database_url._url.replace("postgresql+psycopg", "postgresql")
        self._pool = psycopg_pool.AsyncConnectionPool(url, open=False, **self._options)

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
    _connection: typing.Optional[psycopg.AsyncConnection] = None

    def __init__(self, database: PsycopgBackend, dialect: Dialect) -> None:
        self._database = database
        self._dialect = dialect

    async def acquire(self) -> None:
        if self._connection is not None:
            return

        if self._database._pool is None:
            raise RuntimeError("PsycopgBackend is not running")

        # TODO: Add configurable timeouts
        self._connection = await self._database._pool.getconn()
        await self._connection.set_autocommit(True)

    async def release(self) -> None:
        if self._connection is None:
            return

        await self._database._pool.putconn(self._connection)
        self._connection = None

    async def fetch_all(self, query: ClauseElement) -> typing.List[RecordInterface]:
        if self._connection is None:
            raise RuntimeError("Connection is not acquired")

        query_str, args, result_columns = self._compile(query)

        async with self._connection.cursor(row_factory=namedtuple_row) as cursor:
            await cursor.execute(query_str, args)
            rows = await cursor.fetchall()

        column_maps = create_column_maps(result_columns)
        return [PsycopgRecord(row, result_columns, self._dialect, column_maps) for row in rows]

    async def fetch_one(self, query: ClauseElement) -> typing.Optional[RecordInterface]:
        if self._connection is None:
            raise RuntimeError("Connection is not acquired")

        query_str, args, result_columns = self._compile(query)

        async with self._connection.cursor(row_factory=namedtuple_row) as cursor:
            await cursor.execute(query_str, args)
            row = await cursor.fetchone()

        if row is None:
            return None

        return PsycopgRecord(
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

        query_str, args, _ = self._compile(query)

        async with self._connection.cursor(row_factory=namedtuple_row) as cursor:
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

        query_str, args, result_columns = self._compile(query)
        column_maps = create_column_maps(result_columns)

        async with self._connection.cursor(row_factory=namedtuple_row) as cursor:
            await cursor.execute(query_str, args)

            while True:
                row = await cursor.fetchone()

                if row is None:
                    break

                yield PsycopgRecord(row, result_columns, self._dialect, column_maps)

    def transaction(self) -> "TransactionBackend":
        return PsycopgTransaction(connection=self)

    @property
    def raw_connection(self) -> typing.Any:
        if self._connection is None:
            raise RuntimeError("Connection is not acquired")
        return self._connection

    def _compile(
        self, query: ClauseElement,
    ) -> typing.Tuple[str, typing.Mapping[str, typing.Any], tuple]:
        compiled = query.compile(
            dialect=self._dialect,
            compile_kwargs={"render_postcompile": True},
        )

        compiled_query = compiled.string
        params = compiled.params
        result_map = compiled._result_columns

        return compiled_query, params, result_map


class PsycopgTransaction(TransactionBackend):
    _connecttion: PsycopgConnection
    _transaction: typing.Optional[psycopg.AsyncTransaction]

    def __init__(self, connection: PsycopgConnection):
        self._connection = connection
        self._transaction: typing.Optional[psycopg.AsyncTransaction] = None

    async def start(
        self, is_root: bool, extra_options: typing.Dict[typing.Any, typing.Any]
    ) -> None:
        if self._connection._connection is None:
            raise RuntimeError("Connection is not acquired")

        transaction = psycopg.AsyncTransaction(
            self._connection._connection, **extra_options
        )
        async with transaction._conn.lock:
            await transaction._conn.wait(transaction._enter_gen())
        self._transaction = transaction

    async def commit(self) -> None:
        if self._transaction is None:
            raise RuntimeError("Transaction was not started")

        async with self._transaction._conn.lock:
            await self._transaction._conn.wait(self._transaction._commit_gen())

    async def rollback(self) -> None:
        if self._transaction is None:
            raise RuntimeError("Transaction was not started")

        async with self._transaction._conn.lock:
            await self._transaction._conn.wait(self._transaction._rollback_gen(None))


class PsycopgRecord(Record):
    @property
    def _mapping(self) -> typing.Mapping:
        return self._row._asdict()

    def __getitem__(self, key: typing.Any) -> typing.Any:
        if len(self._column_map) == 0:
            return self._mapping[key]
        elif isinstance(key, Column):
            idx, datatype = self._column_map_full[str(key)]
        elif isinstance(key, int):
            idx, datatype = self._column_map_int[key]
        else:
            idx, datatype = self._column_map[key]

        return self._row[idx]
