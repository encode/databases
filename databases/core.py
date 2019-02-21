import asyncio
import functools
import sys
import typing
from types import TracebackType
from urllib.parse import SplitResult, parse_qsl, urlsplit

from sqlalchemy.engine import RowProxy
from sqlalchemy.sql import ClauseElement

from databases.importer import import_from_string
from databases.interfaces import ConnectionBackend, DatabaseBackend, TransactionBackend

if sys.version_info >= (3, 7):  # pragma: no cover
    from contextvars import ContextVar
else:  # pragma: no cover
    from aiocontextvars import ContextVar


class Database:
    SUPPORTED_BACKENDS = {
        "postgresql": "databases.backends.postgres:PostgresBackend",
        "mysql": "databases.backends.mysql:MySQLBackend",
        "sqlite": "databases.backends.sqlite:SQLiteBackend",
    }

    def __init__(
        self, url: typing.Union[str, "DatabaseURL"], *, force_rollback: bool = False
    ):
        self._url = DatabaseURL(url)
        self._force_rollback = force_rollback
        self.is_connected = False

        backend_str = self.SUPPORTED_BACKENDS[self._url.dialect]
        backend_cls = import_from_string(backend_str)
        assert issubclass(backend_cls, DatabaseBackend)
        self._backend = backend_cls(self._url)

        # Connections are stored as task-local state.
        self._connection_context = ContextVar("connection_context")  # type: ContextVar

        # When `force_rollback=True` is used, we use a single global
        # connection, within a transaction that always rolls back.
        self._global_connection = None  # type: typing.Optional[Connection]
        self._global_transaction = None  # type: typing.Optional[Transaction]

        if self._force_rollback:
            self._global_connection = Connection(self._backend)
            self._global_transaction = self._global_connection.transaction(
                force_rollback=True
            )

    async def connect(self) -> None:
        """
        Establish the connection pool.
        """
        assert not self.is_connected, "Already connected."

        await self._backend.connect()
        self.is_connected = True

        if self._force_rollback:
            assert self._global_transaction is not None
            await self._global_transaction.__aenter__()

    async def disconnect(self) -> None:
        """
        Close all connections in the connection pool.
        """
        assert self.is_connected, "Already disconnected."

        if self._force_rollback:
            assert self._global_transaction is not None
            await self._global_transaction.__aexit__()

        await self._backend.disconnect()
        self.is_connected = False

    async def __aenter__(self) -> "Database":
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: typing.Type[BaseException] = None,
        exc_value: BaseException = None,
        traceback: TracebackType = None,
    ) -> None:
        await self.disconnect()

    async def fetch_all(self, query: ClauseElement) -> typing.List[RowProxy]:
        async with self.connection() as connection:
            return await connection.fetch_all(query=query)

    async def fetch_one(self, query: ClauseElement) -> RowProxy:
        async with self.connection() as connection:
            return await connection.fetch_one(query=query)

    async def execute(self, query: ClauseElement, values: dict = None) -> typing.Any:
        async with self.connection() as connection:
            return await connection.execute(query=query, values=values)

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        async with self.connection() as connection:
            return await connection.execute_many(query=query, values=values)

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[RowProxy, None]:
        async with self.connection() as connection:
            async for record in connection.iterate(query):
                yield record

    def connection(self) -> "Connection":
        if self._global_connection is not None:
            return self._global_connection

        try:
            return self._connection_context.get()
        except LookupError:
            connection = Connection(self._backend)
            self._connection_context.set(connection)
            return connection

    def transaction(self, *, force_rollback: bool = False) -> "Transaction":
        return self.connection().transaction(force_rollback=force_rollback)


class Connection:
    def __init__(self, backend: DatabaseBackend) -> None:
        self._backend = backend

        self._connection_lock = asyncio.Lock()
        self._connection = self._backend.connection()
        self._connection_counter = 0

        self._transaction_lock = asyncio.Lock()
        self._transaction_stack = []  # type: typing.List[Transaction]

    async def __aenter__(self) -> "Connection":
        async with self._connection_lock:
            self._connection_counter += 1
            if self._connection_counter == 1:
                await self._connection.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: typing.Type[BaseException] = None,
        exc_value: BaseException = None,
        traceback: TracebackType = None,
    ) -> None:
        async with self._connection_lock:
            assert self._connection is not None
            self._connection_counter -= 1
            if self._connection_counter == 0:
                await self._connection.release()

    async def fetch_all(self, query: ClauseElement) -> typing.Any:
        return await self._connection.fetch_all(query=query)

    async def fetch_one(self, query: ClauseElement) -> typing.Any:
        return await self._connection.fetch_one(query=query)

    async def execute(self, query: ClauseElement, values: dict = None) -> typing.Any:
        return await self._connection.execute(query, values)

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        await self._connection.execute_many(query, values)

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        async for record in self._connection.iterate(query):
            yield record

    def transaction(self, *, force_rollback: bool = False) -> "Transaction":
        return Transaction(self, force_rollback)


class Transaction:
    def __init__(self, connection: Connection, force_rollback: bool) -> None:
        self._connection = connection
        self._force_rollback = force_rollback
        self._transaction = connection._connection.transaction()

    async def __aenter__(self) -> "Transaction":
        """
        Called when entering `async with database.transaction()`
        """
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: typing.Type[BaseException] = None,
        exc_value: BaseException = None,
        traceback: TracebackType = None,
    ) -> None:
        """
        Called when exiting `async with database.transaction()`
        """
        if exc_type is not None or self._force_rollback:
            await self.rollback()
        else:
            await self.commit()

    def __await__(self) -> typing.Generator:
        """
        Called if using the low-level `transaction = await database.transaction()`
        """
        return self.start().__await__()

    def __call__(self, func: typing.Callable) -> typing.Callable:
        """
        Called if using `@database.transaction()` as a decorator.
        """

        @functools.wraps(func)
        async def wrapper(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            async with self:
                return await func(*args, **kwargs)

        return wrapper

    async def start(self) -> "Transaction":
        async with self._connection._transaction_lock:
            is_root = not self._connection._transaction_stack
            await self._connection.__aenter__()
            await self._transaction.start(is_root=is_root)
            self._connection._transaction_stack.append(self)
        return self

    async def commit(self) -> None:
        async with self._connection._transaction_lock:
            assert self._connection._transaction_stack[-1] is self
            self._connection._transaction_stack.pop()
            await self._transaction.commit()
            await self._connection.__aexit__()

    async def rollback(self) -> None:
        async with self._connection._transaction_lock:
            assert self._connection._transaction_stack[-1] is self
            self._connection._transaction_stack.pop()
            await self._transaction.rollback()
            await self._connection.__aexit__()


class _EmptyNetloc(str):
    def __bool__(self) -> bool:
        return True


class DatabaseURL:
    def __init__(self, url: typing.Union[str, "DatabaseURL"]):
        self._url = str(url)

    @property
    def components(self) -> SplitResult:
        if not hasattr(self, "_components"):
            self._components = urlsplit(self._url)
        return self._components

    @property
    def dialect(self) -> str:
        return self.components.scheme.split("+")[0]

    @property
    def driver(self) -> str:
        if "+" not in self.components.scheme:
            return ""
        return self.components.scheme.split("+", 1)[1]

    @property
    def username(self) -> typing.Optional[str]:
        return self.components.username

    @property
    def password(self) -> typing.Optional[str]:
        return self.components.password

    @property
    def hostname(self) -> typing.Optional[str]:
        return self.components.hostname

    @property
    def port(self) -> typing.Optional[int]:
        return self.components.port

    @property
    def netloc(self) -> typing.Optional[str]:
        return self.components.netloc

    @property
    def database(self) -> str:
        return self.components.path.lstrip("/")

    @property
    def options(self) -> dict:
        if not hasattr(self, "_options"):
            self._options = dict(parse_qsl(self.components.query))
        return self._options

    def replace(self, **kwargs: typing.Any) -> "DatabaseURL":
        if (
            "username" in kwargs
            or "password" in kwargs
            or "hostname" in kwargs
            or "port" in kwargs
        ):
            hostname = kwargs.pop("hostname", self.hostname)
            port = kwargs.pop("port", self.port)
            username = kwargs.pop("username", self.username)
            password = kwargs.pop("password", self.password)

            netloc = hostname
            if port is not None:
                netloc += f":{port}"
            if username is not None:
                userpass = username
                if password is not None:
                    userpass += f":{password}"
                netloc = f"{userpass}@{netloc}"

            kwargs["netloc"] = netloc

        if "database" in kwargs:
            kwargs["path"] = "/" + kwargs.pop("database")

        if "dialect" in kwargs or "driver" in kwargs:
            dialect = kwargs.pop("dialect", self.dialect)
            driver = kwargs.pop("driver", self.driver)
            kwargs["scheme"] = f"{dialect}+{driver}" if driver else dialect

        if not kwargs.get("netloc", self.netloc):
            # Using an empty string that evaluates as True means we end
            # up with URLs like `sqlite:///database` instead of `sqlite:/database`
            kwargs["netloc"] = _EmptyNetloc()

        components = self.components._replace(**kwargs)
        return self.__class__(components.geturl())

    def __str__(self) -> str:
        return self._url

    def __repr__(self) -> str:
        url = str(self)
        if self.password:
            url = str(self.replace(password="********"))
        return f"{self.__class__.__name__}({repr(url)})"
