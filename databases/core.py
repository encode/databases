import asyncio
import sys
import typing
from types import TracebackType
from urllib.parse import SplitResult, urlsplit

from sqlalchemy.sql import ClauseElement

from databases.importer import import_from_string
from databases.interfaces import ConnectionBackend, DatabaseBackend, TransactionBackend

if sys.version_info >= (3, 7):  # pragma: no cover
    from contextvars import ContextVar
else:  # pragma: no cover
    from aiocontextvars import ContextVar


class DatabaseURL:
    def __init__(self, url: typing.Union[str, "DatabaseURL"]):
        if isinstance(url, DatabaseURL):
            self._url = str(url)
        else:
            self._url = url

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
    def database(self) -> str:
        return self.components.path.lstrip("/")

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

        components = self.components._replace(**kwargs)
        return self.__class__(components.geturl())

    def __str__(self) -> str:
        return self._url

    def __repr__(self) -> str:
        url = str(self)
        if self.password:
            url = str(self.replace(password="********"))
        return f"{self.__class__.__name__}({repr(url)})"


class Database:
    SUPPORTED_BACKENDS = {
        "postgresql": "databases.backends.postgres:PostgresBackend",
        "mysql": "databases.backends.mysql:MySQLBackend",
    }

    def __init__(self, url: typing.Union[str, DatabaseURL]):
        self.url = DatabaseURL(url)
        self.is_connected = False

        backend_str = self.SUPPORTED_BACKENDS[self.url.dialect]
        backend_cls = import_from_string(backend_str)
        assert issubclass(backend_cls, DatabaseBackend)
        self._backend = backend_cls(self.url)
        self._connection_context = ContextVar("connection_context")  # type: ContextVar

    async def connect(self) -> None:
        if not self.is_connected:
            await self._backend.connect()
            self.is_connected = True

    async def disconnect(self) -> None:
        if self.is_connected:
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

    async def fetch_all(self, query: ClauseElement) -> typing.Any:
        async with self.connection() as connection:
            return await connection.fetch_all(query=query)

    async def fetch_one(self, query: ClauseElement) -> typing.Any:
        async with self.connection() as connection:
            return await connection.fetch_one(query=query)

    async def execute(self, query: ClauseElement, values: dict = None) -> None:
        async with self.connection() as connection:
            return await connection.execute(query=query, values=values)

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        async with self.connection() as connection:
            return await connection.execute_many(query=query, values=values)

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        async with self.connection() as connection:
            async for record in connection.iterate(query):
                yield record

    def connection(self) -> "Connection":
        try:
            return self._connection_context.get()
        except LookupError:
            connection = Connection(self._connection_context, self._backend)
            self._connection_context.set(connection)
            return connection

    def transaction(self, force_rollback: bool = False) -> "Transaction":
        return self.connection().transaction(force_rollback)


class Connection:
    def __init__(self, contextvar: ContextVar, backend: DatabaseBackend) -> None:
        self._context_var = contextvar
        self._backend = backend
        self._lock = asyncio.Lock()
        self._connection = self._backend.connection()
        self._transaction_stack = []
        self._counter = 0

    async def __aenter__(self) -> "Connection":
        # async with self._lock:
        #     print('got lock')
        self._counter += 1
        if self._counter == 1:
            await self._connection.acquire()
        return self

    async def __aexit__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        # async with self._lock:
        #     print('got lock')
        assert self._connection is not None
        self._counter -= 1
        if self._counter == 0:
            await self._connection.release()

    async def fetch_all(self, query: ClauseElement) -> typing.Any:
        return await self._connection.fetch_all(query=query)

    async def fetch_one(self, query: ClauseElement) -> typing.Any:
        return await self._connection.fetch_one(query=query)

    async def execute(self, query: ClauseElement, values: dict = None) -> None:
        await self._connection.execute(query, values)

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        await self._connection.execute_many(query, values)

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        async for record in self._connection.iterate(query):
            yield record

    def transaction(self, force_rollback: bool = False) -> "Transaction":
        return Transaction(self, force_rollback)


class Transaction:
    def __init__(self, connection: Connection, force_rollback: bool) -> None:
        self._connection = connection
        self._force_rollback = force_rollback
        self._transaction = connection._connection.transaction()

    async def __aenter__(self) -> "Transaction":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: typing.Type[BaseException] = None,
        exc_value: BaseException = None,
        traceback: TracebackType = None,
    ) -> None:
        if exc_type is not None or self._force_rollback:
            await self.rollback()
        else:
            await self.commit()

    def __await__(self) -> typing.Generator:
        return self.start().__await__()

    async def start(self) -> "Transaction":
        # async with self._connection._lock:
        is_root = not self._connection._transaction_stack
        await self._connection.__aenter__()
        await self._transaction.start(is_root=is_root)
        self._connection._transaction_stack.append(self)
        return self

    async def commit(self) -> None:
        # async with self._connection._lock:
        assert self._connection._transaction_stack[-1] is self
        self._connection._transaction_stack.pop()
        await self._transaction.commit()
        await self._connection.__aexit__()

    async def rollback(self) -> None:
        # async with self._connection._lock:
        assert self._connection._transaction_stack[-1] is self
        self._connection._transaction_stack.pop()
        await self._transaction.rollback()
        await self._connection.__aexit__()
