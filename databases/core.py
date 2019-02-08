import sys
import typing
from types import TracebackType
from urllib.parse import SplitResult, urlsplit

from sqlalchemy.sql import ClauseElement

from databases.importer import import_from_string
from databases.interfaces import DatabaseBackend, DatabaseSession, DatabaseTransaction


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
    backends = {
        "postgresql": "databases.backends.postgres:PostgresBackend",
        "mysql": "databases.backends.mysql:MySQLBackend",
    }

    def __init__(self, url: typing.Union[str, DatabaseURL]):
        self.url = DatabaseURL(url)
        backend_str = self.backends[self.url.dialect]
        backend_cls = import_from_string(backend_str)
        assert issubclass(backend_cls, DatabaseBackend)
        self.backend = backend_cls(self.url)
        self.is_connected = False
        self.session_context = ContextVar("session_context")  # type: ContextVar

    async def connect(self) -> None:
        if not self.is_connected:
            await self.backend.connect()
            self.is_connected = True

    async def disconnect(self) -> None:
        if self.is_connected:
            await self.backend.disconnect()
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
        with SessionContext(self.session_context, self.backend) as session:
            return await session.fetch_all(query=query)

    async def fetch_one(self, query: ClauseElement) -> typing.Any:
        with SessionContext(self.session_context, self.backend) as session:
            return await session.fetch_one(query=query)

    async def execute(self, query: ClauseElement, values: dict = None) -> None:
        with SessionContext(self.session_context, self.backend) as session:
            return await session.execute(query=query, values=values)

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        with SessionContext(self.session_context, self.backend) as session:
            return await session.execute_many(query=query, values=values)

    def transaction(self, force_rollback: bool = False) -> DatabaseTransaction:
        return TransactionContext(self.session_context, self.backend, force_rollback)


class SessionContext:
    def __init__(self, context: ContextVar, backend: DatabaseBackend) -> None:
        self.context = context
        self.backend = backend

    def __enter__(self) -> DatabaseSession:
        current_session, counter = self.context.get((None, 0))
        if current_session is None:
            self.session = self.backend.session()
        else:
            self.session = current_session
        counter += 1
        self.context.set((self.session, counter))
        return self.session

    def __exit__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        current_session, counter = self.context.get((None, 0))
        assert current_session is self.session
        assert counter > 0
        counter -= 1
        if counter == 0:
            current_session = None
        self.context.set((current_session, counter))


class TransactionContext(DatabaseTransaction):
    def __init__(
        self, context: ContextVar, backend: DatabaseBackend, force_rollback: bool
    ) -> None:
        self.session_context = SessionContext(context, backend)
        super().__init__(force_rollback)

    async def start(self) -> None:
        session = self.session_context.__enter__()
        self.transaction = session.transaction(force_rollback=self.force_rollback)
        await self.transaction.start()

    async def commit(self) -> None:
        await self.transaction.commit()
        self.session_context.__exit__()

    async def rollback(self) -> None:
        await self.transaction.rollback()
        self.session_context.__exit__()
