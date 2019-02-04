import typing
from types import TracebackType
from urllib.parse import SplitResult, parse_qsl, urlencode, urlsplit


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
    def __init__(self, url: typing.Union[str, DatabaseURL]):
        from databases.backends.postgres import PostgresBackend
        self.url = DatabaseURL(url)
        self.backend = PostgresBackend(self.url)
        self.connected = False

    async def connect(self):
        if not self.connected:
            await self.backend.startup()
            self.connected = True

    async def disconnect(self):
        if self.connected:
            await self.backend.shutdown()
            self.connected = False

    def session(self, rollback_isolation: bool=False):
        return self.backend.session(rollback_isolation=rollback_isolation)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: typing.Type[BaseException] = None,
        exc_value: BaseException = None,
        traceback: TracebackType = None):
        await self.disconnect()
