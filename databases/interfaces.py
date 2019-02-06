import typing
from types import TracebackType

from sqlalchemy.sql import ClauseElement


class DatabaseBackend:
    async def connect(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def disconnect(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    def session(self) -> "DatabaseSession":
        raise NotImplementedError()  # pragma: no cover


class DatabaseSession:
    async def fetchall(self, query: ClauseElement) -> typing.Any:
        raise NotImplementedError()  # pragma: no cover

    async def fetchone(self, query: ClauseElement) -> typing.Any:
        raise NotImplementedError()  # pragma: no cover

    async def execute(self, query: ClauseElement) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def executemany(self, query: ClauseElement, values: list) -> None:
        raise NotImplementedError()  # pragma: no cover

    def transaction(self) -> "DatabaseTransaction":
        raise NotImplementedError()  # pragma: no cover


class DatabaseTransaction:
    async def __aenter__(self) -> None:
        await self.start()

    async def __aexit__(
        self,
        exc_type: typing.Type[BaseException] = None,
        exc_value: BaseException = None,
        traceback: TracebackType = None,
    ) -> None:
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()

    async def start(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def commit(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def rollback(self) -> None:
        raise NotImplementedError()  # pragma: no cover
