import typing

from sqlalchemy.sql import ClauseElement


class DatabaseBackend:
    async def connect(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def disconnect(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    def session(self) -> "DatabaseSession":
        raise NotImplementedError()  # pragma: no cover


class DatabaseSession:
    async def fetch_all(self, query: ClauseElement) -> typing.Any:
        raise NotImplementedError()  # pragma: no cover

    async def fetch_one(self, query: ClauseElement) -> typing.Any:
        raise NotImplementedError()  # pragma: no cover

    async def execute(self, query: ClauseElement, values: dict = None) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        raise NotImplementedError()  # pragma: no cover

    def transaction(self) -> "DatabaseTransaction":
        raise NotImplementedError()  # pragma: no cover

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        raise NotImplementedError()  # pragma: no cover
        # mypy needs async iterators to contain a `yield`
        # https://github.com/python/mypy/issues/5385#issuecomment-407281656
        yield True  # pragma: no cover

    async def raw(
        self, query: ClauseElement, **kwargs: typing.Any
    ) -> typing.AsyncGenerator[typing.Any, None]:
        raise NotImplementedError()  # pragma: no cover
        yield True  # pragma: no cover


class DatabaseTransaction:
    async def start(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def commit(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def rollback(self) -> None:
        raise NotImplementedError()  # pragma: no cover
