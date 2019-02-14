import typing

from sqlalchemy.engine import RowProxy
from sqlalchemy.sql import ClauseElement


class DatabaseBackend:
    async def connect(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def disconnect(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    def connection(self) -> "ConnectionBackend":
        raise NotImplementedError()  # pragma: no cover


class ConnectionBackend:
    async def acquire(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def release(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def fetch_all(self, query: ClauseElement) -> typing.List[RowProxy]:
        raise NotImplementedError()  # pragma: no cover

    async def fetch_one(self, query: ClauseElement) -> RowProxy:
        raise NotImplementedError()  # pragma: no cover

    async def execute(self, query: ClauseElement, values: dict = None) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def execute_many(self, query: ClauseElement, values: list) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[RowProxy, None]:
        raise NotImplementedError()  # pragma: no cover
        # mypy needs async iterators to contain a `yield`
        # https://github.com/python/mypy/issues/5385#issuecomment-407281656
        yield True  # pragma: no cover

    def transaction(self) -> "TransactionBackend":
        raise NotImplementedError()  # pragma: no cover


class TransactionBackend:
    async def start(self, is_root: bool) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def commit(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    async def rollback(self) -> None:
        raise NotImplementedError()  # pragma: no cover
