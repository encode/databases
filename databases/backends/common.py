import typing

from sqlalchemy import ColumnDefault
from sqlalchemy.engine.default import DefaultDialect


class ConstructDefaultParamsMixin:
    """
    A mixin to support column default values for insert queries for asyncpg,
    aiomysql and aiosqlite
    """

    prefetch: typing.List
    dialect: DefaultDialect

    def construct_params(
        self,
        params: typing.Optional[typing.Mapping] = None,
        _group_number: typing.Any = None,
        _check: bool = True,
    ) -> typing.Dict:
        pd = super().construct_params(params, _group_number, _check)  # type: ignore

        for column in self.prefetch:
            pd[column.key] = self._exec_default(column.default)

        return pd

    def _exec_default(self, default: ColumnDefault) -> typing.Any:
        if default.is_callable:
            return default.arg(self.dialect)
        else:
            return default.arg
