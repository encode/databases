import typing


class ConstructDefaultParamsMixin:
    """
    A mixin to support column default values for insert queries for asyncpg,
    aiomysql and aiosqlite
    """

    def construct_params(self, params=None, _group_number=None, _check=True):
        pd = super().construct_params(params, _group_number, _check)

        for column in self.prefetch:
            pd[column.key] = self._exec_default(column.default)

        return pd

    def _exec_default(self, default: typing.Any) -> typing.Any:
        if default.is_callable:
            return default.arg(self.dialect)
        else:
            return default.arg
