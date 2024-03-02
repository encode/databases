"""
All the unique changes for the databases package
with the custom Numeric as the deprecated pypostgresql
for backwards compatibility and to make sure the
package can go to SQLAlchemy 2.0+.
"""

import typing

from sqlalchemy import types, util
from sqlalchemy.dialects.postgresql.base import PGDialect, PGExecutionContext
from sqlalchemy.engine import processors
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.ddl import DDLElement
from sqlalchemy.types import Float, Numeric


class PGExecutionContext_psycopg(PGExecutionContext):
    ...


class PGNumeric(Numeric):
    def bind_processor(
        self, dialect: typing.Any
    ) -> typing.Union[str, None]:  # pragma: no cover
        return processors.to_str

    def result_processor(
        self, dialect: typing.Any, coltype: typing.Any
    ) -> typing.Union[float, None]:  # pragma: no cover
        if self.asdecimal:
            return None
        else:
            return processors.to_float


class PGDialect_psycopg(PGDialect):
    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            types.Numeric: PGNumeric,
            types.Float: Float,
        },
    )
    execution_ctx_cls = PGExecutionContext_psycopg


def get_dialect() -> Dialect:
    dialect = PGDialect_psycopg(paramstyle="pyformat")
    dialect.implicit_returning = True
    dialect.supports_native_enum = True
    dialect.supports_smallserial = True  # 9.2+
    dialect._backslash_escapes = False
    dialect.supports_sane_multi_rowcount = True  # psycopg 2.0.9+
    dialect._has_native_hstore = True
    dialect.supports_native_decimal = True
    return dialect


def compile_query(
    query: ClauseElement, dialect: Dialect
) -> typing.Tuple[str, list, tuple]:
    compiled = query.compile(
        dialect=dialect, compile_kwargs={"render_postcompile": True}
    )

    if not isinstance(query, DDLElement):
        compiled_params = sorted(compiled.params.items())

        mapping = {
            key: "$" + str(i) for i, (key, _) in enumerate(compiled_params, start=1)
        }
        compiled_query = compiled.string % mapping

        processors = compiled._bind_processors
        args = [
            processors[key](val) if key in processors else val
            for key, val in compiled_params
        ]
        result_map = compiled._result_columns
    else:
        compiled_query = compiled.string
        args = []
        result_map = None

    return compiled_query, args, result_map
