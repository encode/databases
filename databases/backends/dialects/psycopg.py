"""
All the unique changes for the databases package
with the custom Numeric as the deprecated pypostgresql
for backwards compatibility and to make sure the
package can go to SQLAlchemy 2.0+.
"""

from sqlalchemy import types, util
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.engine import processors
from sqlalchemy.types import Float, Numeric


class PGNumeric(Numeric):
    def bind_processor(self, dialect):
        return processors.to_str

    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            return None
        else:
            return processors.to_float


class PGDialect_psycopg(PGDialect):
    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            types.Numeric: PGNumeric,
            # prevents PGNumeric from being used
            types.Float: Float,
        },
    )
