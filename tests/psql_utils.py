#
# Workaround for unusual PostgreSQL syntax, where functions act like columns.
# Recipe provided by zzzeek (Mike Bayer), author of SA.
# https://bitbucket.org/zzzeek/sqlalchemy/issues/3594/using-text-in-a-join
# https://bitbucket.org/zzzeek/sqlalchemy/issues/3566/figure-out-how-to-support-all-of-pgs#comment-23564706
#

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import functions
from sqlalchemy.sql.elements import ColumnClause
from sqlalchemy.sql.selectable import FromClause


class FunctionColumn(ColumnClause):
    def __init__(self, function, name, type_=None):
        self.function = self.table = function
        self.name = self.key = name
        self.key = self.name
        self.type_ = type_
        self.is_literal = False

    @property
    def _from_objects(self):
        return []

    def _make_proxy(
        self, selectable, name=None, attach=True, name_is_truncatable=False, **kw
    ):
        if self.name == self.function.name:
            name = selectable.name
        else:
            name = self.name

        co = ColumnClause(name, self.type)
        co.key = self.name
        co._proxies = [self]
        if selectable._is_clone_of is not None:
            co._is_clone_of = selectable._is_clone_of.columns.get(co.key)
        co.table = selectable
        co.named_with_table = False
        if attach:
            selectable._columns[co.key] = co
        return co


@compiles(FunctionColumn)
def _compile_function_column(element, compiler, **kw):
    if kw.get("asfrom", False):
        return "(%s).%s" % (
            compiler.process(element.function, **kw),
            compiler.preparer.quote(element.name),
        )
    else:
        return element.name


class ColumnFunction(functions.FunctionElement):
    __visit_name__ = "function"

    @property
    def columns(self):
        return FromClause.columns.fget(self)

    def _populate_column_collection(self):
        for name in self.column_names:
            self._columns[name] = FunctionColumn(self, name)


class unnest_func(ColumnFunction):
    name = "unnest"
    column_names = ["unnest", "ordinality"]


@compiles(unnest_func)
def _compile_unnest_func(element, compiler, **kw):
    return compiler.visit_function(element, **kw) + " WITH ORDINALITY"
