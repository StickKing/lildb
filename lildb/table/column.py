"""Module contain components for work with db table."""
from __future__ import annotations

from numbers import Number
from typing import TYPE_CHECKING
from typing import Any
from typing import Iterable

from lildb.operations import Query

from ..sql import func


if TYPE_CHECKING:
    from ..sql import TFunc
    from ..sql import TFuncCLS
    from .table import Table


__all__ = (
    "ResultComparison",
    "Column",
    "Columns",
)


class ResultComparison(str):
    """The result of the comparison."""

    def _create_operation(
        self,
        operation: str,
        value: ResultComparison,
    ) -> ResultComparison:
        return ResultComparison(f"{value} {operation} {self}")

    def __and__(self, value: ResultComparison) -> ResultComparison:
        """."""
        return self._create_operation("AND", value)

    def __or__(self, value: ResultComparison) -> ResultComparison:
        """."""
        return self._create_operation("OR", value)

    def __rand__(self, value: ResultComparison) -> ResultComparison:
        """."""
        return self._create_operation("AND", value)

    def __ror__(self, value: ResultComparison) -> ResultComparison:
        """."""
        return self._create_operation("OR", value)


class Column:
    """Column."""

    __slots__ = (
        "_table",
        "_column_name",
        "_full_column_name",
        "complete_label",
    )

    def __init__(self, table: Table, column_name: str) -> None:
        """Initialize."""
        self._table = table
        self._column_name = column_name
        self.complete_label = column_name
        self._full_column_name = "`{}`.{}".format(
            self._table.name,
            self._column_name,
        )

    def _create_operation(
        self,
        operation: str,
        value: Any,
        *,
        in_operation: bool = False
    ) -> ResultComparison:
        """."""
        if in_operation:
            result = "{} {} {}".format(
                self._full_column_name,
                operation,
                value,
            )
            return ResultComparison(result)
        if isinstance(value, str):
            result = "{} {} '{}'".format(
                self._full_column_name,
                operation,
                value,
            )
            return ResultComparison(result)
        if value is None and operation == "=":
            return ResultComparison(
                "{} is NULL".format(
                    self._full_column_name,
                )
            )
        elif value is None:
            value = "NULL"
        result = "{} {} {}".format(
            self._full_column_name,
            operation,
            value,
        )
        return ResultComparison(result)

    def __eq__(self, value: object) -> ResultComparison:
        """."""
        return self._create_operation("=", value)

    def __ne__(self, value: object) -> ResultComparison:
        """."""
        return self._create_operation("!=", value)

    def __lt__(self, value: object) -> ResultComparison:
        """."""
        return self._create_operation("<", value)

    def __le__(self, value: object) -> ResultComparison:
        """."""
        return self._create_operation("<=", value)

    def __gt__(self, value: object) -> ResultComparison:
        """."""
        return self._create_operation(">", value)

    def __ge__(self, value: object) -> ResultComparison:
        """."""
        return self._create_operation(">=", value)

    def is_(self, value: object):
        """."""
        return self._create_operation("IS", value)

    def is_not(self, value: object):
        """."""
        return self._create_operation("IS NOT", value)

    def in_(self, value: Iterable | Query) -> str:
        """Realization in operation."""
        if not value:
            msg = "Value could not be empty"
            raise ValueError(msg)
        if isinstance(value, Query):
            value = "({})".format(str(value))
        elif isinstance(value[0], Number):
            value = "(" + ", ".join(str(i) for i in value) + ")"
        elif isinstance(value[0], str):
            value = "(" + ", ".join(f"'{i}'"for i in value) + ")"
        return self._create_operation("IN", value, in_operation=True)

    def not_in(self, value: Iterable | Query) -> str:
        """Realization in operation."""
        query = self.in_(value)
        return query.replace("IN", "NOT IN")

    def __getattr__(self, name: TFunc) -> type[TFuncCLS]:
        """Realize sql funcs."""
        FUNC_NAMES = {
            "avg",
            "sum",
            "min",
            "max",
            "count",
            "lower",
            "upper",
            "length",
        }
        if name not in FUNC_NAMES:
            msg = "{} {} {}".format(
                self.__class__.__name__,
                "object has no attribute",
                name,
            )
            raise AttributeError(msg)
        func_cls = getattr(func, name)
        return func_cls(self._full_column_name).label(self._column_name)

    def __str__(self) -> str:
        """Return column full name."""
        return self._full_column_name


class Columns:

    def __init__(self, table: Table) -> None:
        """Initialize."""
        self._table = table

    def __getattr__(self, name: str) -> Column:
        """Return column object."""
        if name not in self._table.column_names:
            msg = "{} {} {}".format(
                self.__class__.__name__,
                "object has no attribute",
                name,
            )
            raise AttributeError(msg)
        return Column(self._table, name)
