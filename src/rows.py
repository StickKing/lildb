"""Module contains row classes."""
from __future__ import annotations

from dataclasses import make_dataclass
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from .table import Table


class _RowOperationMixin:

    __slots__ = ()

    def delete(self) -> None:
        """Delete this row from db."""
        if isinstance(self, dict):
            if self.table.id_exist:
                self.table.delete(id=self["id"])
                return
            self.table.delete(**self)

        if self.table.id_exist:
            self.table.delete(id=self.id)
            return
        column_value = {
            name: getattr(self, name)
            for name in self.__slots__
            if name != "table"
        }
        self.table.delete(**column_value)


class RowDict(dict, _RowOperationMixin):
    """DB row like a dict."""

    def __init__(self, table: Table, **kwargs: dict) -> None:
        """Initialize dict row."""
        self.table = table
        if self.table is None:
            msg = "missing 1 required named argument: 'table'"
            raise TypeError(msg)
        super().__init__(kwargs)



def make_row_data_cls(table: Table) -> type:
    """Create data cls row for the transmitted table."""
    data_cls = make_dataclass(
        "RowDataClass",
        [*table.column_names, "table"],
        slots=True,
    )
    return type(
        f"Row{table.name.title()}DataClass",
        (data_cls, _RowOperationMixin),
        {"__slots__": data_cls.__slots__},
    )
