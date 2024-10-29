"""Module contains row classes."""
from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from dataclasses import field
from dataclasses import make_dataclass
from sqlite3 import OperationalError
from typing import TYPE_CHECKING
from typing import Any
from typing import NoReturn


if TYPE_CHECKING:
    from .table import Table


__all__ = (
    "ABCRow",
    "RowDict",
    "_RowDataClsMixin",
    "make_row_data_cls",
)


class ABCRow(ABC):
    """Abstract row interface."""

    table: Table
    changed_columns: set

    @property
    @abstractmethod
    def not_changed_column_values(self) -> dict[str, Any]:
        """Fetch not changed column name with value like dict."""
        ...

    @property
    @abstractmethod
    def changed_column_values(self) -> dict[str, Any]:
        """Fetch changed column name with value like dict."""
        ...

    def delete(self) -> None:
        """Delete this row from db."""
        self.table.delete(**self.not_changed_column_values)

    def change(self) -> None:
        """Update this row."""
        if not self.changed_columns:
            return
        self.table.update(
            self.changed_column_values,
            **self.not_changed_column_values,
        )
        self.changed_columns = set()


class _RowDataClsMixin(ABCRow):
    """Mixin for realize change control in row."""

    @property
    def not_changed_column_values(self) -> dict[str, Any]:
        """Fetch not changed column name with value like dict."""
        not_change_column = set(self.table.column_names) - self.changed_columns
        return {
            name: getattr(self, name)
            for name in self.table.column_names
            if name in not_change_column
        }

    @property
    def changed_column_values(self) -> dict[str, Any]:
        """Fetch changed column name with value like dict."""
        return {
            name: getattr(self, name)
            for name in self.table.column_names
            if name in self.changed_columns
        }

    def __setattr__(self, name: str, value: Any) -> None:
        if hasattr(self, name) and self.changed_columns is not None:
            old_value = getattr(self, name)
            super().__setattr__(name, value)
            if value != old_value:
                self.changed_columns.add(name)
            return
        super().__setattr__(name, value)


class RowDict(ABCRow, dict):
    """DB row like a dict."""

    def __init__(
        self,
        table: Table,
        changed_columns: set,
        **kwargs: Any,
    ) -> None:
        """Initialize dict row."""
        self.table = table
        self.changed_columns = changed_columns
        if self.table is None:
            msg = "missing 1 required named argument: 'table'"
            raise TypeError(msg)
        super().__init__(**kwargs)

    @property
    def not_changed_column_values(self) -> dict[str, Any]:
        """Fetch not changed column name with value like dict."""
        not_change_column = set(self.table.column_names) - self.changed_columns
        return {
            key: value
            for key, value in self.items()
            if key in not_change_column
        }

    @property
    def changed_column_values(self) -> dict[str, Any]:
        """Fetch changed column name with value like dict."""
        return {
            key: value
            for key, value in self.items()
            if key in self.changed_columns
        }

    def __setitem__(self, key: str, value: int | str | bool) -> None:
        """Check changes columns."""
        if self[key] != value and key in self.table.column_names:
            self.changed_columns.add(key)
        super().__setitem__(key, value)


def make_row_data_cls(table: Table) -> type:
    """Create data cls row for the transmitted table."""
    attributes = [
        (atr, Any, field(default=None))
        for atr in [*table.column_names, "table", "changed_columns"]
    ]

    def repr(self: data_cls) -> str:
        """View sting by object."""
        columns = ", ".join(
            f"{atr_name}={getattr(self, atr_name)}"
            for atr_name in self.table.column_names
            # if atr_name not in {"table", "changed_columns"}
        )
        return f"{self.__class__.__name__}({columns})"

    data_cls = make_dataclass(
        "RowDataClass",
        attributes,
        # slots=True,
        repr=False,
        namespace={"repr": repr},
    )

    data_cls.__repr__ = repr

    return type(
        f"Row{table.name.title()}DataClass",
        (data_cls, _RowDataClsMixin),
        {},
        # {"__slots__": data_cls.__slots__},
    )


def create_result_row(cls: type[ABCRow]) -> type[ABCRow]:
    """Create result row cls."""
    row_cls = type(
        "ResultRow",
        (cls,),
        {},
    )
    msg = "ResultRow cannot be changed"
    def operation_error(self: row_cls) -> NoReturn:
        """Change delete/update row method."""
        raise OperationalError(msg)
    row_cls.delete = row_cls.change = operation_error
    return row_cls
