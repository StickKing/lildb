"""Module contains row classes."""
from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from dataclasses import _process_class  # type: ignore
from dataclasses import field
from dataclasses import make_dataclass
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Iterable
from typing import TypeVar


if TYPE_CHECKING:
    from .table import Table


TRow = TypeVar("TRow", bound="ABCRow")


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
        """Check changed attribute for updating and deleting row."""
        if (
            hasattr(self, "changed_columns") and
            self.changed_columns is not None and
            self.table is not None
        ):
            old_value = getattr(self, name)
            super().__setattr__(name, value)
            if value != old_value:
                self.changed_columns.add(name)
            return
        super().__setattr__(name, value)

    def __repr__(self: Any) -> str:
        """View string by obj."""
        columns = ", ".join(
            f"{atr_name}={getattr(self, atr_name)}"
            for atr_name in self.table.column_names
        )
        return f"{self.__class__.__name__}({columns})"


class RowDict(ABCRow, dict):
    """DB row like a dict."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize."""
        self.table = kwargs.pop("table")
        self.changed_columns = set()
        if self.table is None:
            msg = "missing 1 required named argument: 'table'"
            raise TypeError(msg)
        super().__init__(*args, **kwargs)

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
    attributes: list[tuple[str, Any, field]] = [
        (atr, Any, field(default=None))
        for atr in [*table.column_names, "table"]
    ]
    attributes.append(
        ("changed_columns", set, field(default_factory=lambda: set()))
    )

    return make_dataclass(
        f"Row{table.name.title()}DataClass",
        attributes,
        repr=False,
        bases=(_RowDataClsMixin,),
    )

    # data_cls.__repr__ = repr

    # return type(
    #     f"Row{table.name.title()}DataClass",
    #     (data_cls, _RowDataClsMixin),
    #     {},
    #     # {"__slots__": data_cls.__slots__},
    # )


def create_result_row(columns_name: Iterable[str]) -> type[Any]:
    """Create result row cls."""
    return make_dataclass(
        "ResultRow",
        columns_name,
    )


def dataclass_table(  # noqa: PLR0913
    cls: None | type = None,
    /,
    *,
    init: bool = True,
    repr: bool = True,  # noqa: A002
    eq: bool = True,
    order: bool = False,
    unsafe_hash: bool = False,
    frozen: bool = False,
) -> type | Callable[[type], type]:
    """Make custom row dataclass with mixin, repr
    and arguments: 'table', 'changed_columns'.
    """
    cls.__annotations__["table"] = Any
    cls.__annotations__["changed_columns"] = set
    cls.table = field(default=None)  # type: ignore
    cls.changed_columns = field(default_factory=lambda: set())  # type: ignore

    def wrap(cls: type) -> type:
        return _process_class(cls, init, False, eq, order, unsafe_hash, frozen)

    # See if we're being called as @dataclass or @dataclass().
    if cls is None:
        # We're called with parens.
        return wrap

    # We're called as @dataclass without parens.
    cls = wrap(cls)

    cls = type(
        cls.__name__,
        (cls, _RowDataClsMixin),
        {},
    )

    if repr is False:
        cls.__repr__ = cls.__str__

    return cls
