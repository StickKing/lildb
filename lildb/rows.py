"""Module contains row classes."""
from __future__ import annotations

import sys
from abc import ABC
from abc import abstractmethod
from collections import defaultdict
from dataclasses import _process_class  # type: ignore
from dataclasses import field
from dataclasses import fields
from dataclasses import make_dataclass
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Iterable
from typing import Literal
from typing import Sequence
from typing import TypeVar


if TYPE_CHECKING:
    from .table.table import Table


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


class _BaseRowDataClsMixin(ABCRow):
    """Mixin for realize change control in row."""

    def __repr__(self: Any) -> str:
        """View string by obj."""
        columns = ", ".join(
            f"{atr_name}={getattr(self, atr_name)}"
            for atr_name in self.table.column_names
        )
        return f"{self.__class__.__name__}({columns})"


class _RowDataClsMixin(_BaseRowDataClsMixin):
    """Row data cls mixin."""

    def __setattr__(self, name: str, value: Any) -> None:
        """Check changed attribute for updating and deleting row."""
        if (
            hasattr(self, "changed_columns") and
            self.changed_columns is not None and
            self.table is not None and
            name.startswith("_") is False and
            name in self.table.column_names
        ):
            old_value = getattr(self, name)
            super().__setattr__(name, value)
            if value != old_value:
                self.changed_columns.add(name)
            return
        super().__setattr__(name, value)

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


def contain_relation_objects(orm_object: _RowORMModelMixin) -> bool:
    """Check object contain new relation objects."""
    return len(orm_object._relation_object_add_funcs) > 0


def refresh_old_obj_by_new(
    table: Table,
    old_orm_object: _RowORMModelMixin,
    new_orm_object: _RowORMModelMixin,
) -> None:
    """Move relation funcs to other object."""
    for name in table.column_names:
        col_name = f"_column_data_{name}_"
        setattr(
            old_orm_object,
            col_name,
            getattr(new_orm_object, col_name)
        )


def process_add_relation_objects(
    orm_object: _RowORMModelMixin,
    ref_type: Literal["Relation", "RelationForeignKey"] | None = None,
) -> None:
    """Add relation object."""
    if ref_type is None:
        for funcs in orm_object._relation_object_add_funcs.values():
            for func in funcs:
                func()
        orm_object._relation_object_add_funcs = defaultdict(list)
        return

    for func in orm_object._relation_object_add_funcs[ref_type]:
        func()

    orm_object._relation_object_add_funcs.pop(ref_type)


class _RowORMModelMixin(_BaseRowDataClsMixin):
    """ORM row mixin."""

    __table_name__: str
    __relation_fields__: tuple[str]

    def __init__(self, **kwargs: Any) -> None:
        """Initialize orm object."""
        super().__setattr__("_is_init", True)

        self.table = kwargs.pop("table", None)
        self.changed_columns = set()
        self._relation_object_add_funcs: defaultdict[list] = defaultdict(list)

        row_fields = {
            field.name
            for field in fields(self)
        }
        row_fields.update(self.__relation_fields__)

        for key, value in kwargs.items():
            if key not in row_fields:
                continue
            setattr(self, key, value)
            row_fields.remove(key)

        for name in row_fields:
            object.__setattr__(self, f"_column_data_{name}_", None)

        self.orm_obj = True
        self._is_init = False

    def get_row_data_as_dict(self) -> dict:
        """Return row data like dict."""
        return {
            name: getattr(self, f"_column_data_{name}_")
            for name in self.table.column_names
            if getattr(self, f"_column_data_{name}_") is not None
        }

    def __setattr__(self, name: str, value: Any) -> None:
        """Check changed attribute for updating and deleting row."""
        if self._is_init:
            super().__setattr__(name, value)
            return

        if (
            hasattr(self, "changed_columns") and
            self.changed_columns is not None and
            self.table is not None and
            name.startswith("_") is False and
            name in self.table.column_names
        ):
            old_value = getattr(self, name)
            super().__setattr__(name, value)
            if value != old_value:
                self.changed_columns.add(name)
            return
        super().__setattr__(name, value)

    @property
    def not_changed_column_values(self) -> dict[str, Any]:
        """Fetch not changed column name with value like dict."""
        not_change_column = set(self.table.column_names) - self.changed_columns
        return {
            name: getattr(self, f"_column_data_{name}_")
            for name in self.table.column_names
            if name in not_change_column
        }

    @property
    def changed_column_values(self) -> dict[str, Any]:
        """Fetch changed column name with value like dict."""
        return {
            name: getattr(self, f"_column_data_{name}_")
            for name in self.table.column_names
            if name in self.changed_columns
        }

    def change(self) -> None:
        """Update this row."""
        process_add_relation_objects(self)
        super().change()


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


def make_row_data_cls(
    table_name: str,
    column_names: Sequence[str],
    bases: Sequence[type[Any]] | None = None,
    *,
    default_none: bool = True,
    create_orm_model: bool = False,
) -> type[Any]:
    """Create data cls row for the transmitted table."""
    attributes = []
    init = True

    if bases is None:
        if create_orm_model is False:
            bases = [_RowDataClsMixin]
        else:
            bases = [_RowORMModelMixin]
    else:
        if create_orm_model is False:
            bases = [_RowDataClsMixin, *bases]
        else:
            bases = [_RowORMModelMixin, *bases]

    if create_orm_model:
        attributes: list[tuple[str, Any, field]] = [
            (atr, Any)
            for atr in column_names
        ]
        init = False
    else:
        attributes: list[tuple[str, Any, field]] = [
            (atr, Any, field(default=None))
            for atr in column_names
        ]

        attributes.extend([
            ("changed_columns", set, field(default_factory=lambda: set())),
            ("table", Any, field(default=None))
        ])

    return make_dataclass(
        f"Row{table_name}DataClass",
        attributes,
        repr=False,
        bases=(*bases,),
        init=init,
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
    columns = list(columns_name)
    columns.append("table")
    return make_dataclass(
        "ResultRow",
        columns,
        frozen=True,
    )


def dataclass_row(  # noqa: PLR0913
    cls: None | type = None,
    /,
    *,
    init: bool = True,
    repr: bool = True,
    eq: bool = True,
    order: bool = False,
    unsafe_hash: bool = False,
    frozen: bool = False,
    **kwargs: Any,
) -> type | Callable[[type], type]:
    """Make custom row dataclass with mixin, repr
    and arguments: 'table', 'changed_columns'.
    """
    python_version = sys.version_info

    if python_version.major == 3 and python_version.minor >= 10:
        kwargs["match_args"] = kwargs.get("match_args", True)
        kwargs["kw_only"] = kwargs.get("kw_only", False)
        kwargs["slots"] = kwargs.get("slots", False)

    if python_version.major == 3 and python_version.minor >= 11:
        kwargs["weakref_slot"] = kwargs.get("weakref_slot", False)

    def wrap(cls: type) -> type:
        cls.__annotations__["table"] = Any
        cls.__annotations__["changed_columns"] = set
        cls.table = field(default=None)  # type: ignore
        cls.changed_columns = field(default_factory=lambda: set())

        # TODO (stickking): Remove _process_class and take wrap
        # 0000
        new_cls = _process_class(
            cls,
            init,
            repr,
            eq,
            order,
            unsafe_hash,
            frozen,
            **kwargs,
        )
        # TODO (stickking): Why dict if slots?!?!
        # 0000
        return type(
            new_cls.__name__,
            (cls, _RowDataClsMixin),
            {} if hasattr(cls, "__dict__")
            else {"__slots__": new_cls.__slots__},
        )

    # See if we're being called as @dataclass or @dataclass().
    if cls is None:
        # We're called with parens.
        return wrap

    # We're called as @dataclass without parens.
    cls = wrap(cls)

    if repr is False:
        cls.__repr__ = cls.__str__

    return cls
