"""Module contains functions that generate an ORM model of the database."""
from __future__ import annotations

import datetime
from collections import defaultdict
from typing import Any
from typing import ClassVar
from typing import Dict
from typing import List
from typing import Type
from typing import TypedDict
from typing import Union

from typing_extensions import NotRequired
from typing_extensions import TypeAlias
from typing_extensions import get_args
from typing_extensions import get_origin
from typing_extensions import get_type_hints

from ..column_types import BaseType
from ..column_types import Blob
from ..column_types import Date
from ..column_types import DateTime
from ..column_types import ForeignKey
from ..column_types import Integer
from ..column_types import Real
from ..column_types import Text
from ..column_types import Time
from ..orm import MColumn
from ..orm import Relation
from ..orm import TColumn
from ..rows import _BaseRowDataClsMixin
from .utils import process_add_relation_objects


TForeignKeys: TypeAlias = List[ForeignKey]
TTableColumns: TypeAlias = Dict[str, BaseType]
TModelClass: TypeAlias = Type[object]


class TableData(TypedDict):
    """Data for creating the table."""

    table_name: str
    columns: TTableColumns
    foreign_keys: NotRequired[TForeignKeys]


TYPE_MAPPER = {
    # Optional types
    Union[int, None]: Integer,
    Union[str, None]: Text,
    Union[bytes, None]: Blob,
    Union[float, None]: Real,
    Union[datetime.datetime, None]: DateTime,
    Union[datetime.date, None]: Date,
    Union[datetime.time, None]: Time,
    # Not null types
    int: lambda: Integer(nullable=False),
    str: lambda: Text(nullable=False),
    bytes: lambda: Blob(nullable=False),
    float: lambda: Real(nullable=False),
    datetime.datetime: lambda: DateTime(nullable=False),
    datetime.date: lambda: Date(nullable=False),
    datetime.time: lambda: Time(nullable=False),
}


class _RowORMModelMixin(_BaseRowDataClsMixin):
    """ORM row mixin."""

    __table_name__: ClassVar[str]
    __relation_fields__: ClassVar[tuple[str, ...]]
    __column_fields__: ClassVar[tuple[str, ...]]

    def __init__(self, **kwargs: Any) -> None:
        """Initialize orm object."""
        super().__setattr__("_is_init", True)

        self.table = kwargs.pop("table", None)
        self.changed_columns = set()
        self._relation_events: defaultdict[str, list] = (
            defaultdict(list)
        )

        column_fields = set(self.__column_fields__)
        column_fields.update(self.__relation_fields__)

        for key, value in kwargs.items():
            if key not in column_fields:
                continue
            setattr(self, key, value)
            column_fields.remove(key)

        for name in column_fields:
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

    def delete(self) -> None:
        """Delete this row from db."""
        self.table.delete(**self.not_changed_column_values)


def _get_table_columns(
    model_cls: TModelClass,
    table_columns: TTableColumns,
    foreign_keys: TForeignKeys,
    relation_names: list[str],
) -> None:
    """Get table columns from cls."""
    annotations: dict[str, TColumn] = get_type_hints(model_cls)

    parent_cls, *_ = model_cls.__bases__

    if parent_cls is not object:
        for parent_cls in model_cls.__bases__:
            _get_table_columns(
                parent_cls,
                table_columns,
                foreign_keys,
                relation_names,
            )

    for key, value in model_cls.__dict__.items():

        if isinstance(value, Relation):
            relation_names.append(key)

        if isinstance(value, ForeignKey):
            foreign_keys.append(value)
            relation_names.append(key)
            continue

        if (
            key.startswith("__") or
            isinstance(value, MColumn) is False or
            key in table_columns  # exists in child cls
        ):
            continue

        table_columns[key] = value.column_type

    for key, value in annotations.items():

        # exists in cls attr
        if key in table_columns:
            continue

        real_cls = get_origin(value)
        if real_cls is not TColumn:
            continue

        python_type, = get_args(value)

        if python_type not in TYPE_MAPPER:
            continue

        # get db type
        db_type_cls = TYPE_MAPPER.get(python_type)

        if db_type_cls is None:
            msg = "Unknown type"
            raise TypeError(msg)

        db_type = db_type_cls()  # type: ignore[operator]

        # set cls column
        setattr(model_cls, key, MColumn(db_type))
        model_cls.__dict__[key].__set_name__(
            model_cls,
            key,
        )

        table_columns[key] = db_type


def create_table_and_data_cls_row(
    model_cls: TModelClass,
) -> tuple[TableData, type[Any]]:
    """Create table by model class and create row class."""

    table_columns: TTableColumns = {}
    foreign_keys: TForeignKeys = []
    relation_names: list[str] = []
    _get_table_columns(model_cls, table_columns, foreign_keys, relation_names)

    table_name = model_cls.__name__.lower()

    # if hasattr(model_cls, "__table_name__"):
    #     table_name = model_cls.__table_name__

    table_data = TableData(
        table_name=table_name,
        columns=table_columns,
    )

    if foreign_keys:
        table_data["foreign_keys"] = foreign_keys

    # row_cls = make_row_data_cls(
    #     table_name,
    #     list(table_columns.keys()),
    #     bases=[model_cls],
    #     create_orm_model=True,
    # )

    ready_model_cls = type(
        f"{model_cls.__name__}Model",
        (_RowORMModelMixin, model_cls),
        {},
    )

    model_cls.__table_name__ = table_name  # type: ignore
    ready_model_cls.__table_name__ = table_name  # type: ignore
    ready_model_cls.__relation_fields__ = tuple(relation_names)  # type: ignore
    ready_model_cls.__column_fields__ = tuple(  # type: ignore
        table_columns.keys()
    )

    return table_data, ready_model_cls
