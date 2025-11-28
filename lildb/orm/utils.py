"""Module contains functions that generate an ORM model of the database."""
from __future__ import annotations

import datetime
from typing import TYPE_CHECKING
from typing import Any
from typing import Dict
from typing import List
from typing import TypedDict
from typing import TypeVar
from typing import Union

from typing_extensions import NotRequired
from typing_extensions import TypeAlias
from typing_extensions import get_args
from typing_extensions import get_origin
from typing_extensions import get_type_hints

from lildb.orm.orm import MColumn
from lildb.orm.orm import TypedColumn

from ..column_types import BaseType
from ..column_types import Blob
from ..column_types import Date
from ..column_types import DateTime
from ..column_types import ForeignKey
from ..column_types import Integer
from ..column_types import Real
from ..column_types import Text
from ..column_types import Time
from ..rows import make_row_data_cls


if TYPE_CHECKING:
    TModelClass = TypeVar("TModelClass", bound=type[Any])

TForeignKeys: TypeAlias = List[ForeignKey]
TTableColumns: TypeAlias = Dict[str, BaseType]


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


def _get_table_columns(
    model_cls: TModelClass,
    table_columns: TTableColumns,
    foreign_keys: TForeignKeys,
) -> None:
    """Get table columns from cls."""
    annotations: dict[str, TypedColumn] = get_type_hints(model_cls)

    for key, value in model_cls.__dict__.items():

        if isinstance(value, ForeignKey):
            foreign_keys.append(value)
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
        if real_cls is not TypedColumn:
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

    parent_cls, *_ = model_cls.__bases__

    if parent_cls is object:
        return

    for parent_cls in model_cls.__bases__:
        _get_table_columns(parent_cls, table_columns, foreign_keys)


def create_table_and_data_cls_row(
    model_cls: TModelClass,
) -> tuple[TableData, type[Any]]:
    """Create table by model class and create row class."""

    table_columns: TTableColumns = {}
    foreign_keys: TForeignKeys = []
    _get_table_columns(model_cls, table_columns, foreign_keys)

    table_name = model_cls.__name__

    table_data = TableData(
        table_name=table_name,
        columns=table_columns,
    )

    if foreign_keys:
        table_data["foreign_keys"] = foreign_keys

    row_cls = make_row_data_cls(
        table_name,
        list(table_columns.keys()),
        bases=[model_cls],
        default_none=False,
    )

    row_cls.orm_obj = True

    return table_data, row_cls
