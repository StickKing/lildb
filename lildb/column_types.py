"""Module contains column types for create table."""
from __future__ import annotations

import json
from abc import abstractmethod
from collections.abc import Mapping
from dataclasses import asdict
from dataclasses import fields
from dataclasses import is_dataclass
from datetime import date
from datetime import datetime
from datetime import time
from decimal import Decimal
from enum import Enum
from fractions import Fraction
from functools import singledispatchmethod
from numbers import Number
from typing import Any
from typing import Literal
from typing import Protocol
from typing import TypedDict
from typing import TypeVar
from typing import Union

from typing_extensions import NotRequired
from typing_extensions import TypeAlias
from typing_extensions import Unpack

from .enumcls import DeleteAction
from .enumcls import UpdateAction


__all__ = (
    "BaseType",
    "Integer",
    "DataClassJson",
    "Real",
    "Text",
    "Blob",
    "ForeignKey",
    "Json",
    "DateTime",
    "Date",
    "Time",
    "EnumName",
    "EnumValue",
    "EnumHide",
)


TNumberType = TypeVar("TNumberType", bound="Number")
TDateTimeDBFormat: TypeAlias = Literal["timestamp", "ISO"]
TDateDBFormat: TypeAlias = Literal["timestamp", "ISO", "ordinal"]


class TColumnType(Protocol):
    """Column type."""

    __slots__ = ()

    def __str__(self) -> str:
        """String view."""
        ...

    def to_db(self, value: Any) -> Any:
        """Serialize data to db type."""
        ...

    def to_python(self, value: Any) -> Any:
        """Serialize data to python type."""


class TBaseTypeKwargs(TypedDict):
    """Base type kwargs."""

    primary_key: NotRequired[bool]
    unique: NotRequired[bool]
    nullable: NotRequired[bool]


class ORMColumnProtocol(Protocol):
    """ORM column serializers."""

    __slots__ = ()

    def to_db(self, value: Any) -> Any:
        ...

    def to_python(self, value: Any) -> Any:
        ...


class ColumnString:
    """Columns string."""

    __slots__ = ("data",)

    def __init__(self, seq: str) -> None:
        self.data = seq

    def __repr__(self) -> str: return repr(self.data)
    def __int__(self) -> int: return int(self.data)
    def __float__(self) -> float: return float(self.data)
    def __complex__(self) -> complex: return complex(self.data)
    def __hash__(self) -> int: return hash(self.data)
    def __len__(self) -> int: return len(self.data)

    @abstractmethod
    def __str__(self) -> str:
        """String view."""
        ...


class BaseType(ColumnString):
    """Base column type."""

    __slots__ = (
        "default",
        "primary_key",
        "unique",
        "nullable",
    )

    def __init__(
        self,
        str_type: str,
        default: Any | None = None,
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = True,
    ) -> None:
        """Initialize base params for abstract type.

        Args:
            default (int | str | None): default value. Defaults to None.
            primary_key (bool): is that primary key. Defaults to False.
            unique (bool): is that unique. Defaults to False.
            nullable (bool): maybe null. Defaults to False.

        """
        super().__init__(str_type)
        self.default = default
        self.primary_key = primary_key
        self.unique = unique
        self.nullable = nullable

    def __str__(self) -> str:
        """Create string column type."""
        sql_column_type = self.data
        if self.default:
            if isinstance(self.default, str):
                sql_column_type += f" DEFAULT '{self.default}' "
            else:
                sql_column_type += f" DEFAULT {self.default} "
        if self.primary_key:
            sql_column_type += " PRIMARY KEY "
            self.nullable = False
        if not self.nullable:
            sql_column_type += " NOT NULL "
        if self.unique:
            sql_column_type += " UNIQUE "
        return sql_column_type


class Integer(BaseType):
    """Integer type."""

    __slots__ = ("autoincrement",)

    def __init__(
        self,
        default: int | None = None,
        *,
        autoincrement: bool = False,
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        """Initialize base params for INTEGER.

        Args:
            default (int | str | None): default value. Defaults to None.
            primary_key (bool): is that primary key. Defaults to False.
            unique (bool): is that unique. Defaults to False.
            nullable (bool): maybe null. Defaults to False.
            autoincrement (bool): on autoincrement. Defaults to False.

        """
        if default is not None and not isinstance(default, int):
            msg = "Incorrect type for default value."
            raise TypeError(msg)

        self.autoincrement = autoincrement
        super().__init__(
            "INTEGER",
            default=default,
            **kwargs,
        )

    def __str__(self) -> str:
        """Create string column type."""
        sql_column_type = super().__str__()
        if self.autoincrement:
            sql_column_type = sql_column_type.replace(
                "PRIMARY KEY",
                "PRIMARY KEY AUTOINCREMENT",
            )
        return sql_column_type

    @singledispatchmethod
    def to_db(self, value: Any) -> Any:
        """Serialize python obj to db data."""
        msg = "Unknown type value can be only number"
        raise TypeError(msg)

    @to_db.register
    def _(self, value: None) -> None:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: str) -> int:
        """Serialize python obj to db data."""
        return int(value)

    @to_db.register
    def _(self, value: int) -> int:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: float) -> int:
        """Serialize python obj to db data."""
        return int(value)

    @to_db.register
    def _(self, value: Decimal) -> int:
        """Serialize python obj to db data."""
        return int(value)

    @to_db.register
    def _(self, value: Fraction) -> int:
        """Serialize python obj to db data."""
        return int(value)

    @singledispatchmethod
    def to_python(self, value: Any) -> Any:
        """Serialize db data to python obj."""
        msg = "Unknown type"
        raise TypeError(msg)

    @to_python.register
    def _(self, value: int) -> int:
        """Serialize python obj to db data."""
        return value

    @to_python.register
    def _(self, value: None) -> None:
        """Serialize db data to python obj."""
        return value


class Real(BaseType):
    """REAL column type."""

    __slots__ = ()

    def __init__(
        self,
        default: float | int | None = None,
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        """Initialize base params for REAL.

        Args:
            default (int | str | None): default value. Defaults to None.
            primary_key (bool): is that primary key. Defaults to False.
            unique (bool): is that unique. Defaults to False.
            nullable (bool): maybe null. Defaults to False.
            autoincrement (bool): on autoincrement. Defaults to False.

        """
        if default is not None and not isinstance(default, Number):
            msg = "Incorrect type for default value."
            raise TypeError(msg)

        super().__init__(
            "REAL",
            default=default,
            **kwargs,
        )

    @singledispatchmethod
    def to_db(self, value: Any) -> Any:
        """Serialize python obj to db data."""
        msg = "Unknown type, value can be only number"
        raise TypeError(msg)

    @to_db.register
    def _(self, value: None) -> None:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: float) -> float:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: int) -> int:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: Decimal) -> Decimal:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: Fraction) -> Fraction:
        """Serialize python obj to db data."""
        return value

    @singledispatchmethod
    def to_python(self, value: Any) -> Any:
        """Serialize db data to python obj."""
        msg = "Unknown type, value can be only number"
        raise TypeError(msg)

    @to_python.register
    def _(self, value: None) -> None:
        """Serialize db data to python obj."""
        return value

    @to_python.register
    def _(self, value: float) -> float:
        """Serialize db data to python obj."""
        return value

    @to_python.register
    def _(self, value: int) -> int:
        """Serialize db data to python obj."""
        return value


class Text(BaseType):
    """TEXT column type."""

    __slots__ = ()

    def __init__(
        self,
        default: str | None = None,
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        """Initialize base params for TEXT.

        Args:
            default (int | str | None): default value. Defaults to None.
            primary_key (bool): is that primary key. Defaults to False.
            unique (bool): is that unique. Defaults to False.
            nullable (bool): maybe null. Defaults to False.
            autoincrement (bool): on autoincrement. Defaults to False.

        """
        if default is not None and not isinstance(default, str):
            msg = "Incorrect type for default value."
            raise TypeError(msg)

        super().__init__(
            "TEXT",
            default=default,
            **kwargs,
        )

    @singledispatchmethod
    def to_db(self, value: Any) -> Any:
        """Serialize python obj to db data."""
        msg = "Unknown type, value can be only str"
        raise TypeError(msg)

    @to_db.register
    def _(self, value: None) -> None:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: str) -> str:
        """Serialize python obj to db data."""
        return value

    @singledispatchmethod
    def to_python(self, value: Any) -> Any:
        """Serialize db data to python obj."""
        msg = "Unknown type, can be only str"
        raise TypeError(msg)

    @to_python.register
    def _(self, value: None) -> None:
        """Serialize db data to python obj."""
        return value

    @to_python.register
    def _(self, value: str) -> str:
        """Serialize db data to python obj."""
        return value


class Blob(BaseType):
    """BLOB column type."""

    __slots__ = ()

    def __init__(
        self,
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        """Initialize base params for BLOB.

        Args:
            default (int | str | None): default value. Defaults to None.
            primary_key (bool): is that primary key. Defaults to False.
            unique (bool): is that unique. Defaults to False.
            nullable (bool): maybe null. Defaults to False.
            autoincrement (bool): on autoincrement. Defaults to False.

        """
        super().__init__(
            "BLOB",
            **kwargs,
        )

    @singledispatchmethod
    def to_db(self, value: Any) -> Any:
        """Serialize python obj to db data."""
        msg = "Unknown type, value can be only bytes"
        raise TypeError(msg)

    @to_db.register
    def _(self, value: None) -> None:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: bytes) -> bytes:
        """Serialize python obj to db data."""
        return value

    @singledispatchmethod
    def to_python(self, value: Any) -> Any:
        """Serialize db data to python obj."""
        msg = "Unknown type, can be only str"
        raise TypeError(msg)

    @to_python.register
    def _(self, value: None) -> None:
        """Serialize db data to python obj."""
        return value

    @to_python.register
    def _(self, value: bytes) -> bytes:
        """Serialize db data to python obj."""
        return value


class ForeignKey(BaseType):
    """Foreign key constraint."""

    __slots__ = (
        "column",
        "second_table",
        "reference_column",
        "on_delete",
        "on_update",
    )

    def __init__(
        self,
        column: str,
        second_table: str,
        reference_column: str,
        on_delete: DeleteAction | None = None,
        on_update: UpdateAction | None = None,
    ) -> None:
        """Initialize."""
        self.column = column
        self.second_table = second_table.lower()
        self.reference_column = reference_column
        self.on_delete = DeleteAction(on_delete) if on_delete else on_delete
        self.on_update = UpdateAction(on_update) if on_update else on_update
        super().__init__("FOREIGN KEY(`{}`) REFERENCES `{}`(`{}`)")

    def __call__(self) -> str:
        """Create command."""
        stmt = self.data.format(
            self.column,
            self.second_table,
            self.reference_column,
        )
        if self.on_delete:
            stmt += f" ON DELETE {self.on_delete.value}"
        if self.on_update:
            stmt += f" ON UPDATE {self.on_update.value}"
        return stmt

    def __str__(self) -> str:
        return self()


class Json(BaseType):
    """TEXT column type with dict conversion."""

    __slots__ = (
        "_json_module",
    )

    def __init__(
        self,
        default: str | None = None,
        *,
        json_module: Any = json,
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        super().__init__(
            "TEXT",
            default,
            **kwargs,
        )
        self._json_module = json_module

    @singledispatchmethod
    def to_db(self, value: Any) -> Any:
        """Serialize python obj to db data."""
        msg = "Unknown type, value can be only str or dict"
        raise TypeError(msg)

    @to_db.register
    def _(self, value: None) -> None:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: str) -> str:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: Mapping) -> str:
        """Serialize python obj to db data."""
        return self._json_module.dumps(value)

    @singledispatchmethod
    def to_python(self, value: Any) -> Any:
        """Serialize db data to python obj."""
        msg = "Unknown type, can be only str"
        raise TypeError(msg)

    @to_python.register
    def _(self, value: None) -> None:
        """Serialize db data to python obj."""
        return value

    @to_python.register
    def _(self, value: str) -> dict:
        """Serialize db data to python obj."""
        return self._json_module.loads(value)


class DataClassJson(BaseType):
    """Text column."""

    __slots__ = (
        "_json_module",
        "_data_class",
    )

    def __init__(
        self,
        default: str | None = None,
        *,
        data_class: Any,
        json_module: Any = json,
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        super().__init__(
            "TEXT",
            default,
            **kwargs,
        )
        if is_dataclass(data_class) is False:
            msg = "data_class is not a data class"
            raise TypeError(msg)
        self._data_class = data_class
        self._json_module = json_module

    @singledispatchmethod
    def to_db(self, value: Any) -> str:
        """Serialize python obj to db data."""
        if is_dataclass(value):
            return self._json_module.dumps(
                asdict(value),  # type: ignore[arg-type]
            )
        msg = "Unknown type"
        raise TypeError(msg)

    @to_db.register  # type: ignore[arg-type]
    def _(self, value: None) -> None:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: str) -> str:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: Mapping) -> str:
        """Serialize python obj to db data."""
        return self._json_module.dumps(value)

    @singledispatchmethod
    def to_python(self, value: Any) -> Any:
        """Serialize db data to python obj."""
        msg = "Unknown type"
        raise TypeError(msg)

    @to_python.register
    def _(self, value: None) -> None:
        """Serialize db data to python obj."""
        return value

    @to_python.register
    def _(self, value: str) -> Any:
        """Serialize db data to python obj."""
        data = self._json_module.loads(value)
        if not data:
            data = {
                field.name: None
                for field in fields(self._data_class)
            }
        return self._data_class(**data)


class DateTime(BaseType):
    """TEXT or REAL column type with dict conversion."""

    __slots__ = ("_datetime_db_format",)

    def __init__(
        self,
        default: str | None = None,
        *,
        datetime_db_format: TDateTimeDBFormat = "ISO",
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        db_type = "TEXT"

        if datetime_db_format not in {"ISO", "timestamp"}:
            msg = "Unknown format for datetime_db_format"
            raise ValueError(msg)

        self._datetime_db_format = datetime_db_format
        if datetime_db_format == "timestamp":
            db_type = "REAL"

        super().__init__(
            db_type,
            default,
            **kwargs,
        )

    @singledispatchmethod
    def to_db(self, value: Any) -> Any:
        """Serialize python obj to db data."""
        msg = "Unknown type"
        raise TypeError(msg)

    @to_db.register
    def _(self, value: None) -> None:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: str) -> str:
        """Serialize python obj to db data."""
        if self._datetime_db_format != "ISO":
            msg = "Unknown type"
            raise TypeError(msg)
        return value

    @to_db.register
    def _(self, value: float) -> float:
        """Serialize python obj to db data."""
        if self._datetime_db_format != "timestamp":
            msg = "Unknown type"
            raise TypeError(msg)
        return value

    @to_db.register
    def _(self, value: datetime) -> Union[float, str]:
        """Serialize python obj to db data."""
        if self._datetime_db_format == "timestamp":
            return value.timestamp()
        return value.isoformat()

    @to_db.register
    def _(self, value: date) -> Union[float, str]:
        """Serialize python obj to db data."""
        datetime_value = datetime.combine(value, datetime.min.time())
        if self._datetime_db_format == "timestamp":
            return datetime_value.timestamp()
        return datetime_value.isoformat()

    @singledispatchmethod
    def to_python(self, value: Any) -> Any:
        """Serialize db data to python obj."""
        msg = "Unknown type, can be only str"
        raise TypeError(msg)

    @to_python.register
    def _(self, value: None) -> None:
        """Serialize db data to python obj."""
        return value

    @to_python.register
    def _(self, value: str) -> datetime:
        """Serialize db data to python obj."""
        if self._datetime_db_format != "ISO":
            msg = "Unknown type"
            raise TypeError(msg)

        return datetime.fromisoformat(value)

    @to_python.register
    def _(self, value: float) -> datetime:
        """Serialize db data to python obj."""
        if self._datetime_db_format != "timestamp":
            msg = "Unknown type"
            raise TypeError(msg)

        return datetime.fromtimestamp(value)


class Date(BaseType):
    """TEXT or REAL or INTEGER column type with dict conversion."""

    __slots__ = ("_date_db_format",)

    def __init__(
        self,
        default: str | None = None,
        *,
        date_db_format: TDateDBFormat = "ISO",
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        db_type = "TEXT"
        self._date_db_format = date_db_format

        if date_db_format not in {"timestamp", "ISO", "ordinal"}:
            msg = "Unknown name for date_db_format"
            raise ValueError(msg)

        if date_db_format == "timestamp":
            db_type = "REAL"

        if date_db_format == "ordinal":
            db_type = "INTEGER"

        super().__init__(
            db_type,
            default,
            **kwargs,
        )

    @singledispatchmethod
    def to_db(self, value: Any) -> Any:
        """Serialize python obj to db data."""
        msg = "Unknown type"
        raise TypeError(msg)

    @to_db.register
    def _(self, value: None) -> None:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: str) -> str:
        """Serialize python obj to db data."""
        if self._date_db_format != "ISO":
            msg = "Unknown type"
            raise TypeError(msg)
        return value

    @to_db.register
    def _(self, value: float) -> float:
        """Serialize python obj to db data."""
        if self._date_db_format != "timestamp":
            msg = "Unknown type"
            raise TypeError(msg)
        return value

    @to_db.register
    def _(self, value: datetime) -> Union[float, str, int]:
        """Serialize python obj to db data."""
        if self._date_db_format == "timestamp":
            return value.timestamp()
        if self._date_db_format == "ordinal":
            return value.toordinal()
        return value.date().isoformat()

    @to_db.register
    def _(self, value: date) -> Union[float, str, int]:
        """Serialize python obj to db data."""
        if self._date_db_format == "timestamp":
            datetime_value = datetime.combine(value, datetime.min.time())
            return datetime_value.timestamp()
        if self._date_db_format == "ordinal":
            return value.toordinal()
        return value.isoformat()

    @singledispatchmethod
    def to_python(self, value: Any) -> Any:
        """Serialize db data to python obj."""
        msg = "Unknown type"
        raise TypeError(msg)

    @to_python.register
    def _(self, value: None) -> None:
        """Serialize db data to python obj."""
        return value

    @to_python.register
    def _(self, value: float) -> date:
        """Serialize db data to python obj."""
        if self._date_db_format != "timestamp":
            msg = "Unknown type"
            raise TypeError(msg)
        return datetime.fromtimestamp(value).date()

    @to_python.register
    def _(self, value: str) -> date:
        """Serialize db data to python obj."""
        if self._date_db_format != "ISO":
            msg = "Unknown type"
            raise TypeError(msg)

        return datetime.fromisoformat(value).date()

    @to_python.register
    def _(self, value: int) -> date:
        """Serialize db data to python obj."""
        if self._date_db_format != "ordinal":
            msg = "Unknown type"
            raise TypeError(msg)

        return datetime.fromordinal(value).date()


class Time(BaseType):
    """TEXT column type with dict conversion."""

    __slots__ = ()

    def __init__(
        self,
        default: str | None = None,
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        super().__init__(
            "TEXT",
            default,
            **kwargs,
        )

    @singledispatchmethod
    def to_db(self, value: Any) -> Any:
        """Serialize python obj to db data."""
        msg = "Unknown type, value can be only str or dict"
        raise TypeError(msg)

    @to_db.register
    def _(self, value: None) -> None:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: str) -> str:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: time) -> str:
        """Serialize python obj to db data."""
        return value.isoformat()

    @singledispatchmethod
    def to_python(self, value: Any) -> Any:
        """Serialize db data to python obj."""
        msg = "Unknown type, can be only str"
        raise TypeError(msg)

    @to_python.register
    def _(self, value: None) -> None:
        """Serialize db data to python obj."""
        return value

    @to_python.register
    def _(self, value: str) -> time:
        """Serialize db data to python obj."""
        return time.fromisoformat(value)


class EnumName(BaseType):
    """"TEXT type that stores the name from an Enum object."""

    __slots__ = (
        "_enum_cls",
        "_enum_names",
    )

    def __init__(
        self,
        enum_cls: type[Enum],
        *,
        default: str | None = None,
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        super().__init__(
            "TEXT",
            default,
            **kwargs,
        )
        self._enum_cls = enum_cls
        self._enum_names = {item.name for item in enum_cls}

    @singledispatchmethod
    def to_db(self, value: Any) -> Any:
        """Serialize python obj to db data."""
        msg = "Unknown type, value can be only str or dict"
        raise TypeError(msg)

    @to_db.register
    def _(self, value: None) -> None:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: str) -> str:
        """Serialize python obj to db data."""
        if value in self._enum_names:
            return value
        msg = f"Enum do not have a '{value}' name"
        raise ValueError(msg)

    @to_db.register
    def _(self, value: Enum) -> str:
        """Serialize python obj to db data."""
        return value.name

    @singledispatchmethod
    def to_python(self, value: Any) -> Any:
        """Serialize db data to python obj."""
        msg = "Unknown type, can be only str"
        raise TypeError(msg)

    @to_python.register
    def _(self, value: None) -> None:
        """Serialize db data to python obj."""
        return value

    @to_python.register
    def _(self, value: str) -> Any:
        """Serialize db data to python obj."""
        return getattr(self._enum_cls, value)


class EnumValue(BaseType):
    """"TEXT type that stores the value from an Enum object."""

    __slots__ = (
        "_enum_cls",
        "_enum_values",
    )

    def __init__(
        self,
        enum_cls: type[Enum],
        *,
        default: Any = None,
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        super().__init__(
            "TEXT",
            default,
            **kwargs,
        )
        self._enum_cls = enum_cls
        self._enum_values = {item.value for item in enum_cls}

    @singledispatchmethod
    def to_db(self, value: Any) -> Any:
        """Serialize python obj to db data."""
        if value in self._enum_values:
            return value
        msg = f"Enum do not have a '{value}' value"
        raise ValueError(msg)

    @to_db.register
    def _(self, value: None) -> None:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: Enum) -> Any:
        """Serialize python obj to db data."""
        return value.value

    @singledispatchmethod
    def to_python(self, value: Any) -> Any:
        """Serialize db data to python obj."""
        return self._enum_cls(value)

    @to_python.register
    def _(self, value: None) -> None:
        """Serialize db data to python obj."""
        return value


class EnumHide(BaseType):
    """"
    TEXT type that stores the name from an Enum object.
    And return only value without enum obj.
    """

    __slots__ = (
        "_enum_cls",
        "_enum_names",
    )

    def __init__(
        self,
        enum_cls: type[Enum],
        *,
        default: str | None = None,
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        super().__init__(
            "TEXT",
            default,
            **kwargs,
        )
        self._enum_cls = enum_cls
        self._enum_names = {item.name for item in enum_cls}

    @singledispatchmethod
    def to_db(self, value: Any) -> Any:
        """Serialize python obj to db data."""
        msg = "Unknown type"
        raise TypeError(msg)

    @to_db.register
    def _(self, value: None) -> None:
        """Serialize python obj to db data."""
        return value

    @to_db.register
    def _(self, value: str) -> str:
        """Serialize python obj to db data."""
        if value in self._enum_names:
            return value
        msg = f"Enum do not have a '{value}' name"
        raise ValueError(msg)

    @to_db.register
    def _(self, value: Enum) -> str:
        """Serialize python obj to db data."""
        return value.name

    @singledispatchmethod
    def to_python(self, value: Any) -> Any:
        """Serialize db data to python obj."""
        msg = "Unknown type"
        raise TypeError(msg)

    @to_python.register
    def _(self, value: None) -> None:
        """Serialize db data to python obj."""
        return value

    @to_python.register
    def _(self, value: str) -> Any:
        """Serialize db data to python obj."""
        return getattr(self._enum_cls, value).value
