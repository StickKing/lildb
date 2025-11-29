"""Module contains column types for create table."""
from __future__ import annotations

import json
from abc import abstractmethod
from datetime import date
from datetime import datetime
from datetime import time
from enum import Enum
from functools import singledispatchmethod
from numbers import Number
from typing import Any
from typing import Callable
from typing import Hashable
from typing import Literal
from typing import Protocol
from typing import TypedDict
from typing import TypeVar

from typing_extensions import NotRequired
from typing_extensions import TypeAlias
from typing_extensions import Unpack

from .enumcls import DeleteAction
from .enumcls import UpdateAction


__all__ = (
    "BaseType",
    "Integer",
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


TColumnType = TypeVar("TColumnType", bound="BaseType")
TNumberType = TypeVar("TNumberType", bound="Number")
TDateTimeDBFormat: TypeAlias = Literal["timestamp", "ISO"]
TDateDBFormat: TypeAlias = Literal["timestamp", "ISO", "ordinal"]


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


class BaseORMColumnVixin:
    """Base serializers methods."""

    __slots__ = ()

    def to_db(self, value: Any) -> Any:
        """Serialize python obj to db data."""
        if value is None:
            return None
        return value

    @singledispatchmethod
    def to_python(self, value) -> Any:
        """Serialize db data to python obj."""



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


class BaseType(ColumnString, ORMColumnProtocol):
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
        if self.default:
            if isinstance(self.default, str):
                self.data += f" DEFAULT '{self.default}' "
            else:
                self.data += f" DEFAULT {self.default} "
        if self.primary_key:
            self.data += " PRIMARY KEY "
        if not self.nullable:
            self.data += " NOT NULL "
        if self.unique:
            self.data += " UNIQUE "
        return str(self.data)


class Integer(BaseType, BaseORMColumnVixin):
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
        self.data = super().__str__()
        if self.autoincrement:
            self.data = self.data.replace(
                "PRIMARY KEY",
                "PRIMARY KEY AUTOINCREMENT",
            )
        return self.data


class Real(BaseType, BaseORMColumnVixin):
    """REAL column type."""

    def __init__(
        self,
        default: TNumberType | None = None,
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


class Text(BaseType, BaseORMColumnVixin):
    """TEXT column type."""

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


class Blob(BaseType, BaseORMColumnVixin):
    """BLOB column type."""

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
        self.second_table = second_table
        self.reference_column = reference_column
        self.on_delete = DeleteAction(on_delete) if on_delete else on_delete
        self.on_update = DeleteAction(on_update) if on_update else on_update
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


class Json(Text, ORMColumnProtocol):
    """TEXT column type with dict conversion."""

    __slots__ = (
        "_json_module",
        "_factory_cls",
    )

    def __init__(
        self,
        default: str | None = None,
        *,
        json_module: Any = json,
        factory_cls: type[Any] | None = None,
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        super().__init__(
            default,
            **kwargs,
        )
        self._json_module = json_module
        self._factory_cls = factory_cls

    def to_db(self, value: dict | str) -> str:
        """Serialize python obj to db data."""
        if isinstance(value, str):
            return value
        return self._json_module.dumps(value)

    def to_python(self, value: str | None) -> dict | None:
        """Serialize db data to python obj."""
        if value is None:
            return None

        data = self._json_module.loads(value)
        if self._factory_cls is None:
            return data
        return self._factory_cls(data)


class DateTime(Text, ORMColumnProtocol):
    """TEXT or REAL column type with dict conversion."""

    __slots__ = ("_datetime_db_format",)

    def __init__(
        self,
        default: str | None = None,
        *,
        datetime_db_format: TDateTimeDBFormat = "ISO",
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        super().__init__(
            default,
            **kwargs,
        )
        self._datetime_db_format = datetime_db_format
        if datetime_db_format == "timestamp":
            self.data = "REAL"

    def to_db(
        self,
        value: datetime | str | float | None,
    ) -> str | float | None:
        """Serialize python obj to db data."""
        if value is None:
            return None

        # for __init__ data
        if isinstance(value, (float, str)):
            return value

        serialize_func: Callable[[], str | float | Any] | None = {
            "timestamp": value.timestamp,
            "ISO": value.isoformat,
        }.get(self._datetime_db_format, None)

        if serialize_func is None:
            return None

        return serialize_func()

    def to_python(
        self,
        value: str | float | None,
    ) -> datetime | None:
        """Serialize db data to python obj."""
        if value is None:
            return None

        serialize_func: Callable[[str | float], datetime] | None = {
            "timestamp": datetime.fromtimestamp,
            "ISO": datetime.fromisoformat,
        }.get(self._datetime_db_format)

        if serialize_func is None:
            return None

        return serialize_func(value)


class Date(Text):
    """TEXT or REAL or INTEGER column type with dict conversion."""

    __slots__ = ("_date_db_format",)

    def __init__(
        self,
        default: str | None = None,
        *,
        date_db_format: TDateDBFormat = "ISO",
        **kwargs: Unpack[TBaseTypeKwargs],
    ) -> None:
        super().__init__(
            default,
            **kwargs,
        )
        self._date_db_format = date_db_format

        if date_db_format not in {"timestamp", "ISO", "ordinal"}:
            msg = "Unknown name for date_db_format"
            raise ValueError(msg)

        if date_db_format == "timestamp":
            self.data = "REAL"

        if date_db_format == "ordinal":
            self.data = "INTEGER"

    def to_db(
        self,
        value: datetime | date | None,
    ) -> str | float | int | None:
        """Serialize python obj to db data."""
        if value is None:
            return None

        # for __init__ data
        if isinstance(value, (float, str, int)):
            return value

        if isinstance(value, datetime):
            serialize_func = {
                "timestamp": value.timestamp,
                "ISO": value.date().isoformat,
                "ordinal": value.toordinal,
            }.get(self._date_db_format, None)
        else:
            value_datetime = datetime.combine(value, datetime.min.time())
            serialize_func = {
                "timestamp": value_datetime.timestamp,
                "ISO": value.isoformat,
                "ordinal": value.toordinal,
            }.get(self._date_db_format, None)

        if serialize_func is None:
            return None

        return serialize_func()

    @singledispatchmethod
    def to_python(self, value) -> Any:
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

    # def to_python(
    #     self,
    #     value: str | float | None,
    # ) -> date | None:
    #     """Serialize db data to python obj."""
    #     if value is None:
    #         return value

    #     serialize_func = {
    #         "timestamp": datetime.fromtimestamp,
    #         "ISO": datetime.fromisoformat,
    #         "ordinal": datetime.fromordinal,
    #     }.get(self._date_db_format)

    #     if serialize_func is None:
    #         return None

    #     return serialize_func(value).date()


class Time(Text, ORMColumnProtocol):
    """TEXT column type with dict conversion."""

    __slots__ = ()

    def to_db(
        self,
        value: time | str | None,
    ) -> str | None:
        """Serialize python obj to db data."""
        if value is None:
            return None

        if isinstance(value, str):
            return value

        return value.isoformat()

    def to_python(
        self,
        value: str | None,
    ) -> time | None:
        """Serialize db data to python obj."""
        if value is None:
            return None

        return time.fromisoformat(value)


class EnumName(Text):
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
            default,
            **kwargs,
        )
        self._enum_cls = enum_cls
        self._enum_names = {item.name for item in enum_cls}

    def to_db(self, value: Any | str | None) -> str | Hashable | None:
        """Serialize to db type."""
        if value is None:
            return None

        if isinstance(value, Hashable):
            if value in self._enum_names:
                return value

            msg = "Unknown type"
            raise TypeError(msg)

        if isinstance(value, Enum):
            return value.name

        return self._enum_cls(value).name

    def to_python(self, value: str | None) -> Any:
        """Serialize to python obj."""
        if value is None:
            return None
        return getattr(self._enum_cls, value)


class EnumValue(Text):
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
            default,
            **kwargs,
        )
        self._enum_cls = enum_cls
        self._enum_values = {item.value for item in enum_cls}

    def to_db(self, value: type[Enum] | Hashable | None) -> Any:
        """Serialize to python obj."""
        if value is None:
            return None

        # __init__ dataclass
        if isinstance(value, Hashable):
            if value in self._enum_values:
                return value

            msg = "Unknown type"
            raise TypeError(msg)

        return value.value

    def to_python(self, value: Any) -> Enum | None:
        """Serialize to db type."""
        if value is None:
            return None

        return self._enum_cls(value)


class EnumHide(Text):
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
            default,
            **kwargs,
        )
        self._enum_cls = enum_cls
        self._enum_names = {item.name for item in enum_cls}

    def to_db(
        self,
        value: type[Enum] | Hashable | None,
    ) -> str | Hashable | None:
        """Serialize to db type."""
        if value is None:
            return None

        # __init__ dataclass
        if isinstance(value, Hashable) and value in self._enum_names:
            return value

        if isinstance(value, Enum):
            return value.name

        return self._enum_cls(value).name

    def to_python(self, value: str | None) -> Any:
        """Serialize to python obj."""
        if value is None:
            return None
        return getattr(self._enum_cls, value).value
