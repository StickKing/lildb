"""Module contains column types for create table."""
from __future__ import annotations

import json
from collections import Hashable
from collections import UserString
from datetime import date
from datetime import datetime
from datetime import time
from enum import Enum
from numbers import Number
from typing import Any
from typing import Literal
from typing import Protocol
from typing import TypeVar

from .enumcls import DeleteAction
from .enumcls import UpdateAction


__all__ = (
    "BaseType",
    "Integer",
    "Real",
    "Text",
    "Blob",
    "ForeignKey",
)


TColumnType = TypeVar("TColumnType", bound="BaseType")
TNumberType = TypeVar("TNumberType", bound="Number")
TDateTimeDBFormat = Literal["timestamp", "ISO"]
TDateDBFormat = Literal["timestamp", "ISO", "ordinal"]


class ORMColumnProtocol(Protocol):
    """ORM column serializers."""

    def to_db(self, value: Any | None) -> Any:
        ...

    def to_python(self, value: Any | None) -> Any:
        ...


class BaseORMColumnVixin(ORMColumnProtocol):
    """Base serializers methods."""

    def to_db(self, value: Any) -> Any:
        """Serialize python obj to db data."""
        if value is None:
            return None
        return value

    def to_python(self, value: Any) -> Any:
        """Serialize db data to python obj."""
        if value is None:
            return None
        return value


class BaseType(UserString, ORMColumnProtocol):
    """Base column type."""

    def __init__(
        self,
        str_type: str,
        default: TNumberType | str | bytes | None = None,
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

    def __init__(
        self,
        default: int | None = None,
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = True,
        autoincrement: bool = False,
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
            primary_key=primary_key,
            unique=unique,
            nullable=nullable,
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
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = True,
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
            primary_key=primary_key,
            unique=unique,
            nullable=nullable,
        )


class Text(BaseType, BaseORMColumnVixin):
    """TEXT column type."""

    def __init__(
        self,
        default: str | None = None,
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = True,
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
            primary_key=primary_key,
            unique=unique,
            nullable=nullable,
        )


class Blob(BaseType, BaseORMColumnVixin):
    """BLOB column type."""

    def __init__(
        self,
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = True,
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
            primary_key=primary_key,
            unique=unique,
            nullable=nullable,
        )


class ForeignKey(UserString):
    """Foreign key constraint."""

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


class Json(Text, ORMColumnProtocol):
    """TEXT column type with dict conversion."""

    def __init__(
        self,
        default: str | None = None,
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = True,
        json_module: Any = json,
        factory_cls: type[Any] | None = None,
    ) -> None:
        super().__init__(
            default,
            primary_key=primary_key,
            unique=unique,
            nullable=nullable,
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

    def __init__(
        self,
        default: str | None = None,
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = True,
        datetime_db_format: TDateTimeDBFormat = "ISO",
    ) -> None:
        super().__init__(
            default,
            primary_key=primary_key,
            unique=unique,
            nullable=nullable,
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

        serialize_func = {
            "timestamp": value.timestamp,
            "ISO": value.isoformat,
        }.get(self._datetime_db_format, None)

        if serialize_func:
            return serialize_func()
        return None

    def to_python(
        self,
        value: str | float | None,
    ) -> datetime:
        """Serialize db data to python obj."""
        if value is None:
            return None

        serialize_func = {
            "timestamp": datetime.fromtimestamp,
            "ISO": datetime.fromisoformat,
        }.get(self._datetime_db_format)

        return serialize_func(value)


class Date(Text, ORMColumnProtocol):
    """TEXT or REAL or INTEGER column type with dict conversion."""

    def __init__(
        self,
        default: str | None = None,
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = True,
        date_db_format: TDateDBFormat = "ISO",
    ) -> None:
        super().__init__(
            default,
            primary_key=primary_key,
            unique=unique,
            nullable=nullable,
        )
        self._datetime_db_format = date_db_format

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
            }.get(self._datetime_db_format, None)
        else:
            value_datetime = datetime.combine(value, datetime.min.time())
            serialize_func = {
                "timestamp": value_datetime.timestamp,
                "ISO": value.isoformat,
                "ordinal": value.toordinal,
            }.get(self._datetime_db_format, None)

        if serialize_func:
            return serialize_func()
        return None

    def to_python(
        self,
        value: str | float | None,
    ) -> date:
        """Serialize db data to python obj."""
        if not value:
            return value

        serialize_func = {
            "timestamp": datetime.fromtimestamp,
            "ISO": datetime.fromisoformat,
            "ordinal": datetime.fromordinal,
        }.get(self._datetime_db_format)

        return serialize_func(value).date()


class Time(Text, ORMColumnProtocol):
    """TEXT column type with dict conversion."""

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
    ) -> time:
        """Serialize db data to python obj."""
        if value is None:
            return None

        return time.fromisoformat(value)


class EnumName(Text):
    """"TEXT type that stores the name from an Enum object."""

    def __init__(
        self,
        enum_cls: type[Enum],
        *,
        default=None,
        primary_key=False,
        unique=False,
        nullable=True,
    ) -> None:
        super().__init__(
            default,
            primary_key=primary_key,
            unique=unique,
            nullable=nullable,
        )
        self._enum_cls = enum_cls
        self._enum_names = {item.name for item in enum_cls}

    def to_db(self, value: Any | str | None) -> str:
        """Serialize to db type."""
        if value is None:
            return None

        if isinstance(value, Hashable) and value in self._enum_names:
            return value

        if isinstance(value, str):
            return self._enum_cls(value).name

        return value.name

    def to_python(self, value: str | None) -> Any:
        """Serialize to python obj."""
        if value is None:
            return None
        return getattr(self._enum_cls, value)


class EnumValue(Text):
    """"TEXT type that stores the value from an Enum object."""

    def __init__(
        self,
        enum_cls: type[Enum],
        *,
        default=None,
        primary_key=False,
        unique=False,
        nullable=True,
    ) -> None:
        super().__init__(
            default,
            primary_key=primary_key,
            unique=unique,
            nullable=nullable,
        )
        self._enum_cls = enum_cls
        self._enum_values = {item.value for item in enum_cls}

    def to_db(self, value: str | None) -> Any:
        """Serialize to python obj."""
        if value is None:
            return None

        if value in self._enum_values:
            return value

        return value.value

    def to_python(self, value: Any) -> str:
        """Serialize to db type."""
        if value is None:
            return None

        return self._enum_cls(value)


class EnumHide(Text):
    """"
    TEXT type that stores the name from an Enum object.
    And return only value without enum obj.
    """

    def __init__(
        self,
        enum_cls: type[Enum],
        *,
        default=None,
        primary_key=False,
        unique=False,
        nullable=True,
    ) -> None:
        super().__init__(
            default,
            primary_key=primary_key,
            unique=unique,
            nullable=nullable,
        )
        self._enum_cls = enum_cls
        self._enum_names = {item.name for item in enum_cls}

    def to_db(self, value: Any | str | None) -> str:
        """Serialize to db type."""
        if value is None:
            return None

        if isinstance(value, Hashable) and value in self._enum_names:
            return value

        if isinstance(value, str):
            return self._enum_cls(value).name

        return value.name

    def to_python(self, value: str | None) -> Any:
        """Serialize to python obj."""
        if value is None:
            return None
        return getattr(self._enum_cls, value).value
