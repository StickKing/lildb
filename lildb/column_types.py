"""Module contains column types for create table."""
from __future__ import annotations

from collections import UserString
from numbers import Number
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


class BaseType(UserString):
    """Base column type."""

    def __init__(
        self,
        str_type: str,
        default: TNumberType | str | bytes | None = None,
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = False,
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


class Integer(BaseType):
    """Integer type."""

    def __init__(
        self,
        default: int | None = None,
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = False,
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


class Real(BaseType):
    """REAL column type."""

    def __init__(
        self,
        default: TNumberType | None = None,
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = False,
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


class Text(BaseType):
    """TEXT column type."""

    def __init__(
        self,
        default: str | None = None,
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = False,
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


class Blob(BaseType):
    """BLOB column type."""

    def __init__(
        self,
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = False,
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
        table: str,
        reference_column: str,
        on_delete: DeleteAction | None = None,
        on_update: UpdateAction | None = None,
    ) -> None:
        """Initialize."""
        self.column = column
        self.table = table
        self.reference_column = reference_column
        self.on_delete = DeleteAction(on_delete) if on_delete else on_delete
        self.on_update = DeleteAction(on_update) if on_update else on_update
        super().__init__("FOREIGN KEY(`{}`) REFERENCES `{}`(`{}`)")

    def __call__(self) -> str:
        """Create command."""
        stmt = self.data.format(
            self.column,
            self.table,
            self.reference_column,
        )
        if self.on_delete:
            stmt += f" ON DELETE {self.on_delete.value}"
        if self.on_update:
            stmt += f" ON UPDATE {self.on_update.value}"
        return stmt
