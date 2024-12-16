"""Module contain SQL functions and other operation."""
from __future__ import annotations

from collections import UserString
from typing import Literal
from typing import TypeVar


TFunc = Literal[
    "avg",
    "sum",
    "min",
    "max",
    "count",
    "random",
    "lower",
    "upper",
    "length",
]
TFuncCLS = TypeVar("TFuncCLS", bound="SQLBase")


__all__ = (
    "func",
)


FUNC_NAMES = {
    "avg",
    "sum",
    "min",
    "max",
    "count",
    "random",
    "lower",
    "upper",
    "length",
}


class ChangedUserStr(UserString):
    """User str."""

    def __init__(self, seq: object) -> None:
        """Initialize"""
        if isinstance(seq, str):
            self._data = seq
        elif isinstance(seq, UserString):
            self._data = str(seq)
        else:
            self._data = str(seq)
        self._label: str | None = None

    @property
    def data(self) -> str:
        """Return completed."""
        return str(self)

    @data.setter
    def data(self, value: str) -> None:
        """Return completed."""
        self._data = value


class SQLBase(ChangedUserStr):
    """Base sql function or operation."""

    def label(self, name: str) -> SQLBase:
        """Create AS label."""
        self._label = name
        return self

    @property
    def complete_label(self) -> str | None:
        """Prepare label for command"""
        if self._label:
            return self._label

        label = self.__class__.__name__.lower()
        if bool(self._data.replace("*", "")) is False or not self._data:
            return f"{label}"

        for operator in {"*", "-", "+"}:
            if operator in self._data:
                return f"{label}"

        if "DISTINCT" in self._data:
            return self._data.replace("DISTINCT ", "")

        return self._data

    def __str__(self) -> str:
        """Create string view"""
        operation = self.__class__.__name__.upper()
        label = self.complete_label
        return f"{operation}({self._data}) AS {label}"


class Distinct(ChangedUserStr):
    """Distinct operator."""

    def __str__(self) -> str:
        """Create string view"""
        operation = self.__class__.__name__.upper()
        return f"{operation} {self._data}"


class Substr(SQLBase):
    """Substr operation."""

    def __init__(self, seq: object, start: int, end: int | None = 0) -> None:
        super().__init__(seq)
        self._start = start
        self._end = end

    def __str__(self) -> str:
        """Create string view"""
        operation = self.__class__.__name__.upper()
        label = self.complete_label
        if not self._end:
            return "{}({}, {}) AS {}".format(
                operation,
                self._data,
                self._start,
                label,
            )
        return "{}({}, {}, {}) AS {}".format(
            operation,
            self._data,
            self._start,
            self._end,
            label,
        )


class Func:
    """Object with all func."""

    def distinct(self, name: str) -> Distinct:
        """Use distinct."""
        return Distinct(name)

    def __getattr__(self, name: TFunc) -> type[TFuncCLS]:
        """Create func for column."""
        if name not in FUNC_NAMES:
            msg = "{} {} {}".format(
                self.__class__.__name__,
                "object has no attribute",
                name,
            )
            raise AttributeError(msg)
        func_cls = type(
            name.upper(),
            (SQLBase,),
            {},
        )
        if name.lower() == "random":
            def __init__(self):
                self._data = ""
                self._label = None
            func_cls.__init__ = __init__
        return func_cls


func = Func()
