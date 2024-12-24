"""Module contain SQL functions and other operation."""
from __future__ import annotations

from typing import Any
from typing import Literal
from typing import Sequence
from typing import TypeVar


TFunc = Literal[
    "abs",
    "avg",
    "sum",
    "min",
    "max",
    "count",
    "random",
    "lower",
    "upper",
    "length",
    "distinct",
    "substr",
    "like",
    "ltrim",
    "rtrim",
    "trim",
    "replace",
    "instr",
    "date",
    "time",
    "datetime",
    "julianday",
    "strftime",
]
TFuncCLS = TypeVar("TFuncCLS", bound="SQLBase")


__all__ = (
    "func",
)


FUNC_NAMES = {
    "abs",
    "avg",
    "sum",
    "min",
    "max",
    "count",
    "random",
    "lower",
    "upper",
    "length",
    "distinct",
    "substr",
    "like",
    "ltrim",
    "rtrim",
    "trim",
    "replace",
    "instr",
    "date",
    "time",
    "datetime",
    "julianday",
    "strftime",
}


class SQLBase:
    """Base sql function or operation."""

    __slots__ = ("_data", "_label")

    template = "{func}({data}) AS {label}"

    def __init__(self, *args: Any) -> None:
        """Initialize"""
        self._data = args
        self._label: str | None = None

    def label(self, name: str) -> SQLBase:
        """Create AS label."""
        self._label = name
        return self

    @property
    def complete_label(self) -> str | None:
        """Prepare label for command"""
        if self._label:
            return self._label

        label = "_".join(
            column._column_name
            for column in self._data
            if hasattr(column, "_column_name")
        )

        if label:
            return label

        label = self.__class__.__name__.lower()
        return label

    @property
    def data(self) -> str | list[str]:
        """Return completed."""
        if "]}" in self.template:
            return [
                f"'{arg}'" if isinstance(arg, str) else str(arg)
                for arg in self._data
            ]
        return ", ".join(
            f"'{arg}'" if isinstance(arg, str) else str(arg)
            for arg in self._data
        )

    @data.setter
    def data(self, value: Sequence) -> None:
        """Return completed."""
        self._data = value

    def __str__(self) -> str:
        """Create string view"""
        operation = self.__class__.__name__.upper()
        label = self.complete_label
        return self.template.format(**{
            "func": operation,
            "data": self.data,
            "label": label,
        })

    def __eq__(self, value: Any) -> bool:
        """Eq operation."""
        return str(self) == value

    @property
    def row_name(self) -> str:
        """Row name."""
        return self.complete_label


class Func:
    """Object to generation all sql funcs."""

    __slots__ = ()

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

        name_lower = name.lower()

        if name_lower in "distinct":
            func_cls.template = "{func} {data}"

        if name_lower == "like":
            func_cls.template = "{data[0]} {func} {data[1]}"

        if name_lower == "random":
            def __init__(self):
                self._data = ""
                self._label = None
            func_cls.__init__ = __init__
        return func_cls


func = Func()
