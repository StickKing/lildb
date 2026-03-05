"""Module contain SQL functions and other operation."""
from __future__ import annotations

from typing import Any
from typing import Literal
from typing import Sequence
from typing import TypeVar


__all__ = (
    "func",
)


TFuncObj = TypeVar("TFuncObj", bound="SQLBase")


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

    __slots__ = ("_data", "_label", "_template", "_disable_arguments", "_name")

    # template = "{func}({data}) AS {label}"

    def __init__(
        self,
        name: str,
        template: str | None = None,
        data: Any = None,
        *,
        disable_arguments: bool = False,
    ) -> None:
        """Initialize"""
        self._name = name
        self._template = template or "{func}({data}) AS {label}"
        self._data = data
        self._disable_arguments = disable_arguments
        self._label: str | None = None

    def __call__(self, *args: Any) -> SQLBase:
        """Call sql."""
        if self._disable_arguments and args:
            msg = "Function takes 0 positional arguments"
            raise TypeError(msg)

        self._data = args
        return self

    def label(self, name: str) -> SQLBase:
        """Create AS label."""
        self._label = name
        return self

    @property
    def complete_label(self) -> str:
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

        # label = self.__class__.__name__.lower()
        label = self._name.lower()
        return label

    @property
    def data(self) -> str | list[str]:
        """Return completed."""
        if "]}" in self._template:
            return [
                f"'{arg}'" if isinstance(arg, str) else str(arg)
                for arg in self._data
            ]
        return ", ".join(
            f"'{arg}'" if isinstance(arg, str) else str(arg)
            for arg in self._data
        )

    @data.setter
    def data(self, value: Sequence[Any]) -> None:
        """Return completed."""
        self._data = value

    def __str__(self) -> str:
        """Create string view"""
        # operation = self.__class__.__name__.upper()
        operation = self._name
        label = self.complete_label
        return self._template.format(**{
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

    def __getattr__(self, name: TFunc | str) -> SQLBase:
        """Create func for column."""
        if name not in FUNC_NAMES:
            msg = "{} {} {}".format(
                self.__class__.__name__,
                "object has no attribute",
                name,
            )
            raise AttributeError(msg)
        # func_cls: type[TFuncObj] = type(
        #     name.upper(),
        #     (SQLBase,),
        #     {},
        # )

        name_lower = name.lower()

        if name_lower in "distinct":
            # func_cls.template = "{func} {data}"
            return SQLBase(
                name.upper(),
                template="{func} {data}",
            )

        if name_lower == "like":
            # func_cls.template = "{data[0]} {func} {data[1]}"
            return SQLBase(
                name.upper(),
                template="{data[0]} {func} {data[1]}",
            )

        if name_lower == "random":
            # def __init__(self) -> None:
            #     self._data = ""
            #     self._label = None
            # func_cls.__init__ = __init__
            return SQLBase(
                name.upper(),
                data="",
                disable_arguments=True,
            )
        return SQLBase(
            name.upper(),
        )


func = Func()
