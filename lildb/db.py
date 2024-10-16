"""Module contain DB component."""
from __future__ import annotations

import sqlite3
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Final
from typing import Iterator
from typing import MutableMapping
from typing import Sequence

from .enumcls import ResultFetch
from .operations import CreateTable
from .table import Table


__all__ = (
    "DB",
)


class DB:
    """DB component."""

    custom_tables: ClassVar[list[Table]]

    _instances: Final[dict[str, DB]] = {}

    def __new__(cls: type[DB], *args: Any, **kwargs: Any) -> DB:
        """Use singleton template. Check path and match paths."""
        if not args and kwargs.get("path") is None:
            msg = "DB.__init__() missing 1 required argument: 'path'"
            raise TypeError(msg)

        path = kwargs["path"] if kwargs.get("path") else args[0]
        normalized_path = cls.normalize_path(Path(path))

        for inst_path, instance in cls._instances.items():
            if cls.normalize_path(Path(inst_path)) == normalized_path:
                return instance

        new_instance = super().__new__(cls)
        cls._instances[path] = new_instance
        return cls._instances[path]

    @classmethod
    def normalize_path(cls: type[DB], path: Path) -> Path:
        """Normalize path."""
        return path.parent.resolve().joinpath(path.name)

    def __init__(
        self,
        path: str,
        *,
        use_datacls: bool = False,
        **connect_params: Any,
    ) -> None:
        """Initialize DB create connection and cursor."""
        self.path = path
        self.connect: sqlite3.Connection = sqlite3.connect(
            path,
            **connect_params,
        )
        self.cursor: sqlite3.Cursor = self.connect.cursor()

        self.commit: Callable[[], None] = self.connect.commit
        self.use_datacls = use_datacls
        self.table_names: set = set()
        self.initialize_tables()

        self.create_table = CreateTable(self)

    def initialize_tables(self) -> None:
        """Initialize all db tables."""
        stmt = "SELECT name FROM sqlite_master WHERE type='table';"
        result = self.cursor.execute(stmt)

        custom_tables = getattr(self, "custom_tables", [])

        for table in custom_tables:
            if hasattr(self, table.name.lower()):
                continue
            table(self)
            setattr(self, table.name.lower(), table)
            self.table_names.add(table.name)

        for name in result.fetchall():
            if hasattr(self, name[0].lower()):
                continue
            new_table = Table(name[0], use_datacls=self.use_datacls)
            new_table(self)
            setattr(
                self,
                name[0].lower(),
                new_table,
            )
            self.table_names.add(name[0].lower())
        if hasattr(self, "tables"):
            del self.tables

    @cached_property
    def tables(self) -> tuple[Table]:
        """Return all tables obj."""
        return tuple(
            getattr(self, table_name)
            for table_name in self.table_names
        )

    def __iter__(self) -> Iterator[Any]:
        """Iterate by db tables."""
        return self.tables.__iter__()

    def drop_tables(self) -> None:
        """Drop all db tables."""
        for table in self.tables:
            table.drop()
        self.initialize_tables()

    def execute(
        self,
        query: str,
        parameters: MutableMapping | Sequence = (),
        *,
        many: bool = False,
        size: int | None = None,
        result: ResultFetch | None = None,
    ) -> list[Any] | None:
        """Single execute to simplify it.

        Args:
            query (str): sql query
            parameters (MutableMapping | Sequence): data for executing.
            Defaults to ().
            many (bool): flag for executemany operation. Defaults to False.
            size (int | None): size for fetchmany operation. Defaults to None.
            result (ResultFetch | None): enum for fetch func. Defaults to None.
        Returns:
            list[Any] or None
        """
        command = query.partition(" ")[0].lower()
        if many:
            self.cursor.executemany(query, parameters)
        else:
            self.cursor.execute(query, parameters)

        if command in {"insert", "delete", "update", "create", "drop"}:
            self.commit()

        # Check result
        if result is None:
            return

        ResultFetch(result)

        result_func: Callable = getattr(self.cursor, result.value)

        if result.value == "fetchmany":
            return result_func(size=size)
        return result_func()

    if TYPE_CHECKING:
        def __getattr__(self, name: str) -> Table:
            """Cringe for dynamic table."""
            ...


if __name__ == "__main__":
    db = DB("local")

