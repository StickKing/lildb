"""Module contain DB component."""
from __future__ import annotations

import sqlite3
from functools import cached_property
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Iterator

from operations import CreateTable
from table import Table


__all__ = (
    "DB",
)


class DB:
    """DB component."""

    custom_tables: ClassVar[list[Table]]

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

        self.execute: sqlite3.Cursor = self.cursor.execute
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

    if TYPE_CHECKING:
        def __getattr__(self, name: str) -> Table:
            """Cringe for dynamic table."""
            ...


if __name__ == "__main__":
    db = DB("./local.db")

