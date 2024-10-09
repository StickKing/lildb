"""Module contain DB component."""
from __future__ import annotations

import sqlite3
from functools import cached_property
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Iterator

from operations import CreateTable
from table import Table


class DB:
    """DB component."""

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
        self.initialize_tables(use_datacls=use_datacls)

        self.create_table = CreateTable(self)

    def initialize_tables(self, *, use_datacls: bool = False) -> None:
        """Initialized all db tables."""
        table_names = []
        stmt = "SELECT name FROM sqlite_master WHERE type='table';"
        result = self.cursor.execute(stmt)

        for name in result.fetchall():
            table_names.append(name[0])
            if hasattr(self, name[0].lower()):
                continue
            setattr(
                self,
                name[0].lower(),
                Table(self, name[0], use_datacls=use_datacls),
            )

        self.table_names = tuple(table_names)

    @cached_property
    def tables(self) -> tuple[Table]:
        """Return all tables obj."""
        return tuple(
            getattr(self, table_name)
            for table_name in self.table_names
        )

    def __iter__(self) -> Iterator[Any]:
        """Iteration by db tables."""
        return self.tables.__iter__()

    def drop_tables(self) -> None:
        """Drop all db tables."""
        pass


    if TYPE_CHECKING:
        def __getattr__(self, name: str) -> Table:
            """Cringe for dynamic table."""
            ...


if __name__ == "__main__":
    db = DB("./local.db")
    for i in db.folder:
        print(i)


    print()
    row = db.folder[3]
    print(row)
    row["title"] = "World"
    row.update()
    print(row)
    print(row.changed_columns)

    for i in db.folder:
        print(i)

