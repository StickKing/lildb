"""Module contain DB component."""
from __future__ import annotations

import sqlite3
from functools import cached_property
from typing import Any
from typing import Iterator

from table import Table


class DB:
    """DB component."""

    def __init__(self, path: str) -> None:
        """Initialize DB create connection and cursor."""
        self.path = path
        self.connect: sqlite3.Connection = sqlite3.connect(path)
        self.cursor: sqlite3.Cursor = self.connect.cursor()
        self.initialize_tables()

    def initialize_tables(self) -> None:
        """Initialized all db tables."""
        table_names = []
        stmt = "SELECT name FROM sqlite_master WHERE type='table';"
        result = self.cursor.execute(stmt)
        for name in result.fetchall():
            table_names.append(name[0])
            setattr(self, name[0].lower(), Table(self, name[0], data_class_row=True))
        self.table_names = tuple(table_names)

    @cached_property
    def tables(self) -> tuple[Table]:
        """Return all tables obj."""
        return tuple(getattr(self, table_name) for table_name in self.table_names)

    def __iter__(self) -> Iterator[Any]:
        return self.tables.__iter__()

    def drop_tables(self) -> None:
        """Drop all db tables."""
        pass


if __name__ == "__main__":
    db = DB("./local.db")
    for i in db.folder:
        print(i)

    print()
    # print(db.folder[1].delete())
    print()

    # db.folder.delete.filter()

    for i in db.folder:
        print(i.title)

