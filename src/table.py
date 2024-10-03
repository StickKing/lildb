"""Module contain components for work with db table."""
from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING
from typing import Any
from typing import Iterator

from operations import Delete
from operations import Insert
from operations import Select
from operations import Update
from rows import ABCRow
from rows import RowDict
from rows import make_row_data_cls


if TYPE_CHECKING:
    import sqlite3

    from .db import DB


class Table:
    """Conponent for work with table."""

    row_cls = RowDict

    def __init__(
        self,
        db: DB,
        name: str,
        data_class_row: bool = False,
    ) -> None:
        """Initialize."""
        self.db = db
        self.name = name
        self.select = Select(self)
        self.insert = Insert(self)
        self.delete = Delete(self)
        self.update = Update(self)

        if data_class_row:
            self.row_cls = make_row_data_cls(self)

    @property
    def cursor(self) -> sqlite3.Cursor:
        """Shortcut for cursor."""
        return self.db.cursor

    @cached_property
    def column_names(self) -> tuple[str, ...]:
        """Fetch table column name."""
        stmt = f"SELECT name FROM PRAGMA_TABLE_INFO('{self.name}');"
        result = self.cursor.execute(stmt)
        return tuple(
            name[0]
            for name in result.fetchall()
        )

    @cached_property
    def id_exist(self) -> bool:
        """Check exist id column."""
        return "id" in self.column_names

    def all(self) -> list[dict[str, Any]]:
        """Get all rows from table."""
        return self.select()

    def __iter__(self) -> Iterator[Any]:
        """Iterate through the row list."""
        return self.select().__iter__()

    def __getitem__(self, index: int | str) -> ABCRow | RowDict:
        """Get row item by id or index in list."""
        result = None
        if not self.id_exist:
            result = self.select()[index]
        result = self.select(id=index)
        return result[0] if result else None
