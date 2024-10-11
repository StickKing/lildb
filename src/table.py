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
    from .rows import ABCRow


__all__ = (
    "Table",
)


class Table:
    """Component for work with table."""

    row_cls: type[ABCRow] = RowDict

    def __init__(
        self,
        name: str,
        *,
        use_datacls: bool = False,
    ) -> None:
        """Initialize."""
        self.name = name
        self.select = getattr(self, "select", Select)(self)
        self.insert = getattr(self, "insert", Insert)(self)
        self.delete = getattr(self, "delete", Delete)(self)
        self.update = getattr(self, "update", Update)(self)

        self.use_datacls = use_datacls

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

    def all(self) -> list[ABCRow]:
        """Get all rows from table."""
        return self.select()

    def __iter__(self) -> Iterator[Any]:
        """Iterate through the row list."""
        return self.select().__iter__()

    def __getitem__(self, index: int | str) -> ABCRow | RowDict | None:
        """Get row item by id or index in list."""
        result = None
        if not self.id_exist:
            result = self.select()[index]
        result = self.select(id=index)
        return result[0] if result else None

    def drop(self) -> None:
        """Drope this table."""
        self.db.execute(f"DROP TABLE IF EXISTS {self.name}")
        self.db.commit()

    def __repr__(self) -> str:
        """Repr view."""
        return f"<{self.__class__.__name__}: {self.name.title()}>"

    def __call__(self, db: DB) -> None:
        """Prepare table obj."""
        self.db = db
        if self.use_datacls:
            self.row_cls = make_row_data_cls(self)
