"""Module contain components for work with db table."""
from __future__ import annotations

import sqlite3
from dataclasses import make_dataclass
from functools import cached_property
from typing import TYPE_CHECKING
from typing import Any
from typing import Iterator

from operations import Delete
from operations import Insert
from operations import Select
from rows import RowDict
from rows import _RowOperationMixin


if TYPE_CHECKING:
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

        if data_class_row:
            data_cls = make_dataclass(
                "RowDataClass",
                [*self.column_names, "table"],
                slots=True,
            )
            self.row_cls = type(
                f"Row{self.name.title()}DataClass",
                (data_cls, _RowOperationMixin),
                {},
            )

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
        for name in self.column_names:
            if name != "id":
                continue
            return True
        return False

    def __iter__(self) -> Iterator[Any]:
        """Iterate through the row list."""
        return self.select().__iter__()

    def __getitem__(self, index: int | str) -> dict[str, Any] | None:
        """Get row item by id or index in list."""
        result = None
        if not self.id_exist:
            result = self.select()[index]
        result = self.select.filter({"id": index})
        return result[0] if result else None
