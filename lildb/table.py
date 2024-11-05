"""Module contain components for work with db table."""
from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Generic
from typing import Iterator

from .enumcls import ResultFetch
from .operations import Delete
from .operations import Insert
from .operations import Select
from .operations import Update
from .rows import RowDict
from .rows import TRow
from .rows import make_row_data_cls


if TYPE_CHECKING:
    import sqlite3

    from .db import DB


__all__ = (
    "Table",
)


class Table(Generic[TRow]):
    """Component for work with table."""

    row_cls: type[TRow] = RowDict  # type: ignore
    name: str | None = None

    def __init__(
        self,
        name: str | None = None,
        *,
        use_datacls: bool = False,
    ) -> None:
        """Initialize."""
        self.name = self.name or name
        if self.name is None:
            msg = "Table name do not be None."
            raise ValueError(msg)

        self.use_datacls = use_datacls

        # Operations
        self.select = getattr(self, "select", Select)(self)
        self.insert = getattr(self, "insert", Insert)(self)
        self.delete = getattr(self, "delete", Delete)(self)
        self.update = getattr(self, "update", Update)(self)

        # Sugar
        self.add = self.insert

    @property
    def cursor(self) -> sqlite3.Cursor:
        """Shortcut for cursor."""
        return self.db.connect.cursor()

    @property
    def execute(
        self,
    ) -> Callable[..., list[Any] | None]:
        """Shortcut for execute.

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
        return self.db.execute


    @cached_property
    def column_names(self) -> tuple[str, ...]:
        """Fetch table column name."""
        stmt = "SELECT name FROM PRAGMA_TABLE_INFO('{}');".format(
            self.name,
        )
        result = self.db.execute(stmt, result=ResultFetch.fetchall)
        return tuple(
            name[0].lower()
            for name in result
        )

    @cached_property
    def id_exist(self) -> bool:
        """Check exist id column."""
        return "id" in self.column_names

    def all(self) -> list[TRow]:
        """Get all rows from table."""
        return self.select()

    def __iter__(self) -> Iterator[Any]:
        """Iterate through the row list."""
        return self.select().__iter__()

    def __getitem__(self, index: int | str) -> TRow | None:
        """Get row item by id or index in list."""
        result = None
        if not self.id_exist:
            result = self.select()[index]
        result = self.select(id=index)
        return result[0] if result else None

    def get(self, **filter_by: str | int) -> TRow | None:
        """Get one row by filter."""
        result = self.select(size=1, **filter_by)
        return result[0] if result else None

    def drop(self, *, init_tables: bool = True) -> None:
        """Drop this table."""
        self.db.execute(f"DROP TABLE IF EXISTS {self.name}")
        if init_tables:
            self.db.initialize_tables()

    def __repr__(self) -> str:
        """Repr view."""
        return f"<{self.__class__.__name__}: {self.name.title()}>"

    def __call__(self, db: DB) -> None:
        """Prepare table obj."""
        self.db = db
        if self.use_datacls and self.row_cls == RowDict:
            self.row_cls = make_row_data_cls(self)
