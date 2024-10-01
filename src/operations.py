"""Module contains base operation classes."""
from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING
from typing import Any
from typing import Iterable


if TYPE_CHECKING:
    from table import Table


__all__ = (
    "Select",
    "Insert",
    "Delete",
)


class Opertion:
    """Base operation."""

    def __init__(self, table: Table) -> None:
        self.table = table


class Select(Opertion):
    """Component for select and filtred DB data."""

    @cached_property
    def query(self) -> str:
        """Fetch base query."""
        return f"SELECT * FROM {self.table.name}"

    def execute(self, query: str, size: int = 0) -> list[dict[str, Any]]:
        """Execute with size."""
        result = None
        if size:
            result = self.table.cursor.execute(query).fetchmany(size)
        else:
            result = self.table.cursor.execute(query).fetchall()
        return self.as_list_dict(result)

    def as_list_dict(self, items: list[tuple[Any]]) -> list[dict[str, Any]]:
        """Create dict from data."""
        return [
            self.table.row_cls(
                table=self.table,
                **dict(zip(self.table.column_names, item)),
            )
            for item in items
        ]

    def filter(
        self,
        filters: dict[str, str | int | bool],
        size: int = 0,
    ) -> list[dict[str, Any]]:
        """Filter data by filters value where
        key is column name value is content.
        """
        query_and: str = " AND ".join(
            f"{key} = {item}"
            if item is not None else f"{key} is NULL"
            for key, item in filters.items()
        )
        query = f"{self.query} WHERE {query_and}"
        return self.execute(query, size)

    def __call__(self, size: int = 0) -> Any:
        return self.execute(self.query, size)


class Insert(Opertion):
    """Component for insert data in DB."""

    def query(self, item: Iterable) -> str:
        """Create insert sql-query."""
        query = ", ".join("?" for _ in item)
        return f"INSERT INTO {self.table.name} VALUES({query})"

    def _prepare_item(self, item: dict[str, Any]) -> tuple:
        """Validate dict and create tuple for insert."""
        return tuple(
            item[name] if name in item else None
            for name in self.table.column_names
        )

    def __call__(
        self,
        data: dict[str, Any] | Iterable[dict[str, Any]],
    ) -> Any:
        """."""
        if isinstance(data, dict):
            data = (data,)
        insert_data = tuple(
            self._prepare_item(item)
            for item in data
        )
        if not all(insert_data):
            return
        self.table.cursor.executemany(self.query(insert_data[0]), insert_data)
        self.table.db.connect.commit()


class Delete(Opertion):
    """Component for delete row from db."""

    def query(self) -> str:
        return f"DELETE FROM {self.table.name} WHERE id=?"

    def filter(self, value: dict) -> None:
        """Filtred delete row from table."""
        if not value:
            msg = "Value do not be empty."
            raise ValueError(msg)
        query_and: str = " AND ".join(
            f"{key} = {item}"
            if item is not None else f"{key} is NULL"
            for key, item in value.items()
        )
        query = f"DELETE FROM {self.table.name} WHERE {query_and}"
        self.table.cursor.execute(query)
        self.table.db.connect.commit()

    def __call__(self, id_: int | Iterable[int]) -> None:
        if not isinstance(id_, (int, Iterable)):
            msg = "Wrong type."
            raise TypeError(msg)
        ids = (id_,)
        ids = tuple((str(id_),) for id_ in ids)
        self.table.cursor.executemany(self.query(), ids)
        self.table.db.connect.commit()
