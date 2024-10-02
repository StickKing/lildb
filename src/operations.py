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


class Operation:
    """Base operation."""

    def __init__(self, table: Table) -> None:
        self.table = table

    def _make_and_query(
        self,
        filter_by: dict[str, int | bool | str | None],
    ) -> str:
        return " AND ".join(
            f"{key} is NULL"
            if value is None else
            f"{key} = '{value}'"
            if isinstance(value, str)
            else f"{key} = {value}"
            for key, value in filter_by.items()
        )


class Select(Operation):
    """Component for select and filtred DB data."""

    @cached_property
    def query(self) -> str:
        """Fetch base query."""
        return f"SELECT * FROM {self.table.name}"

    def _execute(self, query: str, size: int = 0) -> list[dict[str, Any]]:
        """Execute with size."""
        result = None
        if size:
            result = self.table.cursor.execute(query).fetchmany(size)
        else:
            result = self.table.cursor.execute(query).fetchall()
        return self._as_list_row(result)

    def _as_list_row(self, items: list[tuple[Any]]) -> list[dict[str, Any]]:
        """Create dict from data."""
        return [
            self.table.row_cls(
                table=self.table,
                **dict(zip(self.table.column_names, item)),
            )
            for item in items
        ]

    def _filter(
        self,
        filter_by: dict[str, str | int | bool | None],
        size: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Filter data by filters value where
        key is column name value is content.
        """
        query = f"{self.query} WHERE {self._make_and_query(filter_by)}"
        return self._execute(query, size)

    def __call__(
        self,
        size: int = 0,
        **filter_by: int | str | bool,
    ) -> list[dict[str, Any]]:
        """Select-query for current table."""
        if filter_by:
            return self._filter(filter_by, size)
        return self._execute(self.query, size)


class Insert(Operation):
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


class Delete(Operation):
    """Component for delete row from db."""

    def query(self) -> str:
        """Base delete query."""
        return f"DELETE FROM {self.table.name} WHERE id=?"

    def _filter(self, filter_by: dict) -> None:
        """Filtred delete row from table."""
        if not filter_by:
            msg = "Value do not be empty."
            raise ValueError(msg)
        query_and = self._make_and_query(filter_by)
        query = f"DELETE FROM {self.table.name} WHERE {query_and}"
        self.table.cursor.execute(query)
        self.table.db.commit()

    def __call__(
        self,
        id: int | Iterable[int] | None = None,  # noqa: A002
        **filter_by: str | bool | int | None,
    ) -> None:
        """Delete-query for current table."""
        if isinstance(id, Iterable):
            ids = tuple((str(id_),) for id_ in id)
            self.table.cursor.executemany(self.query(), ids)
            self.table.db.commit()
            return
        if id is not None:
            filter_by["id"] = id
        self._filter(filter_by)


class Update(Operation):
    pass
