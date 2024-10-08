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
    "Update",
)


class Operation:
    """Base operation."""

    def __init__(self, table: Table) -> None:
        self.table = table

    def _make_operator_query(
        self,
        filter_by: dict[str, int | bool | str | None],
        operator: str = "AND",
        with_values: bool = False,  # noqa: FBT001, FBT002, ARG002
    ) -> tuple[str, tuple[Any]] | str:
        if operator.lower() not in {"and", "or", " ,"}:
            msg = "Incorrect operator."
            raise ValueError(msg)
        operator = f" {operator} "
        if not with_values:
            query = f" {operator} ".join(
                f"{key} is NULL" if value is None else f"{key} = ?"
                for key, value in filter_by.items()
            )
            return (query, tuple(filter(None, filter_by.values())))
        return f" {operator} ".join(
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

    def _execute(
        self,
        query: str,
        parameters: Iterable[Any],
        size: int = 0,
    ) -> list[dict[str, Any]]:
        """Execute with size."""
        result = None
        if size:
            result = self.table.cursor.execute(
                query,
                parameters,
            ).fetchmany(size)
        else:
            result = self.table.cursor.execute(
                query,
                parameters,
            ).fetchall()
        return self._as_list_row(result)

    def _as_list_row(self, items: list[tuple[Any]]) -> list[dict[str, Any]]:
        """Create dict from data."""
        return [
            self.table.row_cls(
                table=self.table,
                changed_columns=set(),
                **dict(zip(self.table.column_names, item)),
            )
            for item in items
        ]

    def _filter(
        self,
        filter_by: dict[str, str | int | bool | None],
        size: int = 0,
        operator: str = "AND",
    ) -> list[dict[str, Any]]:
        """Filter data by filters value where
        key is column name value is content.
        """
        operator_query, parameters = self._make_operator_query(
            filter_by,
            operator,
        )
        query = f"{self.query} WHERE {operator_query}"
        return self._execute(str(query), parameters, size)

    def __call__(
        self,
        size: int = 0,
        operator: str = "AND",
        **filter_by: int | str | bool,
    ) -> list[dict[str, Any]]:
        """Select-query for current table."""
        if filter_by:
            return self._filter(filter_by, size, operator)
        return self._execute(self.query, (), size)


class Insert(Operation):
    """Component for insert data in DB."""

    def query(self, item: Iterable) -> str:
        """Create insert sql-query."""
        query = ", ".join("?" for _ in item)
        return f"INSERT INTO {self.table.name} VALUES({query})"

    def _prepare_input_data(self, data: dict[str, Any]) -> tuple:
        """Validate dict and create tuple for insert."""
        return tuple(
            data.get(name)
            for name in self.table.column_names
        )

    def __call__(
        self,
        data: dict[str, Any] | Iterable[dict[str, Any]],
    ) -> Any:
        """Insert-query for current table."""
        if isinstance(data, dict):
            data = (data,)
        insert_data = tuple(
            self._prepare_input_data(item)
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
        query_and, parameters = self._make_operator_query(filter_by)
        query = f"DELETE FROM {self.table.name} WHERE {query_and}"
        self.table.cursor.execute(query, parameters)
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
    """Componen for updateing table row."""

    @cached_property
    def query(self) -> str:
        """Return base str query."""
        return f"UPDATE {self.table.name} SET "  # noqa: S608

    def __call__(
        self,
        data: dict[str, int | bool | str | None],
        **filter_by: int | bool | str | None,
    ) -> None:
        """Insert-query for current table."""
        if not isinstance(data, dict):
            msg = "Incorrect type for 'data.'"
            raise TypeError(msg)
        if not data:
            msg = "Argument 'data' do not be empty."
            raise ValueError(msg)
        query_coma, parameters = self._make_operator_query(data, operator=" ,")
        query_operator = self._make_operator_query(
            filter_by,
            with_values=True,
        )
        query = self.query + query_coma
        if filter_by:
            query = f"{query} WHERE {query_operator}"
        print(query)
        self.table.cursor.execute(query, parameters)
        self.table.db.commit()

