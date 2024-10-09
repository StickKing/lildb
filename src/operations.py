"""Module contains base operation classes."""
from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING
from typing import Any
from typing import Iterable
from typing import Literal
from typing import MutableMapping
from typing import TypeAlias


if TYPE_CHECKING:
    from db import DB
    from rows import ABCRow
    from table import Table

    TOperator: TypeAlias = Literal["AND", "and", "OR", "or", ","]
    TQueryData: TypeAlias = dict[str, int | bool | str | None]


__all__ = (
    "Select",
    "Insert",
    "Delete",
    "Update",
)


class Operation(ABC):
    """Base operation."""

    @abstractmethod
    def __init__(self) -> None:
        ...

    def _make_operator_query(
        self,
        data: TQueryData,
        operator: TOperator = "AND",
        without_parameters: bool = False,  # noqa: FBT001, FBT002, ARG002
    ) -> str:
        if operator.lower() not in {"and", "or", ","}:
            msg = "Incorrect operator."
            raise ValueError(msg)

        if not without_parameters:
            return f" {operator} ".join(
                f"{key} is NULL" if value is None else f"{key} = :{key}"
                for key, value in data.items()
            )

        return f" {operator} ".join(
            f"{key} is NULL"
            if value is None else
            f"{key} = '{value}'"
            if isinstance(value, str)
            else f"{key} = {value}"
            for key, value in data.items()
        )

    @abstractmethod
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        ...


class TableOperation(Operation, ABC):
    """Base operation."""

    def __init__(self, table: Table) -> None:
        self.table = table

    def _make_operator_query(
        self,
        data: TQueryData,
        operator: TOperator = "AND",
        without_parameters: bool = False,  # noqa: FBT001, FBT002, ARG002
    ) -> str:
        if operator.lower() not in {"and", "or", ","}:
            msg = "Incorrect operator."
            raise ValueError(msg)

        if not without_parameters:
            return f" {operator} ".join(
                f"{key} is NULL" if value is None else f"{key} = :{key}"
                for key, value in data.items()
            )

        return f" {operator} ".join(
            f"{key} is NULL"
            if value is None else
            f"{key} = '{value}'"
            if isinstance(value, str)
            else f"{key} = {value}"
            for key, value in data.items()
        )

    @abstractmethod
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        ...


class Select(TableOperation):
    """Component for select and filtred DB data."""

    @cached_property
    def query(self) -> str:
        """Fetch base query."""
        return f"SELECT * FROM {self.table.name}"

    def _execute(
        self,
        query: str,
        parameters: TQueryData,
        size: int = 0,
    ) -> list[ABCRow]:
        """Execute with size."""
        result = None
        if size:
            result = self.table.cursor.execute(
                query,
                parameters,
            ).fetchmany(size)
            return self._as_list_row(result)
        result = self.table.cursor.execute(
            query,
            parameters,
        ).fetchall()
        return self._as_list_row(result)

    def _as_list_row(self, items: list[tuple[Any]]) -> list[ABCRow]:
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
        filter_by: TQueryData,
        size: int = 0,
        operator: TOperator = "AND",
    ) -> list[ABCRow]:
        """
        Filter data by filters value where
        key is column name value is content.
        """
        operator_query = self._make_operator_query(
            filter_by,
            operator,
        )
        query = f"{self.query} WHERE {operator_query}"
        return self._execute(str(query), filter_by, size)

    def __call__(
        self,
        size: int = 0,
        operator: TOperator = "AND",
        **filter_by: TQueryData,
    ) -> list[ABCRow]:
        """Select-query for current table."""
        if filter_by:
            return self._filter(filter_by, size, operator)
        return self._execute(self.query, {}, size)


class Insert(TableOperation):
    """Component for insert data in DB."""

    def query(self, item: Iterable) -> str:
        """Create insert sql-query."""
        query = ", ".join("?" for _ in item)
        return f"INSERT INTO {self.table.name} VALUES({query})"

    def _prepare_input_data(self, data: Iterable[TQueryData]) -> tuple:
        """Validate dict and create tuple for insert."""
        return tuple(
            data.get(name)
            for name in self.table.column_names
        )

    def __call__(
        self,
        data: TQueryData | Iterable[TQueryData],
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


class Delete(TableOperation):
    """Component for delete row from db."""

    def query(self) -> str:
        """Base delete query."""
        return f"DELETE FROM {self.table.name} WHERE id=?"

    def _filter(self, filter_by: TQueryData) -> None:
        """Filtred delete row from table."""
        if not filter_by:
            msg = "Value do not be empty."
            raise ValueError(msg)
        query_and = self._make_operator_query(filter_by)
        query = f"DELETE FROM {self.table.name} WHERE {query_and}"
        self.table.cursor.execute(query, filter_by)
        self.table.db.commit()

    def __call__(
        self,
        id: int | Iterable[int] | None = None,  # noqa: A002
        **filter_by: TQueryData,
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


class Update(TableOperation):
    """Component for updating table row."""

    @cached_property
    def query(self) -> str:
        """Return base str query."""
        return f"UPDATE {self.table.name} SET "  # noqa: S608

    def __call__(
        self,
        data: TQueryData,
        **filter_by: TQueryData,
    ) -> None:
        """Insert-query for current table."""
        if not isinstance(data, dict):
            msg = "Incorrect type for 'data.'"
            raise TypeError(msg)
        if not data:
            msg = "Argument 'data' do not be empty."
            raise ValueError(msg)
        query_coma = self._make_operator_query(data, operator=",")
        query_operator = self._make_operator_query(
            filter_by,
            without_parameters=True,
        )
        query = self.query + query_coma
        if filter_by:
            query = f"{query} WHERE {query_operator}"
        self.table.cursor.execute(query, data)
        self.table.db.commit()


class CreateTable(Operation):

    query = "CREATE TABLE "

    def __init__(self, db: DB) -> None:
        self.db = db

    def _check_column_type(self, columns: MutableMapping) -> dict[str, str]:
        """Initialize type for columns."""
        type_dict = {
            int: "INTEGER",
            float: "REAL",
            str: "TEXT",
            None: "NULL",
        }
        return [
            f"{name} {type_dict.get(type_)}"
            for name, type_ in columns.items()
        ]

    def __call__(
        self,
        table_name: str,
        columns: Iterable[str] | MutableMapping[str, str],
    ) -> None:
        query = f"{self.query}{table_name}"

        if isinstance(columns, list):
            columns_query = ",".join(columns)
            query = f"{query}({columns_query})"
            self.db.execute(query)
            self.db.commit()
            return

        columns_query = ",".join(self._check_column_type(columns))
        query = f"{query} ({columns_query})"
        self.db.execute(query)
        self.db.commit()
