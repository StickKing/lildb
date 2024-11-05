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
from typing import Sequence

from .column_types import BaseType
from .enumcls import ResultFetch
from .rows import create_result_row


if TYPE_CHECKING:
    from .column_types import TColumnType
    from .db import DB
    from .rows import TRow
    from .table import Table

    TOperator = Literal["AND", "and", "OR", "or", ","]
    TQueryData = dict[str, int | bool | str | None]


__all__ = (
    "Select",
    "Insert",
    "Delete",
    "Update",
    "CreateTable",
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
        *,
        without_parameters: bool = False,
    ) -> str:
        """Build an sql expression with 'and' and 'or' operators."""
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
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        ...


class TableOperation(Operation, ABC):
    """Base operation."""

    def __init__(self, table: Table) -> None:
        self.table = table

    def _make_operator_query(
        self,
        data: TQueryData,
        operator: TOperator = "AND",
        *,
        without_parameters: bool = False,
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
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        ...


class Select(TableOperation):
    """Component for select and filtred DB data."""

    def query(self, columns: Iterable[str] | None = None) -> str:
        """Fetch base query."""
        if columns:
            return "SELECT {} FROM {}".format(  # noqa: S608
                ", ".join(columns),
                self.table.name,
            )
        return f"SELECT * FROM {self.table.name}"  # noqa: S608

    def _execute(
        self,
        query: str,
        parameters: TQueryData,
        *,
        size: int = 0,
        columns: Iterable[str] | None = None,
    ) -> list[TRow]:
        """Execute with size."""
        # result = None
        if size:
            result = self.table.execute(
                query,
                parameters,
                size=size,
                result=ResultFetch.fetchmany,
            )
            return self._as_list_row(result, columns=columns)
        result = self.table.execute(
            query,
            parameters,
            result=ResultFetch.fetchall,
        )
        return self._as_list_row(result, columns=columns)

    def _as_list_row(
        self,
        items: Iterable[tuple[tuple[Any, ...]]],
        *,
        columns: Iterable[str] | None = None,
    ) -> list[TRow]:
        """Create dict from data."""
        row_cls = self.table.row_cls
        if columns:
            row_cls = create_result_row(columns)
            return [
                row_cls(**dict(zip(columns, item)))
                for item in items
            ]
        return [
            row_cls(
                table=self.table,
                **dict(zip(self.table.column_names, item)),
            )
            for item in items
        ]

    def _filter(
        self,
        filter_by: TQueryData,
        *,
        size: int = 0,
        operator: TOperator = "AND",
        columns: Iterable[str] | None = None,
    ) -> list[TRow]:
        """Filter data by filters value where key is
        column name value is content.
        """
        operator_query = self._make_operator_query(
            filter_by,
            operator,
        )
        query = f"{self.query(columns)} WHERE {operator_query}"
        return self._execute(
            query,
            filter_by,
            size=size,
            columns=columns,
        )

    def __call__(
        self,
        *,
        size: int = 0,
        operator: TOperator = "AND",
        columns: Iterable[str] | None = None,
        condition: str | None = None,
        **filter_by: int | str | None,
    ) -> list[TRow]:
        """Select-query for current table."""
        query = self.query(columns)
        if filter_by:
            return self._filter(
                filter_by,
                size=size,
                operator=operator,
                columns=columns,
            )
        if condition:
            query = f"{query} WHERE {condition}"
        return self._execute(query, {}, size=size, columns=columns)


class Insert(TableOperation):
    """Component for insert data in DB."""

    def query(
        self,
        data: Sequence[TQueryData],
    ) -> str:
        """Create insert sql-query."""
        query = ", ".join(
            f":{key}"
            for key in data[0]
        )
        colums_name = ", ".join(
            name
            for name in data[0]
        )
        return f"INSERT INTO {self.table.name} ({colums_name}) VALUES({query})"

    # def _prepare_input_data(self, data: TQueryData) -> tuple:
    #     """Validate dict and create tuple for insert."""
    #     return tuple(
    #         data.get(name)
    #         for name in self.table.column_names
    #         if name in data
    #     )

    def __call__(
        self,
        data: TQueryData | Sequence[TQueryData],
    ) -> None:
        """Insert-query for current table."""
        if not data:
            msg = "Data do not be empty."
            raise ValueError(msg)
        if isinstance(data, dict):
            data = (data,)
        self.table.execute(self.query(data), data, many=True)


class Delete(TableOperation):
    """Component for delete row from db."""

    def query(self) -> str:
        """Fetch base delete query."""
        return f"DELETE FROM {self.table.name} WHERE id=?"  # noqa: S608

    def _filter(
        self,
        filter_by: TQueryData,
        *,
        operator: TOperator = "AND",
    ) -> None:
        """Filter delete row from table."""
        if not filter_by:
            msg = "Value do not be empty."
            raise ValueError(msg)
        query_and = self._make_operator_query(filter_by, operator)
        query = "DELETE FROM {} WHERE {}".format(
            self.table.name,
            query_and
        )
        self.table.execute(query, filter_by)

    def __call__(
        self,
        id: int | Iterable[int] | None = None,  # noqa: A002
        *,
        operator: TOperator = "AND",
        condition: str | None = None,
        **filter_by: int | str | None,
    ) -> None:
        """Delete-query for current table."""
        if isinstance(id, Iterable):
            ids = tuple((id_,) for id_ in id)
            self.table.execute(self.query(), ids, many=True)  # type: ignore
            return
        if id is not None:
            filter_by["id"] = id

        if condition:
            query = f"DELETE FROM {self.table.name} WHERE {condition}"
            self.table.execute(query)  # type: ignore
            return

        self._filter(filter_by, operator=operator)


class Update(TableOperation):
    """Component for updating table row."""

    @cached_property
    def query(self) -> str:
        """Return base str query."""
        return f"UPDATE {self.table.name} SET "  # noqa: S608

    def __call__(
        self,
        data: TQueryData,
        operator: TOperator = "AND",
        condition: str | None = None,
        **filter_by: Any,
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
            operator,
            without_parameters=True,
        )
        query = self.query + query_coma
        if filter_by:
            query = f"{query} WHERE {query_operator}"
            self.table.execute(query, data)  # type: ignore
            return
        if condition:
            query = f"{query} WHERE {condition}"
        self.table.execute(query, data)  # type: ignore


class CreateTable(Operation):
    """Create table object."""

    def __init__(self, db: DB) -> None:
        """Initialize."""
        self.db = db

    def query(
        self,
        *,
        if_not_exists: bool = True,
    ) -> str:
        """Return base SQL command."""
        query = "CREATE TABLE "
        if if_not_exists:
            query += "IF NOT EXISTS "
        return query

    def __call__(
        self,
        table_name: str,
        columns: Sequence[str] | MutableMapping[str, TColumnType],
        table_primary_key: Sequence[str] | None = None,
        *,
        if_not_exists: bool = True,
    ) -> None:
        """Create table in DB.

        Args:
            table_name (str): table name
            columns (Sequence[str] | MutableMapping[str, str]): column name or
            dict column with column types
            table_primary_key (Sequence[str] | None): set table primary key.
            Defaults to None.
            if_not_exists (bool): use 'if not exists' in query.
            Defaults to True.

        Raises:
            TypeError: Incorrect type for columns
            TypeError: Incorrect type for column item

        """
        query = f"{self.query(if_not_exists=if_not_exists)}{table_name}"

        if not isinstance(columns, (Sequence, MutableMapping)):
            msg = "Incorrect type for columns"
            raise TypeError(msg)

        primary_key: str = ""

        if isinstance(table_primary_key, Sequence):
            primary_key = ", PRIMARY KEY(" + ",".join(
                _ for _ in table_primary_key
            ) + ")"

        if (
            isinstance(columns, Sequence) and
            all(isinstance(_, str) for _ in columns)
        ):
            columns_query = ", ".join(columns)
            query = f"{query}({columns_query}{primary_key})"
            self.db.execute(query)
            self.db.initialize_tables()
            return

        if (
            not isinstance(columns, MutableMapping) or
            not all(isinstance(_, BaseType) for _ in columns.values())
        ):
            msg = "Incorrect type for column item"
            raise TypeError(msg)

        columns_query = ", ".join(
            f"{key} {value}"
            for key, value in columns.items()
        )
        query = f"{query} ({columns_query}{primary_key})"
        self.db.execute(query)
        self.db.initialize_tables()
