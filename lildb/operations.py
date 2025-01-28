"""Module contains base operation classes."""
from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import TYPE_CHECKING
from typing import Any
from typing import Generator
from typing import Iterable
from typing import Literal
from typing import MutableMapping
from typing import Sequence

from .column_types import BaseType
from .enumcls import ResultFetch
from .rows import ABCRow
from .rows import create_result_row


if TYPE_CHECKING:
    from .column_types import ForeignKey
    from .db import DB
    from .rows import TRow
    from .sql import SQLBase
    from .table import Column
    from .table import Table

    TOperator = Literal["AND", "and", "OR", "or", ","]
    TQueryData = dict[str, int | bool | str | None]


__all__ = (
    "Query",
    "Select",
    "Insert",
    "Delete",
    "Update",
    "CreateTable",
)


class Operation(ABC):
    """Base operation."""

    __slots__ = ()

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

    __slots__ = ("table",)

    def __init__(self, table: Table) -> None:
        self.table = table

    def _make_operator_query(
        self,
        data: TQueryData,
        operator: TOperator = "AND",
        *,
        without_parameters: bool = False,
        is_null: bool = True,
    ) -> str:
        if operator.lower() not in {"and", "or", ","}:
            msg = "Incorrect operator."
            raise ValueError(msg)

        if not without_parameters:
            return f" {operator} ".join(
                f"{key} is NULL" if (
                    value is None and is_null
                ) else f"{key} = :{key}"
                for key, value in data.items()
            )

        return f" {operator} ".join(
            f"{key} is NULL"
            if (
                value is None and is_null
            ) else
            f"{key} = '{value}'"
            if isinstance(value, str)
            else f"{key} = {value}"
            for key, value in data.items()
        )

    @abstractmethod
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        ...


class Select(TableOperation):
    """Component for select and filtered DB data."""

    __slots__ = ()

    def query(self, columns: Iterable[str] | None = None) -> str:
        """Fetch base query."""
        columns_str = self._generate_columns(columns)
        return f"SELECT {columns_str} FROM {self.table.name}"  # noqa: S608

    def _generate_columns(
        self,
        columns: Iterable[str] | None = None,
        table: Table | None = None,
    ) -> str:
        """Create column str."""
        columns_names = self.table.column_names
        table_name = self.table.name
        if columns:
            columns_names = columns
        return ", ".join(
            f"`{table_name}`.{name}"
            for name in columns_names
        )

    def _execute(
        self,
        query: str,
        parameters: TQueryData,
        *,
        size: int = 0,
        columns: Iterable[str] | None = None,
        return_generator: bool = False,
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
        else:
            result = self.table.execute(
                query,
                parameters,
                result=ResultFetch.fetchall,
            )
        if return_generator:
            self._as_generator_row(result, columns=columns)
        return self._as_list_row(result, columns=columns)

    def _as_list_row(
        self,
        items: Iterable[tuple[tuple[Any, ...]]],
        *,
        columns: Iterable[str] | None = None,
    ) -> list[TRow]:
        """Create list rows."""
        row_cls = self.table.row_cls
        columns_name = self.table.column_names
        if columns:
            row_cls = create_result_row(columns)
            columns_name = columns
        return [
            row_cls(
                table=self.table,
                **dict(zip(columns_name, item)),
            )
            for item in items
        ]

    def _as_generator_row(
        self,
        items: Iterable[tuple[tuple[Any, ...]]],
        *,
        columns: Iterable[str] | None = None,
    ) -> Generator[ABCRow, Any, None]:
        """Create rows generator."""
        row_cls = self.table.row_cls
        columns_name = self.table.column_names
        if columns:
            row_cls = create_result_row(columns)
            columns_name = columns
        for item in items:
            yield row_cls(
                table=self.table,
                **dict(zip(columns_name, item)),
            )

    def _filter(
        self,
        filter_by: TQueryData,
        *,
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
        return f"{self.query(columns)} WHERE {operator_query}"

    def __call__(
        self,
        *,
        size: int = 0,
        operator: TOperator = "AND",
        columns: Iterable[str] | None = None,
        condition: str | None = None,
        return_generator: bool = False,
        **filter_by: int | str | None,
    ) -> list[TRow]:
        """Select-query for current table."""
        query = self.query(columns)
        if filter_by:
            query = self._filter(
                filter_by,
                operator=operator,
                columns=columns,
            )
        if condition:
            query = "{} WHERE {}".format(
                self.query(columns),
                condition,
            )
        return self._execute(
            query,
            filter_by,
            size=size,
            columns=columns,
            return_generator=return_generator,
        )


class Insert(TableOperation):
    """Component for insert data in DB."""

    __slots__ = ()

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

    __slots__ = ()

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

    __slots__ = ("query",)

    def __init__(self, table: Table) -> None:
        super().__init__(table)
        self.query = f"UPDATE {self.table.name} SET "

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
        new_column_values = self._make_operator_query(
            data,
            operator=",",
            is_null=False,
        )
        filter_value = self._make_operator_query(
            filter_by,
            operator,
            without_parameters=True,
        )
        query = self.query + new_column_values
        if filter_by:
            query = f"{query} WHERE {filter_value}"
            self.table.execute(query, data)  # type: ignore
            return
        if condition:
            query = f"{query} WHERE {condition}"
        self.table.execute(query, data)  # type: ignore


class Query(TableOperation):
    """Create sql query with more params."""

    __slots__ = (
        "_body",
        "_filters",
        "_having",
        "_orders",
        "_groups",
        "_limit",
        "_offset",
        "columns",
    )

    _body: str
    _filters: tuple[Any, ...]
    _having: tuple[Any, ...]
    _orders: tuple[Any, ...]
    _groups: tuple[Any, ...]
    _limit: int
    _offset: int
    columns: Iterable[str | SQLBase] | None

    def __init__(
        self,
        table: Table | None = None,
    ) -> None:
        """Initialize query and create it body

        Args:
            rowcls (ABCRow | None, optional): row cls for existents table.
                Defaults to None.
        """
        self.table = table

    def _create_query_str(self) -> str:
        """Create sql query with all existent attrs."""
        where_str = " ".join(map(lambda i: str(i), self._filters))
        if where_str:
            where_str = " WHERE " + where_str

        group_by_str = ""
        if self._groups:
            group_by_str = " GROUP BY " + ", ".join(self._groups)

        limit_str = ""
        if self._limit:
            limit_str = f" LIMIT {self._limit}"
            if self._offset:
                limit_str += f" OFFSET {self._offset}"

        having_str = " ".join(self._having)
        if having_str:
            having_str = " HAVING " + having_str

        order_by_str = ", ".join(self._orders)
        if order_by_str:
            order_by_str = " ORDER BY " + order_by_str

        table = self.table.name
        return "SELECT {} FROM {}{}{}{}{}{}".format(
            self._body,
            table,
            where_str,
            group_by_str,
            having_str,
            order_by_str,
            limit_str,
        )

    __str__ = _create_query_str

    def _as_list_row(
        self,
        items: Iterable[tuple[tuple[Any, ...]]],
    ) -> list[ABCRow]:
        """Create list rows."""
        row_cls = self.table.row_cls
        columns_name: Iterable[str] = self.table.column_names
        if self.columns:
            columns_name = self.result_row_column_names
            row_cls = create_result_row(columns_name)
        return [
            row_cls(
                table=self.table,
                **dict(zip(columns_name, item)),
            )
            for item in items
        ]

    @property
    def result_row_column_names(self) -> list[str]:
        """Prepare column names for class row."""
        return [
            col.row_name
            for col in self.columns
        ]

    def _prepare_column(self, column: str | SQLBase | Column) -> str:
        """Prepare one column."""
        # if column is Column or SQLBase
        column = str(column)
        if column in self.table.column_names:
            return f"`{self.table.name}`.{column}"
        return column

    def _generate_column_names(
        self,
        columns: Iterable[str | SQLBase] | None = None,
    ) -> str:
        """Create column str."""
        columns_names = self.table.column_names  # type: ignore
        # table_name = self.table.name  # type: ignore
        if columns:
            columns_names = columns  # type: ignore
        return ", ".join(
            self._prepare_column(name)
            for name in columns_names
        )

    def _execute(self, query: str, size: int | None = None) -> list[tuple]:
        """Execute query."""
        if size:
            return self.table.execute(
                query,
                size=size,
                result=ResultFetch.fetchmany,
            )
        return self.table.execute(
            query,
            result=ResultFetch.fetchall,
        )

    def limit(self, limit_number: int) -> Query:
        """Use limit in sql query."""
        if not isinstance(limit_number, int):
            msg = f"Limit not be {type(limit_number)}"
            raise TypeError(msg)
        self._limit = limit_number
        return self

    def offset(self, offset_number: int) -> Query:
        """Use offset in sql query."""
        if not isinstance(offset_number, int):
            msg = f"Offset not be {type(offset_number)}"
            raise TypeError(msg)
        self._offset = offset_number
        return self

    def order_by(self, *args: str, **orders: Literal["asc", "desc"]) -> Query:
        """Use order by in query."""
        order_types = {"asc", "desc"}
        if args:
            self._orders += tuple(map(lambda i: str(i), args))
            return self

        assert all(value.lower() in order_types for value in orders.values())
        self._orders += tuple(
            f"{key} {value}"
            for key, value in orders.items()
        )
        return self

    def _sum_tuples(self, attr_name: str, value: tuple) -> None:
        """Summing tuples."""
        setattr(
            self,
            attr_name,
            getattr(self, attr_name) + value,
        )

    def _add_condition(
        self,
        condition: str,
        operator: TOperator = "AND",
        in_having: bool = False,
    ) -> None:
        """Adding condition in _filter."""
        attr_name = "_having" if in_having else "_filters"
        condition_orig = condition.strip()
        condition = condition.strip().lower()
        # if self._filters:
        if getattr(self, attr_name):
            if (
                condition.startswith("and") or
                condition.startswith("or")
            ):
                self._sum_tuples(attr_name, (condition_orig,))
                return
            condition_orig = f"{operator} {condition_orig}"
            self._sum_tuples(attr_name, (condition_orig,))
            return

        if condition.startswith("and"):
            condition_orig = condition_orig[3:].strip()
        elif condition.startswith("or"):
            condition_orig = condition_orig[1:].strip()
        self._sum_tuples(attr_name, (condition_orig,))

    def _add_filters(
        self,
        filter_by: Any,
        filter_operator: TOperator = "AND",
        operator: TOperator = "AND",
        in_having: bool = False,
    ) -> None:
        attr_name = "_having" if in_having else "_filters"
        filter_str = self._make_operator_query(
            filter_by,
            filter_operator,
            without_parameters=True,
        )
        # if self._filters:
        if getattr(self, attr_name):
            self._sum_tuples(attr_name, (f"{operator} {filter_str}",))
            # self._filters += (f"{operator} {filter_str}",)
            return
        self._sum_tuples(attr_name, (filter_str,))
        # self._filters += (filter_str,)

    def where(
        self,
        *args: str,
        condition: str | None = None,
        filter_operator: TOperator = "AND",
        operator: TOperator = "AND",
        **filter_by: Any,
    ) -> Query:
        """Use where construction in sql query."""
        # TODO (stickking): check operator and remove from it ','
        # 0000
        if args:
            for arg in args:
                self._add_condition(str(arg))
            return self
        if condition:
            self._add_condition(condition, operator)
            return self
        self._add_filters(filter_by, operator, filter_operator)
        return self

    def having(
        self,
        *args: str,
        condition: str | None = None,
        filter_operator: TOperator = "AND",
        operator: TOperator = "AND",
        **filter_by: Any,
    ) -> Query:
        """Use where construction in sql query."""
        # TODO (stickking): check operator and remove from it ','
        # 0000
        if args:
            for arg in args:
                self._add_condition(arg)
            return self
        if condition:
            self._add_condition(condition, operator, in_having=True)
            return self
        self._add_filters(filter_by, operator, filter_operator, in_having=True)
        return self

    def group_by(self, *args: str | Column) -> Query:
        """Use group by operation."""
        self._groups += tuple(map(str, args))
        return self

    def exists(self) -> bool:
        """Contain all query in exists command."""
        query = self._create_query_str()
        query = f"SELECT EXISTS({query})"
        result = self._execute(query)
        if not result or not result[0]:
            return False
        return bool(result[0][0])

    def count(self) -> int:
        """Contain all query in exists command."""
        first_column = self._body.split(", ")[0]
        if not first_column:
            return 0
        self._body = f"COUNT({first_column})"
        query = self._create_query_str()
        result = self._execute(query)
        if not result or not result[0]:
            return 0
        return result[0][0]

    def first(self) -> ABCRow | None:
        """Return first item from query."""
        self.limit(1)
        query = self._create_query_str()
        result = self._execute(query, 0)
        items = self._as_list_row(result)
        if not items:
            return None
        return items[0]

    one = first

    def all(
        self,
        size: int = 0,
        *,
        only_data: bool = False,
    ) -> list[ABCRow]:
        """Return first item from query."""
        query = self._create_query_str()
        result = self._execute(query, size)
        if only_data:
            return result
        return self._as_list_row(result)

    def generative_all(self, limit: int) -> Generator[ABCRow, Any, None]:
        """Return first item from query."""
        row_cls = self.table.row_cls
        columns_name = self.table.column_names
        if self.columns:
            columns_name = self.result_row_column_names
            row_cls = create_result_row(columns_name)

        offset = 0
        self.limit(limit)
        while True:
            self.offset(offset * limit)
            items = self._execute(self._create_query_str(), 0)
            offset += 1

            if not items:
                break

            for item in items:
                yield row_cls(
                    table=self.table,
                    **dict(zip(columns_name, item)),
                )

    def __iter__(self) -> Iterable:
        """Iteration by data."""
        return iter(self.all())

    def __call__(
        self,
        *columns: SQLBase | Column,
        table: Table | None = None,
    ) -> Query:
        """Initialize base args and create query body."""
        self._body = ""
        self._filters = ()
        self._having = ()
        self._orders = ()
        self._groups = ()
        self._limit = 0
        self._offset = 0
        self.columns = None

        if self.table is None and table:
            self.table = table
            self._body = self._generate_column_names()
        elif columns:
            self.columns = columns
            self._body = self._generate_column_names(columns)
        else:
            self._body = self._generate_column_names()
        return self


class CreateTable(Operation):
    """Create table object."""

    __slots__ = ("db", )

    def __init__(self, db: DB) -> None:
        """Initialize."""
        self.db = db

    def query(
        self,
        table_name: str,
        columns: str,
        table_primary_key: str,
        foreign_keys: str,
        *,
        if_not_exists: bool = True,
    ) -> str:
        """Return SQL command."""
        query = "CREATE TABLE "
        if if_not_exists:
            query += "IF NOT EXISTS "

        # if table_primary_key and foreign_keys:
        #     foreign_keys = ", " + foreign_keys

        pr_fr_keys = table_primary_key + foreign_keys

        return f"{query} `{table_name}` ({columns}{pr_fr_keys})"

    def _genarate_table_primary_keys(
        self,
        table_primary_key: Sequence[str] | None = None,
    ) -> str:
        """Create table primary keys."""
        if isinstance(table_primary_key, Sequence):
            return ", PRIMARY KEY(" + ",".join(
                _ for _ in table_primary_key
            ) + ")"
        return ""

    def _genatate_table_foreign_keys(
        self,
        foreign_keys: Sequence[ForeignKey] | None = None,
    ) -> str:
        """Create table foreign keys."""
        if foreign_keys and isinstance(foreign_keys, Sequence):
            return ", " + ", ".join(
                key()
                for key in foreign_keys
            )
        return ""

    def _generate_columns(
        self,
        columns: Sequence[str] | MutableMapping[str, Any],
    ) -> str:
        """Create column name with type if it exists."""
        if (
            isinstance(columns, Sequence) and
            all(isinstance(_, str) for _ in columns)
        ):
            return ", ".join(columns)

        if (
            not isinstance(columns, MutableMapping) or
            not all(isinstance(_, BaseType) for _ in columns.values())
        ):
            msg = "Incorrect type for column item"
            raise TypeError(msg)

        return ", ".join(
            f"`{key}` {value}"
            for key, value in columns.items()
        )

    def __call__(
        self,
        table_name: str,
        columns: Sequence[str] | MutableMapping[str, Any],
        table_primary_key: Sequence[str] | None = None,
        foreign_keys: Sequence[ForeignKey] | None = None,
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
        if not isinstance(columns, (Sequence, MutableMapping)):
            msg = "Incorrect type for columns"
            raise TypeError(msg)

        columns = self._generate_columns(
            columns,
        )
        table_primary_keys = self._genarate_table_primary_keys(
            table_primary_key,
        )
        foreign_keys = self._genatate_table_foreign_keys(
            foreign_keys
        )

        query = self.query(
            table_name,
            columns,
            table_primary_keys,
            foreign_keys,
            if_not_exists=if_not_exists,
        )
        self.db.execute(query)
