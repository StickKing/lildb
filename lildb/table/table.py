"""Module contain components for work with db table."""
from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import Iterator
from typing import Literal
from typing import Type
from typing import TypeVar
from typing import cast

from ..enumcls import ResultFetch
from ..operations import Delete
from ..operations import Insert
from ..operations import Query
from ..operations import Select
from ..operations import Update
from ..orm.utils import contain_relation_objects
from ..orm.utils import process_add_relation_objects
from ..orm.utils import refresh_old_obj_by_new
from ..rows import RowDict
from ..rows import make_row_data_cls
from .column import Columns


if TYPE_CHECKING:
    import sqlite3

    from ..db import DB
    from ..orm.typings import TModel


T = TypeVar("T")


__all__ = (
    "Table",
)


class Table(Generic[T]):
    """Component for work with table."""

    __slots__ = (
        "_name",
        "select",
        "insert",
        "delete",
        "update",
        "use_datacls",
        "c",
        "columns",
        "db",
        "column_names",
        "primary_keys",
        "row_cls",
        "_query_obj",
        "execute",
    )

    def __init__(
        self,
        name: str | None = None,
        *,
        use_datacls: bool = False,
        row_cls: type[T] | None = None,
        query_cls: Type[Query] | None = None,
        insert_cls: Type[Insert[T]] | None = None,
        delete_cls: Type[Delete] | None = None,
        update_cls: Type[Update] | None = None,
    ) -> None:
        """Initialize."""
        if name is None:
            msg = "Table name do not be None."
            raise ValueError(msg)

        self._name = name
        self.primary_keys: tuple[str, ...] = ()
        self.use_datacls = use_datacls
        self.row_cls = cast(T, row_cls or RowDict)

        # Operations
        self._query_obj = query_cls if query_cls else Query
        self.select: Select[T] = Select(self)
        self.insert = insert_cls(self) if insert_cls else Insert[T](self)
        self.delete = delete_cls(self) if delete_cls else Delete(self)
        self.update = update_cls(self) if update_cls else Update(self)

        # Sugar
        # self.add = self.insert

        self.c = Columns(self)
        self.columns = self.c

    @property
    def query(self) -> Query[T]:
        """Return new query object for this table."""
        return self._query_obj(self)

    @property
    def name(self) -> str:
        """Return table name."""
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """Return table name."""
        self._name = value

    @property
    def cursor(self) -> sqlite3.Cursor:
        """Shortcut for cursor."""
        return self.db.connect.cursor()

    # @property
    # def execute(
    #     self,
    # ) -> Callable[..., list[tuple] | None]:
    #     """Shortcut for execute.

    #     Args:
    #         query (str): sql query
    #         parameters (MutableMapping | Sequence): data for executing.
    #         Defaults to ().
    #         many (bool): flag for executemany operation. Defaults to False.
    #         size (int | None): size for fetchmany operation. Defaults to None.
    #         result (ResultFetch | None): enum for fetch func. Defaults to None.

    #     Returns:
    #         list[Any] or None

    #     """
    #     return self.db.execute

    def _get_column_names(self) -> tuple[str, ...]:
        """Fetch table column name."""
        stmt = "SELECT name, pk FROM PRAGMA_TABLE_INFO('{}');".format(
            self.name,
        )
        result = self.db.execute(stmt, result=ResultFetch.fetchall)
        names = []
        primary_keys: list[str] = []

        for row in result:
            names.append(row[0])
            if row[1] == 1:
                primary_keys.append(row[0])

        self.primary_keys = tuple(primary_keys)
        return tuple(names)

    # @cached_property
    # def id_exist(self) -> bool:
    #     """Check exist id column."""
    #     return "id" in self.column_names

    def all(self) -> list[T]:
        """Get all rows from table."""
        return self.select()

    def __iter__(self) -> Iterator[T]:
        """Iterate through the row list."""
        return self.select().__iter__()

    def __getitem__(self, index: int) -> T:
        """Get row item by index in list."""
        return self.query().all()[index]

    def get(self, **filter_by: Any) -> T | None:
        """Get one row by filter."""
        return self.query().where(**filter_by).limit(1).first()

    def drop(self, *, init_tables: bool = True) -> None:
        """Drop this table."""
        self.db.execute(f"DROP TABLE IF EXISTS {self.name}")
        # TODO(stickking): What ?!?! replace in db.execute
        # 0000
        if init_tables:
            self.db.initialize_tables()

    def add(
        self,
        *objects: dict | Any,
        returning: bool = False,
    ) -> None | T:
        """Add objects in table."""
        data = []
        objects_with_relation: list[TModel] = []
        insert_obj = None

        for obj in objects:
            if isinstance(obj, dict):
                data.append(obj)
                continue

            obj.table = self
            process_add_relation_objects(obj, ref_type="RelationForeignKey")

            if contain_relation_objects(obj):
                objects_with_relation.append(obj)
                continue

            data.append(obj.get_row_data_as_dict())

        if data:
            insert_obj = self.insert(
                data,
                returning=cast(Literal[True, False], returning),
            )

        new_obj = None
        # too slow
        for obj in objects_with_relation:
            new_obj = self.insert(
                obj.get_row_data_as_dict(),
                returning=True,
            )
            refresh_old_obj_by_new(self, obj, new_obj)
            process_add_relation_objects(obj)

        return insert_obj or new_obj

    def __repr__(self) -> str:
        """Repr view."""
        return f"<{self.__class__.__name__}: {self.name.title()}>"

    def __call__(self, db: DB) -> None:
        """Prepare table obj."""
        self.db = db
        self.execute = db.execute

        self.column_names = self._get_column_names()
        if self.use_datacls and self.row_cls == RowDict:
            self.row_cls = cast(
                T,
                make_row_data_cls(
                    self.name,
                    self.column_names,
                ),
            )
