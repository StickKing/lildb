"""Module contain DB component."""
from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict
from functools import cached_property
from functools import singledispatchmethod
from pathlib import Path
from queue import Queue
from threading import Event
from threading import Thread
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Iterator
from typing import MutableMapping
from typing import Sequence
from typing import TypeVar

from typing_extensions import TypeAlias
from typing_extensions import dataclass_transform

from .enumcls import ResultFetch
from .operations import CreateTable
from .orm.model import create_table_and_data_cls_row
from .table.table import Table


_T = TypeVar("_T")

if TYPE_CHECKING:
    from .orm import TModelClass
    from .orm.utils import TableData
    TRegistredTables: TypeAlias = defaultdict[Any, list[TableData]]


__all__ = (
    "DB",
    "ThreadDB",
)


class DB:
    """DB component."""

    _instances: ClassVar[dict[str, DB]] = {}
    _registered_tables_data: ClassVar[TRegistredTables] = defaultdict(
        list,
    )
    orm_classes: ClassVar[dict[str, type[Any]]] = {}

    def __new__(cls: type[DB], *args: Any, **kwargs: Any) -> DB:
        """Use singleton template. Check path and match paths."""
        if not args and kwargs.get("path") is None:
            msg = "DB.__init__() missing 1 required argument: 'path'"
            raise TypeError(msg)

        path = kwargs["path"] if kwargs.get("path") else args[0]
        normalized_path = cls.normalize_path(Path(path))

        # for inst_path, instance in cls._instances.items():
        #     if inst_path == normalized_path:
        #         return instance
        if normalized_path in cls._instances:
            return cls._instances[normalized_path]

        new_instance = super().__new__(cls)
        cls._instances[normalized_path] = new_instance
        return cls._instances[normalized_path]

    @classmethod
    def normalize_path(cls: type[DB], path: Path) -> Path:
        """Normalize path."""
        return path.parent.resolve().joinpath(path.name)

    def __init__(
        self,
        path: str,
        *,
        use_datacls: bool = False,
        debug: bool = False,
        auto_create_registred_tables: bool = True,
        **connect_params: Any,
    ) -> None:
        """Initialize DB create connection and cursor."""
        if debug:
            logging.basicConfig(level=logging.DEBUG)
        self.path = self.normalize_path(Path(path))
        self.connect: sqlite3.Connection = sqlite3.connect(
            self.path,
            **connect_params,
        )
        self.use_datacls = use_datacls
        self.table_names: set = set()
        self.create_table = getattr(self, "create_table", CreateTable)(self)

        if auto_create_registred_tables:
            self.create_registred_table()

        self.initialize_tables()

    def initialize_tables(self) -> None:
        """Initialize all db tables."""
        stmt = (
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND substr(`name`, 1, 6) != 'sqlite';"
        )
        result = self.execute(stmt, result=ResultFetch.fetchall)

        custom_table_names = set()

        # table like class attributes
        for attr in filter(
            lambda i: not i.startswith("_"),
            dir(self.__class__),
        ):
            custom_table = getattr(self, attr)
            if not isinstance(custom_table, Table):
                continue
            custom_table_names.add(custom_table.name.lower())
            custom_table(self)

        # auto create exists table
        for name in result:
            table_name = name[0].lower()
            self.table_names.add(table_name)

            if table_name in custom_table_names:
                continue

            new_table = Table(name[0], use_datacls=self.use_datacls)
            new_table(self)
            setattr(
                self,
                table_name,
                new_table,
            )
            self.table_names.add(table_name)

        if hasattr(self, "tables"):
            del self.tables

    @cached_property
    def tables(self) -> tuple[Table]:
        """Return all tables obj."""
        return tuple(
            getattr(self, table_name)
            for table_name in self.table_names
        )

    def __iter__(self) -> Iterator[Any]:
        """Iterate by db tables."""
        return self.tables.__iter__()

    def drop_tables(self) -> None:
        """Drop all db tables."""
        for table in self.tables:
            table.drop(init_tables=False)
        self.initialize_tables()

    def execute(
        self,
        query: str,
        parameters: MutableMapping | Sequence = (),
        *,
        many: bool = False,
        size: int | None = None,
        result: ResultFetch | None = None,
    ) -> list[Any] | None:
        """Single execute to simplify it.

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
        command = query.partition(" ")[0].lower()
        cursor = self.connect.cursor()
        logging.info("Query: %s", query)
        logging.info("Parameters: %s", parameters)
        if many:
            cursor.executemany(query, parameters)
        else:
            cursor.execute(query, parameters)

        if (
            command in {"delete", "update", "create", "drop", "insert"} and
            result is None
        ):
            self.connect.commit()

        if command in {"drop", "create"}:
            self.initialize_tables()

        # Check result
        if result is None:
            cursor.close()
            return None

        ResultFetch(result)

        result_func: Callable = getattr(cursor, result.value)

        if result.value == "fetchmany":
            return result_func(size=size)

        data = result_func()

        cursor.close()
        if command == "insert":
            self.connect.commit()

        return data

    def close(self) -> None:
        """Close connection."""
        self.connect.close()

    def __enter__(self) -> DB:
        """Create context manager."""
        return self

    def __exit__(self, *args, **kwargs: Any) -> None:
        """Close connection."""
        self.close()

    if TYPE_CHECKING:
        def __getattr__(self, name: str) -> Table:
            """Typing for runtime created table."""
            ...

    def create_registred_table(self) -> None:
        """Create registred table."""
        cls = self.__class__
        cursor = self.connect.cursor()

        for path, tables in self._registered_tables_data.items():
            for table_info in tables:
                if path is None or path == self.path:

                    table_data, row_cls = table_info
                    table_name: str = table_data["table_name"]

                    create_table_query = self.create_table.query(**table_data)
                    logging.info("Query: %s", create_table_query)
                    cursor.execute(create_table_query)

                    table_obj = Table(
                        table_name,
                        use_datacls=True,
                        row_cls=row_cls,
                    )
                    setattr(cls, table_name.lower(), table_obj)

        self.connect.commit()

    @classmethod
    @dataclass_transform(kw_only_default=True)
    def register_table(
        cls,
        model_cls: type[_T] = None,
        *,
        path: str | Path | None = None,
    ) -> type[_T] | Callable[[type[_T]], type[_T]]:
        """Registrate table.

        Args:
            path (str | Path | None): path to datebase file
        """
        def wrap(model_cls: TModelClass) -> TModelClass:
            """Wrap func."""
            table_data_row_cls = create_table_and_data_cls_row(model_cls)
            correct_path = cls.normalize_path(Path(path))
            cls._registered_tables_data[correct_path].append(
                table_data_row_cls,
            )
            cls.orm_classes[table_data_row_cls[0]["table_name"]] = model_cls
            return table_data_row_cls[1]

        if model_cls is None and path is not None:
            return wrap

        table_data_row_cls = create_table_and_data_cls_row(model_cls)
        cls._registered_tables_data[path].append(table_data_row_cls)
        cls.orm_classes[table_data_row_cls[0]["table_name"]] = model_cls
        return table_data_row_cls[1]

    def add(self, *orm_objs: Any) -> list[int | None]:
        """Add new ORM object in db."""
        if hasattr(orm_objs[0], "orm_obj") is False:
            msg = "Unknown object type"
            raise TypeError(msg)

        items_by_table: defaultdict[Table, list] = defaultdict(list)

        for obj in orm_objs:
            table_name: str = obj.__table_name__
            table_obj: Table = getattr(self, table_name)

            items_by_table[table_obj].append(obj)

        object_ids = []
        for table, objects in items_by_table.items():
            object_ids.append(table.add(*objects))

        return object_ids


class Future:
    """Future for managing query execution."""

    def __init__(self) -> None:
        """Initialize."""
        self.event = Event()
        self.exception: Exception | None = None
        self.result: list[Any] | None = None

    def wait(self) -> None:
        """Wait for execution."""
        self.event.wait()

    @singledispatchmethod
    def put(self, result: list[Any] | None) -> None:
        """Write operation result."""
        self.result = result
        self.event.set()

    @put.register(Exception)
    def _(self, result: Exception) -> None:
        """Write exception."""
        self.exception = result
        self.event.set()

    def done(self) -> bool:
        """Check operation complited."""
        return self.event.is_set()


class ThreadDB(DB):
    """Thread safety db cls."""

    def __init__(
        self,
        path: str,
        *,
        use_datacls: bool = False,
        debug: bool = False,
        **connect_params: Any,
    ) -> None:
        """Initialize DB create connection, cursor and worker thread."""
        connect_params["check_same_thread"] = False
        self.worker_event = Event()
        self.worker_queue = Queue()
        self.worker = Thread(
            target=self.execute_worker,
            daemon=True,
        )
        self.worker.start()
        super().__init__(
            path,
            use_datacls=use_datacls,
            debug=debug,
            **connect_params,
        )
        if debug:
            logging.basicConfig(level=logging.DEBUG)

    def execute_worker(self) -> None:
        """Worker for executing sql-query and return result."""
        while not self.worker_event.is_set():
            try:
                future, args, kwargs = self.worker_queue.get()
                if kwargs.get("finish_worker"):
                    self.worker_event.set()
                    continue
                result = super().execute(*args, **kwargs)
                future.put(result)
            except Exception as e:  # noqa: PERF203
                logging.exception(
                    "Error: %s, Arguments: %s, %s",
                    e,
                    args,
                    kwargs,
                )
                future.put(e)
                self.connect.rollback()
            finally:
                future.event.set()
                self.worker_queue.task_done()

    def execute(self, *args: Any, **kwargs: Any) -> list[Any] | None:
        """Create future obj and sending args in worker."""
        future = Future()
        self.worker_queue.put((future, args, kwargs))
        future.wait()
        if future.done():
            return future.result
        return None

    def close(self) -> None:
        """Close worker thread and close db connection."""
        self.execute(finish_worker=True)
        self.worker.join()
        super().close()


if __name__ == "__main__":
    db = DB("local")
