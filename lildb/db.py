"""Module contain DB component."""
from __future__ import annotations

import logging
import sqlite3
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
from typing import Final
from typing import Iterator
from typing import MutableMapping
from typing import Sequence

from .enumcls import ResultFetch
from .operations import CreateTable
from .table import Table


__all__ = (
    "DB",
    "ThreadDB",
)


class DB:
    """DB component."""

    custom_tables: ClassVar[list[Table]]

    _instances: Final[dict[str, DB]] = {}

    def __new__(cls: type[DB], *args: Any, **kwargs: Any) -> DB:
        """Use singleton template. Check path and match paths."""
        if not args and kwargs.get("path") is None:
            msg = "DB.__init__() missing 1 required argument: 'path'"
            raise TypeError(msg)

        path = kwargs["path"] if kwargs.get("path") else args[0]
        normalized_path = cls.normalize_path(Path(path))

        for inst_path, instance in cls._instances.items():
            if cls.normalize_path(Path(inst_path)) == normalized_path:
                return instance

        new_instance = super().__new__(cls)
        cls._instances[path] = new_instance
        return cls._instances[path]

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
        **connect_params: Any,
    ) -> None:
        """Initialize DB create connection and cursor."""
        if debug:
            logging.basicConfig(level=logging.DEBUG)
        self.path = path
        self.connect: sqlite3.Connection = sqlite3.connect(
            path,
            **connect_params,
        )
        self.use_datacls = use_datacls
        self.table_names: set = set()
        self.initialize_tables()

        self.create_table = CreateTable(self)

    def initialize_tables(self) -> None:
        """Initialize all db tables."""
        stmt = "SELECT name FROM sqlite_master WHERE type='table';"
        result = self.execute(stmt, result=ResultFetch.fetchall)

        custom_table_names = set()

        for attr in filter(
            lambda i: not i.startswith("_"),
            dir(self.__class__),
        ):
            custom_table = getattr(self, attr)
            if not isinstance(custom_table, Table):
                continue
            custom_table_names.add(custom_table.name.lower())
            custom_table(self)

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
        if many:
            cursor.executemany(query, parameters)
        else:
            cursor.execute(query, parameters)

        if command in {"insert", "delete", "update", "create", "drop"}:
            self.connect.commit()

        # Check result
        if result is None:
            return None

        ResultFetch(result)

        result_func: Callable = getattr(cursor, result.value)

        if result.value == "fetchmany":
            return result_func(size=size)
        return result_func()

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

