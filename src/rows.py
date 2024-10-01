"""Module contains row classes."""
from __future__ import annotations

from ast import arg
from typing import TYPE_CHECKING
from typing import Any


if TYPE_CHECKING:
    from .table import Table


class _RowOperationMixin:

    def delete(self) -> None:
        """Delete this row from db."""
        id = self.get("id")
        if id is None:
            return
        self.table.delete(id)


class RowDict(dict, _RowOperationMixin):
    """DB row like a dict."""

    def __init__(self, table: Table, **kwargs: dict) -> None:
        """Initialize dict row."""
        self.table = table
        if self.table is None:
            msg = "missing 1 required named argument: 'table'"
            raise TypeError(msg)
        super().__init__(kwargs)

