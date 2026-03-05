"""Module contain orm typings."""
from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar
from typing import Protocol


if TYPE_CHECKING:
    from collections import defaultdict

    from ..table import Table


class TModel(Protocol):
    """Model protocol."""

    __table_name__: ClassVar[str]
    __relation_fields__: ClassVar[tuple[str, ...]]
    __column_fields__: ClassVar[tuple[str, ...]]

    table: Table
    changed_columns: set[str]
    _relation_events: defaultdict[str, list]
    not_changed_column_values: dict[str, Any]
    changed_column_values: dict[str, Any]

    def get_row_data_as_dict(self) -> dict:
        """Return row data like dict."""
        ...

    def change(self) -> None:
        """Update this row."""
        ...
