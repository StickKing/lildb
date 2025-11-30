"""Module contain all relation with orm tables."""
from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from .utils import TModelClass

from .orm import MColumn
from .orm import Relation
from .orm import RelationForeignKey
from .orm import TColumn
from .utils import create_table_and_data_cls_row


__all__ = (
    "RelationForeignKey",
    "Relation",
    "TColumn",
    "create_table_and_data_cls_row",
    "TModelClass",
    "MColumn",
)
