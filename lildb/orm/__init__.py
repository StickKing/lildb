"""Module contain all relation with orm tables."""
from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from .utils import TModelClass

from .model import create_table_and_data_cls_row
from .orm import MColumn
from .orm import Relation
from .orm import RelationForeignKey
from .orm import TColumn


__all__ = (
    "RelationForeignKey",
    "Relation",
    "TColumn",
    "create_table_and_data_cls_row",
    "TModelClass",
    "MColumn",
)
