"""Module contain all relation with orm tables."""
from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from .utils import TModelClass

from .orm import MColumn
from .orm import Relation
from .orm import RelationForeignKey
from .orm import TColumn


__all__ = (
    "RelationForeignKey",
    "Relation",
    "TColumn",
    "TModelClass",
    "MColumn",
)
