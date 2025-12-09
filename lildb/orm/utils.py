"""Module contains any funcs."""
from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import TypeVar


if TYPE_CHECKING:
    from ..table import Table
    from .model import _RowORMModelMixin
    TModelClass = TypeVar("TModelClass", bound=type[Any])


def contain_relation_objects(orm_object: _RowORMModelMixin) -> bool:
    """Check object contain new relation objects."""
    return len(orm_object._relation_object_add_funcs) > 0


def refresh_old_obj_by_new(
    table: Table,
    old_orm_object: _RowORMModelMixin,
    new_orm_object: _RowORMModelMixin,
) -> None:
    """Move relation funcs to other object."""
    for name in table.column_names:
        col_name = f"_column_data_{name}_"
        setattr(
            old_orm_object,
            col_name,
            getattr(new_orm_object, col_name)
        )


def process_add_relation_objects(
    orm_object: _RowORMModelMixin,
    ref_type: Literal["Relation", "RelationForeignKey"] | None = None,
) -> None:
    """Add relation object."""
    if ref_type is None:
        for funcs in orm_object._relation_object_add_funcs.values():
            for func in funcs:
                func()
        orm_object._relation_object_add_funcs = defaultdict(list)
        return

    for func in orm_object._relation_object_add_funcs[ref_type]:
        func()

    orm_object._relation_object_add_funcs.pop(ref_type)
