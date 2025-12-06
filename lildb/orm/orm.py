"""Module contain orm classes."""
from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import Literal
from typing import Sequence
from typing import TypeVar
from typing import overload

from lildb.rows import ABCRow

from ..column_types import Date
from ..column_types import DateTime
from ..column_types import ForeignKey
from ..column_types import Json
from ..column_types import TColumnType
from ..column_types import Time
from ..table.column import Column


if TYPE_CHECKING:
    from ..db import DB
    from ..rows import ABCRow
    from ..table.table import Table


TAnyType = TypeVar("TAnyType")


class TColumn(Generic[TAnyType]):
    """Typed Column."""

    __slots__ = ()

    @overload
    def __get__(
        self,
        instance: None,
        owner: Any,
    ) -> Column:
        ...

    @overload
    def __get__(
        self,
        instance: object,
        owner: Any,
    ) -> TAnyType:
        ...


class DBTableGetterMixin:
    """Mixin to get db or table from current row instance."""

    __slots__ = ()

    def _get_db(self, instance: ABCRow) -> DB:
        """Get instance db or None."""
        if instance.table:
            return instance.table.db

        msg = f"{instance} is not linked to the database"
        raise AssertionError(msg)

    def _get_table(self, db: DB) -> Table:
        table: Table | None = getattr(db, self.second_table.lower(), None)
        if table:
            return table
        msg = f"{self.second_table} not found"
        raise AssertionError(msg)

    def _get_table_by_name(self, db: DB, table_name: str) -> Table:
        """Get table by table name."""
        table: Table | None = getattr(db, table_name.lower(), None)
        if table:
            return table
        msg = f"{table_name} not found"
        raise AssertionError(msg)


class MColumn(TColumn[TAnyType]):
    """Model column discriptor."""

    __slots__ = (
        "column_type",
        "_column_name",
        "_column",
        "_cache",
        "_is_cached",
    )

    _cached_types = {
        Json,
        DateTime,
        Date,
        Time,
    }

    def __init__(self, column_type: TColumnType) -> None:
        self.column_type = column_type
        self._is_cached = column_type.__class__ in self._cached_types
        self._cache = None

    def __set_name__(self, owner: type[Any], column_name: str) -> None:
        self._column_name = f"_column_data_{column_name}_"
        table_name = owner.__name__
        self._column = Column(table_name, column_name)

    def __get__(
        self,
        instance: ABCRow | None,
        owner: type[Any],
    ) -> TAnyType | Column:
        if instance is None:
            return self._column

        # check cache
        if self._is_cached and self._cache:
            return self._cache

        value = getattr(instance, self._column_name)
        data = self.column_type.to_python(value)

        if self._is_cached is False:
            return data

        self._cache = data

        return data

    def __set__(self, instance: ABCRow, value: Any) -> None:
        """Set value."""
        if self._is_cached:
            self._cache = None
        setattr(
            instance,
            self._column_name,
            self.column_type.to_db(value),
        )


class RelationForeignKey(ForeignKey, TColumn, DBTableGetterMixin):
    """Relation foreign key."""

    __slots__ = ()

    def _add_new_foreign_object(
        self,
        instance: Any,
        new_foreign_obj: ABCRow,
    ) -> None:
        """Add new foreing object"""
        db = self._get_db(instance)
        table = self._get_table_by_name(db, self.second_table)
        new_obj = table.add(new_foreign_obj, returning=True)
        self.__set__(instance, new_obj)

    def __get__(
        self,
        instance: ABCRow | None,
        owner: type[Any],
    ) -> TAnyType:
        """Returns a relation object by foreign key."""
        if instance is None:
            msg = "Instance not found"
            raise AttributeError(msg)

        db = self._get_db(instance)
        table = self._get_table_by_name(db, self.second_table)

        ref_column = getattr(table.c, self.reference_column.lower(), None)

        if ref_column is None:
            msg = f"Column '{self.reference_column}' not found"
            raise AttributeError(msg)

        foreign_key_value = getattr(instance, self.column, None)

        if foreign_key_value is None:
            return None

        return table.query().where(ref_column == foreign_key_value).first()

    def __set__(self, instance: ABCRow, value: ABCRow | None) -> None:
        """Set value."""
        if value is None:
            setattr(instance, self.column, value)
            return

        if hasattr(value, "orm_obj") is False:
            msg = "Incorrect object"
            raise TypeError(msg)

        if value.table is None:
            getattr(
                instance,
                "_relation_object_add_funcs",
            )["RelationForeignKey"].append(
                lambda: self._add_new_foreign_object(instance, value),
            )

        ref_table_value = getattr(value, self.reference_column)

        setattr(instance, self.column, ref_table_value)


class Relation(TColumn, DBTableGetterMixin):
    """
    Relation is a descriptor for dealing
    with one-to-many or many-to-many relationships.
    """

    __slots__ = (
        "second_table",
        "_foreign_key_to_current_table",
        "_foreign_key_to_relation_table",
        "_cascade",
    )

    def __init__(
        self,
        second_table: str,
        foreign_key_to_current_table: str,
        foreign_key_to_relation_table: str | None = None,
        cascade: Literal["delete", "update"] = "delete",
    ) -> None:
        """."""
        self.second_table = second_table
        self._foreign_key_to_current_table = foreign_key_to_current_table
        self._foreign_key_to_relation_table = foreign_key_to_relation_table
        self._cascade = cascade

    def _one_many(self, instance: ABCRow) -> list[ABCRow]:
        """One many."""
        db = self._get_db(instance)
        second_table = self._get_table(db)
        second_orm_model = db.orm_classes[self.second_table]

        second_tb_foreign_key: RelationForeignKey = second_orm_model.__dict__[
            self._foreign_key_to_current_table
        ]

        foreign_key_column: Column = getattr(
            second_orm_model,
            second_tb_foreign_key.column,
        )

        instance_ref_column_value = getattr(
            instance,
            second_tb_foreign_key.reference_column,
        )

        return second_table.query().where(
            foreign_key_column == instance_ref_column_value,
        ).all()

    def _many_to_many(self, instance: ABCRow) -> list[ABCRow]:
        """Many to many."""
        db = self._get_db(instance)

        m2m_table = self._get_table(db)
        m2m_orm_model = db.orm_classes[self.second_table]

        current_tb_foreign_key: RelationForeignKey = m2m_orm_model.__dict__[
            self._foreign_key_to_current_table
        ]
        relation_tb_foreign_key: RelationForeignKey = m2m_orm_model.__dict__[
            self._foreign_key_to_relation_table
        ]

        relation_table = self._get_table_by_name(
            db,
            relation_tb_foreign_key.second_table,
        )
        rel_orm_model = db.orm_classes[relation_tb_foreign_key.second_table]

        instance_foreing_key_value = getattr(
            instance,
            current_tb_foreign_key.reference_column,
        )

        rel_column: Column = getattr(
            m2m_orm_model,
            relation_tb_foreign_key.column,
        )
        cur_column: Column = getattr(
            m2m_orm_model,
            current_tb_foreign_key.column,
        )

        # TODO(stickking): join??
        # 0000
        query_m2m = m2m_table.query(
            rel_column,
        ).where(
            cur_column == instance_foreing_key_value,
        ).all(
            only_data=True,
        )

        relation_ids = [id_[0] for id_ in query_m2m]

        if not relation_ids:
            return []

        rel_column: Column = getattr(
            rel_orm_model,
            relation_tb_foreign_key.reference_column,
        )

        return relation_table.query().where(rel_column.in_(relation_ids)).all()

    def _prepare_relation_objects(
        self,
        relation_table: Table,
        objects: list[ABCRow],
    ) -> list[ABCRow]:
        """Prepare relation objects."""
        exists_object = [obj for obj in objects if obj.table is not None]
        new_objects = [obj for obj in objects if obj.table is None]

        added_objects = [
            relation_table.add(new_obj, returning=True)
            for new_obj in new_objects
        ]

        return [*exists_object, *added_objects]

    def _add_new_one_many_object(
        self,
        instance: Any,
        objects: list[ABCRow],
    ) -> None:
        """Add new relation objects."""
        db = self._get_db(instance)
        table = self._get_table_by_name(db, self.second_table)
        second_orm_model = db.orm_classes[self.second_table]
        second_tb_foreign_key: RelationForeignKey = second_orm_model.__dict__[
            self._foreign_key_to_current_table
        ]

        ready_objects = self._prepare_relation_objects(table, objects)

        ref_value = getattr(instance, second_tb_foreign_key.reference_column)

        id_values = [
            {name: getattr(obj, name) for name in table.primary_keys}
            for obj in ready_objects
        ]

        condition = " OR ".join(
            table.update._make_operator_query(pr_key, without_parameters=True)
            for pr_key in id_values
        )

        if self._cascade == "update":
            table.update(
                {second_tb_foreign_key.column: None},
                **{second_tb_foreign_key.column: ref_value},
            )
        else:
            table.delete(**{second_tb_foreign_key.column: ref_value})

        table.update(
            {second_tb_foreign_key.column: ref_value},
            condition=condition,
        )

    def _add_new_many_to_many_object(
        self,
        instance: Any,
        objects: ABCRow,
    ) -> None:
        """Add new relation objects."""
        db = self._get_db(instance)

        m2m_table = self._get_table_by_name(db, self.second_table)
        m2m_orm_model = db.orm_classes[self.second_table]

        current_tb_foreign_key: RelationForeignKey = m2m_orm_model.__dict__[
            self._foreign_key_to_current_table
        ]
        relation_tb_foreign_key: RelationForeignKey = m2m_orm_model.__dict__[
            self._foreign_key_to_relation_table
        ]
        relation_table = self._get_table_by_name(
            db,
            relation_tb_foreign_key.second_table,
        )

        ready_objects = self._prepare_relation_objects(relation_table, objects)

        instance_ref_column_value = getattr(
            instance,
            current_tb_foreign_key.reference_column,
        )

        m2m_current_fk_column = current_tb_foreign_key.column
        m2m_relation_fk_column = relation_tb_foreign_key.column

        relation_ref_col_name = relation_tb_foreign_key.reference_column

        m2m_table.delete(**{
            m2m_current_fk_column: instance_ref_column_value,
        })

        m2m_table.add(*[
            {
                m2m_current_fk_column: instance_ref_column_value,
                m2m_relation_fk_column: getattr(obj, relation_ref_col_name)
            }
            for obj in ready_objects
        ])

    def __get__(
        self,
        instance: ABCRow | None,
        owner: type[Any],
    ) -> list[TAnyType]:
        """Returns a relation objects by foreign keys."""
        if instance is None:
            msg = "Instance not found"
            raise AttributeError(msg)

        if self._foreign_key_to_relation_table:
            return self._many_to_many(instance)

        return self._one_many(instance)

    def __set__(
        self,
        instance: ABCRow,
        value: Sequence[ABCRow],
    ) -> None:
        """Set new relation objects."""
        if self._foreign_key_to_relation_table:
            getattr(
                instance,
                "_relation_object_add_funcs",
            )["Relation"].append(
                lambda: self._add_new_many_to_many_object(instance, value),
            )
            return

        getattr(
            instance,
            "_relation_object_add_funcs",
        )["Relation"].append(
            lambda: self._add_new_one_many_object(instance, value),
        )
