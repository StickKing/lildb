"""Module contain orm model tests."""
from __future__ import annotations

from datetime import date
from itertools import chain
from typing import Optional
from typing import get_args
from typing import get_type_hints

from lildb.column_types import Integer
from lildb.orm import MColumn
from lildb.orm import Relation
from lildb.orm import RelationForeignKey
from lildb.orm import TColumn
from lildb.orm.model import create_table_and_data_cls_row
from lildb.table import Column


class Model1:
    """Class for testing."""

    id: TColumn[int] = MColumn(Integer(primary_key=True))
    title: TColumn[str]

    fg1 = RelationForeignKey("test_id", "Test", "id")
    fg2 = RelationForeignKey("test_id", "Test", "id")


class DateMinix:
    """Class for testing."""

    dt: TColumn[date]


class Model2(Model1, DateMinix):
    """Class for testing."""

    lead: TColumn[str]
    ref = Relation("", "", "")


class Model3:
    """Class for testing."""

    dt: TColumn[date]
    flt: TColumn[Optional[float]]


class TestCreateRowCls:
    """Tests for create row cls and table data."""

    def test_table_data(self) -> None:
        """Table data test"""
        table_data, _ = create_table_and_data_cls_row(Model1)
        model_annotation = get_type_hints(Model1)

        assert Model1.__name__ == table_data["table_name"]

        assert len(table_data["columns"]) == 2
        assert (
            sorted(model_annotation.keys()) ==
            sorted(table_data["columns"].keys())
        )

        assert len(table_data["foreign_keys"]) == 2

        table_data, _ = create_table_and_data_cls_row(Model2)
        model_annotation2 = get_type_hints(Model2)
        model_annotation2.update(model_annotation)

        assert Model2.__name__ == table_data["table_name"]

        assert len(table_data["columns"]) == 4
        assert (
            sorted(model_annotation2.keys()) ==
            sorted(table_data["columns"].keys())
        )

        assert len(table_data["foreign_keys"]) == 2

        # Optional
        table_data, _ = create_table_and_data_cls_row(Model3)
        model_annotation = get_type_hints(Model3)

        assert Model3.__name__ == table_data["table_name"]

        assert len(table_data["columns"]) == 2
        assert (
            sorted(model_annotation.keys()) ==
            sorted(table_data["columns"].keys())
        )

    def test_model_cls(self) -> None:
        """Test model cls."""
        _, model = create_table_and_data_cls_row(Model1)
        model_annotation = get_type_hints(Model1)

        for col_name in model_annotation:
            assert isinstance(getattr(model, col_name), Column)

        cls_dict = vars(Model1)
        for col_name in model_annotation:
            assert isinstance(cls_dict[col_name], MColumn)

        assert model.__table_name__ == Model1.__name__.lower()
        assert model.__relation_fields__ == ("fg1", "fg2")
        assert model.__column_fields__ == ("id", "title")

        _, model = create_table_and_data_cls_row(Model2)
        model_annotation = chain(
            get_type_hints(Model2),
            get_type_hints(Model1),
            get_type_hints(DateMinix),
        )

        for col_name in model_annotation:
            assert isinstance(getattr(model, col_name), Column)

        cls_dict = dict(vars(Model2))
        cls_dict.update(vars(Model1))
        cls_dict.update(vars(DateMinix))

        for col_name in model_annotation:
            assert isinstance(cls_dict[col_name], MColumn)

        assert model.__table_name__ == Model2.__name__.lower()
        assert model.__relation_fields__ == ("ref", "fg1", "fg2")
        assert model.__column_fields__ == ("dt", "id", "title", "lead")

    def test_model_instance(self) -> None:
        """Test model instance."""
        _, model = create_table_and_data_cls_row(Model1)
        model_annotation = get_type_hints(Model1)

        instance = model(id=10, title="hello")

        for col_name, generic_type in model_annotation.items():
            type_, = get_args(generic_type)
            assert isinstance(getattr(instance, col_name), type_)

        _, model = create_table_and_data_cls_row(Model2)
        model_annotation = get_type_hints(Model2)
        model_annotation.update(get_type_hints(Model1))
        model_annotation.update(get_type_hints(DateMinix))

        instance = model(id=10, title="hello", lead="world", dt=date.today())

        for col_name, generic_type in model_annotation.items():
            type_, = get_args(generic_type)
            assert isinstance(getattr(instance, col_name), type_)
