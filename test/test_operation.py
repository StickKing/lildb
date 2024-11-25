"""Module contain test for select query."""
from __future__ import annotations

import uuid
from random import randint

import pytest

from lildb import DB


@pytest.fixture(scope="package")
def dbs() -> tuple[DB, ...]:
    """Create db objects."""
    db_dict = DB("test.db")
    DB._instances.pop("test.db")
    db_dict.drop_tables()
    db_dict.create_table("ttable", ["id", "name", "post", "salary"])
    db_cls = DB("test.db", use_datacls=True)
    return (db_dict, db_cls)


class TestInsert:
    """Test for insert obj."""

    def test_one(self, dbs: tuple[DB, ...]) -> None:
        """Test for add one row."""
        db_dict, _ = dbs
        new_row = {
            "id": 1,
            "name": str(uuid.uuid4()),
            "post": str(uuid.uuid4()),
            "salary": randint(1, 10000),
        }
        cursor = db_dict.connect.cursor()
        db_dict.ttable.add(new_row)

        stmt = "select * from ttable where id = ?"
        row = cursor.execute(stmt, (new_row["id"],)).fetchone()

        assert all(
            value1 == value2
            for value1, value2 in zip(new_row.values(), row)
        )

    def test_many(self, dbs: tuple[DB, ...]) -> None:
        """Test for adding many rows."""
        db_dict, _ = dbs
        new_rows = [
            {
                "id": id_,
                "name": str(uuid.uuid4()),
                "post": str(uuid.uuid4()),
                "salary": randint(1, 10000),
            }
            for id_ in range(2, 11)
        ]
        cursor = db_dict.connect.cursor()
        db_dict.ttable.add(new_rows)

        stmt = "select * from ttable where id > 1"
        rows = cursor.execute(stmt).fetchmany()

        for new_row, db_row in zip(new_rows, rows):
            assert all(
                value1 == value2
                for value1, value2 in zip(new_row.values(), db_row)
            )


class TestSelect:
    """Test for select obj."""

    MAX_COUNT = 10

    def test_all(self, dbs: tuple[DB, ...]) -> None:
        """Test for geting all data."""
        db_dict, db_cls = dbs

        len_dict = len(db_dict.ttable.all())
        len_cls = len(db_cls.ttable.all())

        assert len_dict == len_cls == self.MAX_COUNT

    def test_one(self, dbs: tuple[DB, ...]) -> None:
        """Test for geting one row."""
        db_dict, db_cls = dbs

        # assert db_dict.ttable.get(id=randint(1, 10)) is not None
        # assert db_cls.ttable.get(id=randint(1, 10)) is not None
        assert db_dict.ttable[randint(1, 10)] is not None
        assert db_dict.ttable[randint(1, 10)] is not None

    def test_column(self, dbs: tuple[DB, ...]) -> None:
        """Test for geting one row."""
        db_dict, db_cls = dbs

        column_names = {"name", "salary"}
        # table attr in ResultRow too
        column_names_with_table = {*column_names, "table"}

        for row1, row2 in zip(
            db_dict.ttable.select(columns=column_names),
            db_cls.ttable.select(columns=column_names),
        ):
            assert len(row1.__dict__) == len(column_names_with_table)
            assert all(
                name in column_names_with_table
                for name in row1.__dict__
            )
            assert len(row2.__dict__) == len(column_names_with_table)
            assert all(
                name in column_names_with_table
                for name in row2.__dict__
            )

    def test_condition(self, dbs: tuple[DB, ...]) -> None:
        """Test for geting one row."""
        db_dict, db_cls = dbs

        assert len(db_dict.ttable.select(condition="id < 6")) == 5
        assert len(db_cls.ttable.select(condition="id < 6")) == 5


class TestUpdate:
    """Test for update obj."""

    def test_one(self, dbs: tuple[DB, ...]) -> None:
        """Test for update one row."""
        db_dict, db_cls = dbs

        row = db_dict.ttable.get(id=6)
        row["name"] = "TestValue"
        row["salary"] = 3000
        row.change()

        # regeting row from db
        row = db_dict.ttable.get(id=6)
        assert row["name"] == "TestValue"
        assert row["salary"] == 3000

        row = db_cls.ttable.get(id=7)
        row.name = "TestValue2"
        row.salary = 3002
        row.change()

        # regeting row from db
        row = db_cls.ttable.get(id=7)
        assert row.name == "TestValue2"
        assert row.salary == 3002

        db_dict.ttable.update(
            {"salary": 11, "name": "TEST"},
            id=8,
        )
        row = db_dict.ttable.get(id=8)
        assert row["name"] == "TEST" and row["salary"] == 11

    def test_all(self, dbs: tuple[DB, ...]) -> None:
        """Test for delete one row."""
        db_dict, db_cls = dbs

        db_dict.ttable.update({"salary": 10})

        assert all(
            result.salary == 10
            for result in db_dict.ttable.select(columns=["salary"])
        )

    def test_condition(self, dbs: tuple[DB, ...]) -> None:
        """Test for update rows with condition."""
        db_dict, db_cls = dbs

        db_cls.ttable.update({"post": "hello"}, condition="id = 10 or id = 9")

        assert db_cls.ttable.get(id=10).post == "hello"
        assert db_cls.ttable.get(id=9).post == "hello"


class TestDelete:
    """Test for delete obj."""

    def test_one(self, dbs: tuple[DB, ...]) -> None:
        """Test for delete one row."""
        db_dict, db_cls = dbs

        db_dict.ttable.delete(id=1)
        assert db_dict.ttable.get(id=1) is None

        row = db_dict.ttable[2]
        row.delete()
        assert db_dict.ttable.get(id=2) is None

        row = db_cls.ttable[3]
        row.delete()
        assert db_dict.ttable.get(id=3) is None

    def test_many(self, dbs: tuple[DB, ...]) -> None:
        """Test for deleting many rows."""
        db_dict, db_cls = dbs

        db_dict.ttable.delete([4, 5])
        assert (
            db_dict.ttable.get(id=4) is None and
            db_dict.ttable.get(id=5) is None
        )

    def test_condition(self, dbs: tuple[DB, ...]) -> None:
        """Test for update rows with condition."""
        db_dict, db_cls = dbs

        db_cls.ttable.delete(condition="id > 8 or name = 'TEST'")
        assert db_cls.ttable.get(name="TEST") is None
        assert db_cls.ttable.get(id=9) is None
        assert db_cls.ttable.get(id=10) is None
