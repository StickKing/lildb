"""Module contain test for select query."""
from __future__ import annotations

import uuid
from random import randint
from typing import Any
from typing import MutableMapping
from typing import Sequence

import pytest

from lildb import DB
from lildb.column_types import Blob
from lildb.column_types import ForeignKey
from lildb.column_types import Integer
from lildb.column_types import Real
from lildb.column_types import Text
from lildb.operations import Query


@pytest.fixture(scope="package")
def dbs() -> tuple[DB, ...]:
    """Create db objects."""
    db_dict = DB("test.db")
    DB._instances.pop("test.db")
    db_dict.drop_tables()
    db_dict.create_table("ttable", ["id", "name", "post", "salary"])
    db_cls = DB("test.db", use_datacls=True)
    return (db_dict, db_cls)


class TestCreateTable:
    """Test Create table operations."""

    def prepare_query(
        self,
        db: DB,
        table_name: str,
        columns: Sequence[str] | MutableMapping[str, Any],
        table_primary_key: Sequence[str] | None = None,
        foreign_keys: Sequence[ForeignKey] | None = None,
        *,
        if_not_exists: bool = True,
    ) -> str:
        columns = db.create_table._generate_columns(
            columns,
        )
        table_primary_keys = db.create_table._genarate_table_primary_keys(
            table_primary_key,
        )
        foreign_keys = db.create_table._genatate_table_foreign_keys(
            foreign_keys
        )
        query = db.create_table.query(
            table_name,
            columns,
            table_primary_keys,
            foreign_keys,
            if_not_exists=if_not_exists,
        )
        return query.replace("  ", " ")

    def test_simple(self, dbs: tuple[DB, ...]) -> None:
        """
        Simple creating table test without types,
        table primary keys and foreign keys.
        """
        db, _ = dbs
        query = self.prepare_query(db, "Person", ["name"])
        assert query == "CREATE TABLE IF NOT EXISTS `Person` (name)"

        query = self.prepare_query(db, "Person", ["name", "post"])
        assert query == "CREATE TABLE IF NOT EXISTS `Person` (name, post)"

        query = self.prepare_query(db, "Person", ["name", "post", "salary"])
        assert (
            query == "CREATE TABLE IF NOT EXISTS `Person` (name, post, salary)"
        )

    def test_simple_primary(self, dbs: tuple[DB, ...]) -> None:
        """
        Simple creating table test without types,
        foreign keys but with table primary keys.
        """
        db, _ = dbs
        query = self.prepare_query(
            db,
            "Person",
            ["name"],
            table_primary_key=("name",),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` "
            "(name, PRIMARY KEY(name))"
        )

        query = self.prepare_query(
            db,
            "Person",
            ["id", "name"],
            table_primary_key=("name",),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` "
            "(id, name, PRIMARY KEY(name))"
        )

        query = self.prepare_query(
            db,
            "Person",
            ["id", "name"],
            table_primary_key=("name", "id"),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` "
            "(id, name, PRIMARY KEY(name,id))"
        )

    def test_simple_foreign(self, dbs: tuple[DB, ...]) -> None:
        """
        Simple creating table test without types,
        foreign keys, table primary keys but with foreign keys.
        """
        db, _ = dbs
        query = self.prepare_query(
            db,
            "Person",
            ["name"],
            foreign_keys=(ForeignKey("name", "Person", "name"),),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` "
            "(name, FOREIGN KEY(`name`) REFERENCES `Person`(`name`))"
        )

        query = self.prepare_query(
            db,
            "Person",
            ["name", "id"],
            foreign_keys=(ForeignKey("name", "Person", "name"),),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` "
            "(name, id, FOREIGN KEY(`name`) REFERENCES `Person`(`name`))"
        )

        query = self.prepare_query(
            db,
            "Person",
            ["post_id", "name", "id", "any_id"],
            foreign_keys=(
                ForeignKey("any_id", "AnyTable", "id"),
                ForeignKey("post_id", "Post", "id"),
            ),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` "
            "(post_id, name, id, any_id, FOREIGN KEY(`any_id`) "
            "REFERENCES `AnyTable`(`id`), FOREIGN KEY(`post_id`) "
            "REFERENCES `Post`(`id`))"
        )

    def test_simple_full(self, dbs: tuple[DB, ...]) -> None:
        """
        Simple creating table test with foreign keys, table primary keys.
        """
        db, _ = dbs
        query = self.prepare_query(
            db,
            "Person",
            ["name", "any_id"],
            foreign_keys=(ForeignKey("any_id", "Any", "id"),),
            table_primary_key=("name",),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` "
            "(name, any_id, PRIMARY KEY(name), FOREIGN KEY(`any_id`) "
            "REFERENCES `Any`(`id`))"
        )

    def test_with_types(self, dbs: tuple[DB, ...]) -> None:
        """Create table with column types."""
        db, _ = dbs
        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(),
                "name": Text(),
                "salary": Real(),
                "img": Blob(),
            }
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER NOT NULL , "
            "`name` TEXT NOT NULL , "
            "`salary` REAL NOT NULL , "
            "`img` BLOB NOT NULL )"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(default=10),
                "name": Text(default='David'),
                "salary": Real(default=150.5),
                "img": Blob(),
            }
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER DEFAULT 10 NOT NULL , "
            "`name` TEXT DEFAULT 'David' NOT NULL , "
            "`salary` REAL DEFAULT 150.5 NOT NULL , "
            "`img` BLOB NOT NULL )"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(unique=True),
                "name": Text(unique=True),
                "salary": Real(unique=True),
                "img": Blob(unique=True),
            }
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER NOT NULL UNIQUE , "
            "`name` TEXT NOT NULL UNIQUE , "
            "`salary` REAL NOT NULL UNIQUE , "
            "`img` BLOB NOT NULL UNIQUE )"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(nullable=True),
                "name": Text(nullable=True),
                "salary": Real(nullable=True),
                "img": Blob(nullable=True),
            }
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER, "
            "`name` TEXT, "
            "`salary` REAL, "
            "`img` BLOB)"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(primary_key=True, autoincrement=True),
                "name": Text(nullable=True, unique=True, default='David'),
                "salary": Real(nullable=True, unique=True, default=100.5),
                "img": Blob(nullable=True, unique=True),
            }
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL , "
            "`name` TEXT DEFAULT 'David' UNIQUE , "
            "`salary` REAL DEFAULT 100.5 UNIQUE , "
            "`img` BLOB UNIQUE )"
        )

    def test_with_types_primary(self, dbs: tuple[DB, ...]) -> None:
        """Create table with column types and table primary key."""
        db, _ = dbs
        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(),
                "name": Text(),
                "salary": Real(),
                "img": Blob(),
            },
            table_primary_key=("id",),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER NOT NULL , "
            "`name` TEXT NOT NULL , "
            "`salary` REAL NOT NULL , "
            "`img` BLOB NOT NULL , "
            "PRIMARY KEY(id))"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(default=10),
                "name": Text(default='David'),
                "salary": Real(default=150.5),
                "img": Blob(),
            },
            table_primary_key=("id",),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER DEFAULT 10 NOT NULL , "
            "`name` TEXT DEFAULT 'David' NOT NULL , "
            "`salary` REAL DEFAULT 150.5 NOT NULL , "
            "`img` BLOB NOT NULL , "
            "PRIMARY KEY(id))"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(unique=True),
                "name": Text(unique=True),
                "salary": Real(unique=True),
                "img": Blob(unique=True),
            },
            table_primary_key=("id", "name"),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER NOT NULL UNIQUE , "
            "`name` TEXT NOT NULL UNIQUE , "
            "`salary` REAL NOT NULL UNIQUE , "
            "`img` BLOB NOT NULL UNIQUE , "
            "PRIMARY KEY(id,name))"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(nullable=True),
                "name": Text(nullable=True),
                "salary": Real(nullable=True),
                "img": Blob(nullable=True),
            },
            table_primary_key=("id", "name"),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER, "
            "`name` TEXT, "
            "`salary` REAL, "
            "`img` BLOB, "
            "PRIMARY KEY(id,name))"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(),
                "name": Text(nullable=True, unique=True, default='David'),
                "salary": Real(nullable=True, unique=True, default=100.5),
                "img": Blob(nullable=True, unique=True),
            },
            table_primary_key=("id", "name", "salary"),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER NOT NULL , "
            "`name` TEXT DEFAULT 'David' UNIQUE , "
            "`salary` REAL DEFAULT 100.5 UNIQUE , "
            "`img` BLOB UNIQUE , "
            "PRIMARY KEY(id,name,salary))"
        )

    def test_with_types_foreign(self, dbs: tuple[DB, ...]) -> None:
        """Create table with column types and table primary key."""
        db, _ = dbs
        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(),
                "name": Text(),
                "salary": Real(),
                "img": Blob(),
                "any_id": Integer(),
            },
            foreign_keys=(ForeignKey("any_id", "Any", "id"),),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER NOT NULL , "
            "`name` TEXT NOT NULL , "
            "`salary` REAL NOT NULL , "
            "`img` BLOB NOT NULL , "
            "`any_id` INTEGER NOT NULL , "
            "FOREIGN KEY(`any_id`) REFERENCES `Any`(`id`))"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(default=10),
                "name": Text(default='David'),
                "salary": Real(default=150.5),
                "img": Blob(),
                "any_id": Integer(),
            },
            foreign_keys=(ForeignKey("any_id", "Any", "id"),),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER DEFAULT 10 NOT NULL , "
            "`name` TEXT DEFAULT 'David' NOT NULL , "
            "`salary` REAL DEFAULT 150.5 NOT NULL , "
            "`img` BLOB NOT NULL , "
            "`any_id` INTEGER NOT NULL , "
            "FOREIGN KEY(`any_id`) REFERENCES `Any`(`id`))"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(unique=True),
                "name": Text(unique=True),
                "salary": Real(unique=True),
                "img": Blob(unique=True),
                "any_id": Integer(),
            },
            foreign_keys=(ForeignKey("any_id", "Any", "id"),),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER NOT NULL UNIQUE , "
            "`name` TEXT NOT NULL UNIQUE , "
            "`salary` REAL NOT NULL UNIQUE , "
            "`img` BLOB NOT NULL UNIQUE , "
            "`any_id` INTEGER NOT NULL , "
            "FOREIGN KEY(`any_id`) REFERENCES `Any`(`id`))"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(nullable=True),
                "name": Text(nullable=True),
                "salary": Real(nullable=True),
                "img": Blob(nullable=True),
                "any_id": Integer(),
            },
            foreign_keys=(ForeignKey("any_id", "Any", "id"),),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER, "
            "`name` TEXT, "
            "`salary` REAL, "
            "`img` BLOB, "
            "`any_id` INTEGER NOT NULL , "
            "FOREIGN KEY(`any_id`) REFERENCES `Any`(`id`))"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(primary_key=True, autoincrement=True),
                "name": Text(nullable=True, unique=True, default='David'),
                "salary": Real(nullable=True, unique=True, default=100.5),
                "img": Blob(nullable=True, unique=True),
                "any_id": Integer(),
            },
            foreign_keys=(ForeignKey("any_id", "Any", "id"),),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL , "
            "`name` TEXT DEFAULT 'David' UNIQUE , "
            "`salary` REAL DEFAULT 100.5 UNIQUE , "
            "`img` BLOB UNIQUE , "
            "`any_id` INTEGER NOT NULL , "
            "FOREIGN KEY(`any_id`) REFERENCES `Any`(`id`))"
        )

    def test_with_types_full(self, dbs: tuple[DB, ...]) -> None:
        """Create table with column types table primary key and foreign key."""
        db, _ = dbs
        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(),
                "name": Text(),
                "salary": Real(),
                "img": Blob(),
                "any_id": Integer(),
            },
            table_primary_key=("id",),
            foreign_keys=(ForeignKey("any_id", "Any", "id"),),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER NOT NULL , "
            "`name` TEXT NOT NULL , "
            "`salary` REAL NOT NULL , "
            "`img` BLOB NOT NULL , "
            "`any_id` INTEGER NOT NULL , "
            "PRIMARY KEY(id), "
            "FOREIGN KEY(`any_id`) REFERENCES `Any`(`id`))"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(default=10),
                "name": Text(default='David'),
                "salary": Real(default=150.5),
                "img": Blob(),
                "any_id": Integer(),
            },
            table_primary_key=("id",),
            foreign_keys=(ForeignKey("any_id", "Any", "id"),),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER DEFAULT 10 NOT NULL , "
            "`name` TEXT DEFAULT 'David' NOT NULL , "
            "`salary` REAL DEFAULT 150.5 NOT NULL , "
            "`img` BLOB NOT NULL , "
            "`any_id` INTEGER NOT NULL , "
            "PRIMARY KEY(id), "
            "FOREIGN KEY(`any_id`) REFERENCES `Any`(`id`))"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(unique=True),
                "name": Text(unique=True),
                "salary": Real(unique=True),
                "img": Blob(unique=True),
                "any_id": Integer(),
            },
            table_primary_key=("id", "name"),
            foreign_keys=(ForeignKey("any_id", "Any", "id"),),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER NOT NULL UNIQUE , "
            "`name` TEXT NOT NULL UNIQUE , "
            "`salary` REAL NOT NULL UNIQUE , "
            "`img` BLOB NOT NULL UNIQUE , "
            "`any_id` INTEGER NOT NULL , "
            "PRIMARY KEY(id,name), "
            "FOREIGN KEY(`any_id`) REFERENCES `Any`(`id`))"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(nullable=True),
                "name": Text(nullable=True),
                "salary": Real(nullable=True),
                "img": Blob(nullable=True),
                "any_id": Integer(),
            },
            table_primary_key=("id", "name"),
            foreign_keys=(ForeignKey("any_id", "Any", "id"),),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER, "
            "`name` TEXT, "
            "`salary` REAL, "
            "`img` BLOB, "
            "`any_id` INTEGER NOT NULL , "
            "PRIMARY KEY(id,name), "
            "FOREIGN KEY(`any_id`) REFERENCES `Any`(`id`))"
        )

        query = self.prepare_query(
            db,
            "Person",
            {
                "id": Integer(primary_key=True, autoincrement=True),
                "name": Text(nullable=True, unique=True, default='David'),
                "salary": Real(nullable=True, unique=True, default=100.5),
                "img": Blob(nullable=True, unique=True),
                "any_id": Integer(),
                "any_id2": Integer(),
            },
            table_primary_key=("id", "name", "salary"),
            foreign_keys=(
                ForeignKey("any_id", "Any", "id"),
                ForeignKey("any_id2", "Any", "id"),
            ),
        )
        assert query == (
            "CREATE TABLE IF NOT EXISTS `Person` ("
            "`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL , "
            "`name` TEXT DEFAULT 'David' UNIQUE , "
            "`salary` REAL DEFAULT 100.5 UNIQUE , "
            "`img` BLOB UNIQUE , "
            "`any_id` INTEGER NOT NULL , "
            "`any_id2` INTEGER NOT NULL , "
            "PRIMARY KEY(id,name,salary), "
            "FOREIGN KEY(`any_id`) REFERENCES `Any`(`id`), "
            "FOREIGN KEY(`any_id2`) REFERENCES `Any`(`id`))"
        )


class TestInsert:
    """Test for insert obj."""

    def test_one(self, dbs: tuple[DB, ...]) -> None:
        """Test for add one row."""
        db_dict, _ = dbs
        new_row = {
            "id": 1,
            "name": str(uuid.uuid4()),
            "post": None,
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
                "name": None,
                "post": str(uuid.uuid4()),
                "salary": randint(1, 10000),
            }
            for id_ in range(2, 21)
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

    MAX_COUNT = 20

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

        row = db_cls.ttable.get(id=7)
        row.name = None
        row.salary = None
        row.change()

        row = db_cls.ttable.get(id=7)
        assert row.name is None
        assert row.salary is None

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

    # def test_condition(self, dbs: tuple[DB, ...]) -> None:
    #     """Test for update rows with condition."""
    #     db_dict, db_cls = dbs

    #     db_cls.ttable.delete(condition="id > 8 or name = 'TEST'")
    #     assert db_cls.ttable.get(name="TEST") is None
    #     assert db_cls.ttable.get(id=9) is None
    #     assert db_cls.ttable.get(id=10) is None


class TestQuery:
    """Tests for query operation."""

    def test_where(self, dbs: tuple[DB, ...]) -> None:
        """Test for operation where."""
        db, _ = dbs
        # table = db_dict.ttable

        query = db.ttable.query()
        query.where(name=None)

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable WHERE name is NULL"
        )

        query = db.ttable.query()
        query.where(name=None, salary=10)
        print(query)

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable WHERE name is NULL AND salary = 10"
        )

        query = db.ttable.query()
        query.where(name=None, salary=10)
        query.where(condition="salary < 10", operator="OR")

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable WHERE name is NULL AND salary = 10 "
            "OR salary < 10"
        )

        query = db.ttable.query()
        query.where(condition="salary < 10")
        query.where(name=None, salary=10, filter_operator="OR")

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable WHERE salary < 10 OR "
            "name is NULL AND salary = 10"
        )

        query = db.ttable.query()
        query.where(condition="salary < 10")
        query.where(name=None, salary=10, filter_operator="OR")
        query.where(condition="name is not NULL")

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable WHERE salary < 10 OR "
            "name is NULL AND salary = 10 AND name is not null"
        )

        query = db.ttable.query()
        query.where(condition="salary < 10")
        query.where(name=None, salary=10, filter_operator="OR")
        query.where(salary=100)

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable WHERE salary < 10 OR "
            "name is NULL AND salary = 10 AND salary = 100"
        )

        query = db.ttable.query()
        query.where(condition="salary < 10")
        query.where(name=None, salary=10, filter_operator="OR")
        query.where(condition="name is not NULL")
        query.where(salary=100, operator="OR")

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable WHERE salary < 10 OR "
            "name is NULL AND salary = 10 AND name is not null "
            "AND salary = 100"
        )

        query = db.ttable.query()
        query.where(condition="     AND salary < 10")
        query.where(name=None, salary=10, filter_operator="OR")
        query.where(condition="or name is not NULL")
        query.where(salary=100, operator="OR")

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable WHERE salary < 10 OR "
            "name is NULL AND salary = 10 or name is not null "
            "AND salary = 100"
        )

    def test_limit(self, dbs: tuple[DB, ...]) -> None:
        """Test for limit and offset operation"""
        db, _ = dbs

        query = db.ttable.query()
        query.limit(10)

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable LIMIT 10"
        )

        query = db.ttable.query()
        query.limit(10).offset(10)

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable LIMIT 10 OFFSET 10"
        )

        query = db.ttable.query()
        query.offset(10).limit(10)

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable LIMIT 10 OFFSET 10"
        )

    def test_order(self, dbs: tuple[DB, ...]) -> None:
        """Test for order by operation."""
        db, _ = dbs

        query = db.ttable.query()
        query.order_by("name")

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable ORDER BY name"
        )

        query = db.ttable.query()
        query.order_by("name", "post", "salary")

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable ORDER BY name, post, salary"
        )

        query = db.ttable.query()
        query.order_by(name="desc", post="asc", salary="desc")

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable ORDER BY name desc, "
            "post asc, salary desc"
        )

        query = db.ttable.query()
        query.order_by("name", post="asc", salary="desc")

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable ORDER BY name"
        )

    def test_hard(self, dbs: tuple[DB, ...]) -> None:
        """Test all filtred operation in one query."""
        db, _ = dbs

        query: Query = db.ttable.query()
        query.where(condition="     AND salary < 10").order_by("name")
        query.where(name=None, salary=10, filter_operator="OR").offset(10)
        query.where(condition="or name is not NULL").order_by("salary")
        query.where(salary=100, operator="OR").limit(20)

        assert str(query) == (
            "SELECT `ttable`.id, `ttable`.name, `ttable`.post, "
            "`ttable`.salary FROM ttable WHERE salary < 10 OR "
            "name is NULL AND salary = 10 or name is not null "
            "AND salary = 100 LIMIT 20 OFFSET 10 ORDER BY name, salary"
        )

    def test_item(self, dbs: tuple[DB, ...]) -> None:
        """Test geting one item."""
        db_dict, db_cls = dbs

        query = db_cls.ttable.query()
        assert query.first() is not None

        query = db_dict.ttable.query().offset(10000)
        assert query.first() is None

        query = db_cls.ttable.query("id", "salary")
        extend_columns = {"name", "post"}
        row = query.first()
        assert all(col not in row.__dict__ for col in extend_columns)

    def test_items(self, dbs: tuple[DB, ...]) -> None:
        """Test geting many items."""
        db_dict, db_cls = dbs

        query = db_cls.ttable.query()
        assert all(row is not None for row in query.all())

        query = db_cls.ttable.query("id", "salary")
        extend_columns = {"name", "post"}
        assert all(
            col not in row.__dict__
            for row in query for col in extend_columns
        )

    def test_generative(self, dbs: tuple[DB, ...]) -> None:
        """Test geting many items."""
        db_dict, db_cls = dbs

        query = db_cls.ttable.query()
        for row in query.generative_all(2):
            assert row is not None
