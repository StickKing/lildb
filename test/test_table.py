"""Module contain test for select query."""
from __future__ import annotations

import pytest

from lildb import DB


@pytest.fixture(scope="package")
def db() -> DB:
    """Create db objects."""
    return DB._instances["test.db"]


class TestTable:
    """Test Create table operations."""

    def test_column_func(self, db: DB) -> None:
        """
        Simple creating table test without types,
        table primary keys and foreign keys.
        """
        table = db.ttable

        table.c.id.avg()
        table.c.id.count()
        table.c.id.sum()
        table.c.id.upper()
        table.c.id.lower()
        assert table.c.id.avg() == "AVG(`ttable`.id) AS id"
        assert table.c.id.count() == "COUNT(`ttable`.id) AS id"
        assert table.c.id.sum() == "SUM(`ttable`.id) AS id"
        assert table.c.name.upper() == "UPPER(`ttable`.name) AS name"
        assert table.c.name.lower() == "LOWER(`ttable`.name) AS name"
        # assert False
