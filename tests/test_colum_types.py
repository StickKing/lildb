"""Module contain tests for column types."""
from __future__ import annotations

import json
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from fractions import Fraction
from typing import Any

from lildb.column_types import Blob
from lildb.column_types import DataClassJson
from lildb.column_types import Date
from lildb.column_types import DateTime
from lildb.column_types import EnumHide
from lildb.column_types import EnumName
from lildb.column_types import EnumValue
from lildb.column_types import ForeignKey
from lildb.column_types import Integer
from lildb.column_types import Json
from lildb.column_types import Real
from lildb.column_types import Text
from lildb.column_types import Time
from lildb.enumcls import DeleteAction
from lildb.enumcls import UpdateAction


class MixinCommon:
    """Common test."""

    column_type = None

    def get_type_name(self) -> str:
        """Get upper type name."""
        return self.column_type.__name__.upper()

    def get_instance(self, *args: Any, **kwargs: Any) -> Any:
        """Get instance."""
        return self.column_type(*args, **kwargs)

    def test_type_str_view(self) -> None:
        """Test str view for all type."""
        type_name = self.get_type_name()

        type_instance = self.get_instance()
        assert type_name == str(type_instance)

        type_instance = self.get_instance(primary_key=True)
        assert f"{type_name} PRIMARY KEY  NOT NULL " == str(type_instance)

        type_instance = self.get_instance(unique=True)
        assert f"{type_name} UNIQUE " == str(type_instance)

        type_instance = self.get_instance(nullable=False)
        assert f"{type_name} NOT NULL " == str(type_instance)

        type_instance = self.get_instance(nullable=False, unique=True)
        assert f"{type_name} NOT NULL  UNIQUE " == str(type_instance)

        type_instance = self.get_instance(
            nullable=False,
            unique=True,
            primary_key=True
        )
        assert (
            f"{type_name} PRIMARY KEY  NOT NULL  UNIQUE " ==
            str(type_instance)
        )

    def test_to_db(self) -> None:
        """Serialize data to db data."""
        type_instance = self.get_instance()

        assert None is type_instance.to_db(None)

        class RandType:
            pass

        try:
            type_instance.to_db(RandType)
        except TypeError:
            assert True
        except ValueError:
            assert True
        else:
            assert False

    def test_to_python(self) -> None:
        """Serialize db data to python data."""
        type_instance = self.get_instance()

        assert None is type_instance.to_python(None)

        class RandType:
            pass

        try:
            type_instance.to_python(RandType)
        except TypeError:
            assert True
        except ValueError:
            assert True
        else:
            assert False


class TestInterger(MixinCommon):
    """Test for integer type."""

    column_type = Integer

    def test_type_str_view(self) -> None:
        super().test_type_str_view()
        type_name = self.get_type_name()

        type_instance = self.column_type(default=10)
        assert f"{type_name} DEFAULT 10 " == str(type_instance)

        type_instance = self.column_type(autoincrement=True)
        assert type_name == str(type_instance)

        type_instance = self.column_type(
            primary_key=True,
            autoincrement=True,
        )
        assert (
            f"{type_name} PRIMARY KEY AUTOINCREMENT  NOT NULL " ==
            str(type_instance)
        )

        type_instance = self.column_type(
            primary_key=True,
            default=10,
        )
        assert (
            f"{type_name} DEFAULT 10  PRIMARY KEY  NOT NULL " ==
            str(type_instance)
        )

        type_instance = self.column_type(
            primary_key=True,
            autoincrement=True,
            unique=True,
        )
        assert (
            f"{type_name} PRIMARY KEY AUTOINCREMENT  NOT NULL  UNIQUE " ==
            str(type_instance)
        )

    def test_to_db(self) -> None:
        """Serialize data to db data."""
        super().test_to_db()

        type_instance = self.column_type()

        assert 10 == type_instance.to_db(10)

        assert 10 == type_instance.to_db("10")

        assert 10 == type_instance.to_db(10.12)

        assert 10 == type_instance.to_db(Fraction(10.12))

        assert 10 == type_instance.to_db(Decimal(10.12))

    def test_to_python(self) -> None:
        """Serialize to python data."""
        super().test_to_python()

        type_instance = self.column_type()

        assert 10 == type_instance.to_python(10)


class TestText(MixinCommon):
    """Test for text type."""

    column_type = Text

    def test_type_str_view(self) -> None:
        super().test_type_str_view()
        type_name = self.get_type_name()

        type_instance = self.column_type(default="hello world")
        assert f"{type_name} DEFAULT 'hello world' " == str(type_instance)

        type_instance = self.column_type(
            primary_key=True,
            default="hello world",
        )
        assert (
            f"{type_name} DEFAULT 'hello world'  PRIMARY KEY  NOT NULL " ==
            str(type_instance)
        )

    def test_to_db(self) -> None:
        """."""
        super().test_to_db()

        type_instance = self.column_type()

        assert "hello world" == type_instance.to_db("hello world")

    def test_to_python(self) -> None:
        """."""
        super().test_to_python()

        type_instance = self.column_type()

        assert "hello world" == type_instance.to_python("hello world")


class TestReal(MixinCommon):
    """Test for real type."""

    column_type = Real

    def test_type_str_view(self) -> None:
        super().test_type_str_view()
        type_name = self.get_type_name()

        type_instance = self.column_type(default=10.12)
        assert f"{type_name} DEFAULT 10.12 " == str(type_instance)

        type_instance = self.column_type(
            primary_key=True,
            default=10.12,
        )
        assert (
            f"{type_name} DEFAULT 10.12  PRIMARY KEY  NOT NULL " ==
            str(type_instance)
        )

    def test_to_db(self) -> None:
        """Serialize data to db data."""
        super().test_to_db()

        type_instance = self.column_type()

        assert 10 == type_instance.to_db(10)

        assert 10.12 == type_instance.to_db(10.12)

        assert Fraction(10.12) == type_instance.to_db(Fraction(10.12))

        assert Decimal(10.12) == type_instance.to_db(Decimal(10.12))

    def test_to_python(self) -> None:
        """Serialize to python data."""
        super().test_to_python()

        type_instance = self.column_type()

        assert 10 == type_instance.to_python(10)

        assert 10.12 == type_instance.to_python(10.12)


class TestBlob(MixinCommon):
    """Test for blob type."""

    column_type = Blob

    def test_to_db(self) -> None:
        """."""
        super().test_to_db()

        type_instance = self.column_type()
        assert b"hello world" == type_instance.to_db(b"hello world")

    def test_to_python(self) -> None:
        """."""
        super().test_to_python()

        type_instance = self.column_type()
        assert b"hello world" == type_instance.to_db(b"hello world")


class TestForeignKey:
    """Tests for ForeignKey."""

    def test_type_str_view(self) -> None:
        """Test str view."""
        foreign_key = ForeignKey("test_id", "Test", "id")
        assert (
            "FOREIGN KEY(`test_id`) REFERENCES `Test`(`id`)" ==
            str(foreign_key)
        )

        foreign_key = ForeignKey(
            "test_id",
            "Test",
            "id",
            on_delete=DeleteAction.cascade,
        )
        assert (
            "FOREIGN KEY(`test_id`) REFERENCES `Test`(`id`) ON DELETE cascade"
            == str(foreign_key)
        )

        foreign_key = ForeignKey(
            "test_id",
            "Test",
            "id",
            on_delete=DeleteAction.set_default,
        )
        assert (
            "FOREIGN KEY(`test_id`) REFERENCES "
            "`Test`(`id`) ON DELETE set default" ==
            str(foreign_key)
        )

        foreign_key = ForeignKey(
            "test_id",
            "Test",
            "id",
            on_delete=DeleteAction.set_null,
        )
        assert (
            "FOREIGN KEY(`test_id`) REFERENCES "
            "`Test`(`id`) ON DELETE set null" ==
            str(foreign_key)
        )

        foreign_key = ForeignKey(
            "test_id",
            "Test",
            "id",
            on_delete=DeleteAction.restrict,
        )
        assert (
            "FOREIGN KEY(`test_id`) REFERENCES "
            "`Test`(`id`) ON DELETE restrict" ==
            str(foreign_key)
        )

        foreign_key = ForeignKey(
            "test_id",
            "Test",
            "id",
            on_update=UpdateAction.cascade
        )
        assert (
            "FOREIGN KEY(`test_id`) REFERENCES "
            "`Test`(`id`) ON UPDATE cascade" ==
            str(foreign_key)
        )

        foreign_key = ForeignKey(
            "test_id",
            "Test",
            "id",
            on_update=UpdateAction.set_null
        )
        assert (
            "FOREIGN KEY(`test_id`) REFERENCES "
            "`Test`(`id`) ON UPDATE set null" ==
            str(foreign_key)
        )

        foreign_key = ForeignKey(
            "test_id",
            "Test",
            "id",
            on_update=UpdateAction.restrict
        )
        assert (
            "FOREIGN KEY(`test_id`) REFERENCES "
            "`Test`(`id`) ON UPDATE restrict" ==
            str(foreign_key)
        )

        try:
            foreign_key = ForeignKey(
                "test_id",
                "Test",
                "id",
                on_delete=UpdateAction.restrict
            )
        except ValueError:
            assert True
        else:
            assert False

        try:
            foreign_key = ForeignKey(
                "test_id",
                "Test",
                "id",
                on_update=DeleteAction.restrict
            )
        except ValueError:
            assert True
        else:
            assert False


class TestJson(MixinCommon):
    """Tests for json type."""

    column_type = Json

    def get_type_name(self) -> str:
        super().get_type_name()
        return self.column_type().data.upper()

    def test_to_db(self) -> None:
        """."""
        super().test_to_db()
        type_instance = self.column_type()

        assert "hello world" == type_instance.to_db("hello world")

        assert '{"title": "News"}' == type_instance.to_db({"title": "News"})

    def test_to_python(self) -> None:
        """."""
        super().test_to_python()
        type_instance = self.column_type()

        assert (
            {"title": "News"} ==
            type_instance.to_python('{"title": "News"}')
        )


class TestDateTime(MixinCommon):
    """Tests for datetime type."""

    column_type = DateTime

    def test_type_str_view(self) -> None:
        """."""
        type_name = self.column_type(
            datetime_db_format="ISO",
        ).data.upper()

        self.get_type_name = lambda: type_name
        super().test_type_str_view()

        # type_name = self.column_type(
        #     datetime_db_format="timestamp",
        # ).data.upper()

        # self.get_type_name = lambda: type_name
        # super().test_type_str_view()

    def test_to_db(self) -> None:
        """."""
        super().test_to_db()
        type_instance = self.column_type()

        assert "hello world" == type_instance.to_db("hello world")

        dttm = datetime.now()
        assert dttm.isoformat() == type_instance.to_db(dttm)

        dt = dttm.date()
        dt_like_dttm = datetime.combine(dt, datetime.min.time())
        assert dt_like_dttm.isoformat() == type_instance.to_db(dt)

        try:
            type_instance.to_db(10.12)
        except TypeError:
            assert True
        else:
            assert False

        type_instance = self.column_type(
            datetime_db_format="timestamp"
        )

        assert dttm.timestamp() == type_instance.to_db(dttm)

        assert dt_like_dttm.timestamp() == type_instance.to_db(dt)

        try:
            type_instance.to_db("hello world")
        except TypeError:
            assert True
        else:
            assert False

    def test_to_python(self) -> None:
        super().test_to_python()
        dttm = datetime.now()
        iso_dttm = dttm.isoformat()
        timestamp_dttm = dttm.timestamp()

        type_instance = self.column_type()

        assert dttm == type_instance.to_python(iso_dttm)

        try:
            type_instance.to_python(10.12)
        except TypeError:
            assert True

        type_instance = self.column_type(datetime_db_format="timestamp")

        assert dttm == type_instance.to_python(timestamp_dttm)

        try:
            type_instance.to_python("hello world")
        except TypeError:
            assert True


class TestDate(MixinCommon):
    """Tests for date type."""

    column_type = Date

    def test_type_str_view(self) -> None:
        """."""
        type_name = self.column_type(
            date_db_format="ISO",
        ).data.upper()

        self.get_type_name = lambda: type_name
        super().test_type_str_view()

        # type_name = self.column_type(
        #     datetime_db_format="timestamp",
        # ).data.upper()

        # self.get_type_name = lambda: type_name
        # super().test_type_str_view()

    def test_to_db(self) -> None:
        """."""
        super().test_to_db()
        type_instance = self.column_type()

        assert "hello world" == type_instance.to_db("hello world")

        dttm = datetime.now()
        dt = dttm.date()
        dt_like_dttm = datetime.combine(dt, datetime.min.time())
        assert dt.isoformat() == type_instance.to_db(dttm)

        assert dt.isoformat() == type_instance.to_db(dt)

        try:
            type_instance.to_db(10.12)
        except TypeError:
            assert True
        else:
            assert False

        type_instance = self.column_type(
            date_db_format="timestamp"
        )

        assert dttm.timestamp() == type_instance.to_db(dttm)

        assert dt_like_dttm.timestamp() == type_instance.to_db(dt)

        try:
            type_instance.to_db("hello world")
        except TypeError:
            assert True
        else:
            assert False

        type_instance = self.column_type(
            date_db_format="ordinal"
        )

        assert dt.toordinal() == type_instance.to_db(dttm)

        assert dt.toordinal() == type_instance.to_db(dt)

        try:
            type_instance.to_db("hello world")
        except TypeError:
            assert True
        else:
            assert False

    def test_to_python(self) -> None:
        super().test_to_python()
        dttm = datetime.now()
        date = dttm.date()
        iso_dttm = dttm.isoformat()
        timestamp_dttm = dttm.timestamp()
        ordinal_dttm = dttm.toordinal()

        type_instance = self.column_type()

        assert date == type_instance.to_python(iso_dttm)
        assert date == type_instance.to_python(date.isoformat())

        try:
            type_instance.to_python(10.12)
        except TypeError:
            assert True

        type_instance = self.column_type(date_db_format="timestamp")

        assert date == type_instance.to_python(timestamp_dttm)

        try:
            type_instance.to_python("hello world")
        except TypeError:
            assert True

        type_instance = self.column_type(date_db_format="ordinal")

        assert date == type_instance.to_python(ordinal_dttm)

        try:
            type_instance.to_python("hello world")
        except TypeError:
            assert True


class TestTime(MixinCommon):
    """Tests for time type."""

    column_type = Time

    def get_type_name(self) -> str:
        return self.column_type().data

    def test_to_db(self) -> None:
        """."""
        super().test_to_db()
        dttm = datetime.now()
        tm = dttm.time()
        type_instance = self.column_type()

        test_str = "hello world"
        assert test_str == type_instance.to_db(test_str)

        assert tm.isoformat() == type_instance.to_db(tm)

    def test_to_python(self):
        super().test_to_python()
        dttm = datetime.now()
        tm = dttm.time()
        type_instance = self.column_type()

        assert tm == type_instance.to_python(tm.isoformat())


class EnumTest(Enum):
    """Testing enum."""

    hello = "world"
    age = 100


class TestEnumName(MixinCommon):
    """Tests for enum name type."""

    column_type = EnumName

    def get_type_name(self) -> str:
        return self.get_instance().data

    def get_instance(self, *args, **kwargs) -> EnumName:
        return super().get_instance(EnumTest, *args, **kwargs)

    def test_to_db(self) -> None:
        super().test_to_db()
        type_instance = self.get_instance()

        assert EnumTest.hello.name == type_instance.to_db(EnumTest.hello)
        assert EnumTest.hello.name == type_instance.to_db(EnumTest.hello.name)

        try:
            type_instance.to_db("asasas")
        except ValueError:
            assert True

    def test_to_python(self) -> None:
        super().test_to_python()
        type_instance = self.get_instance()

        assert EnumTest.age == type_instance.to_python("age")


class TestEnumValue(MixinCommon):
    """Tests for enum value type."""

    column_type = EnumValue

    def get_type_name(self) -> str:
        """."""
        return self.get_instance().data

    def get_instance(self, *args, **kwargs) -> EnumValue:
        """."""
        return super().get_instance(EnumTest, *args, **kwargs)

    def test_to_db(self):
        """."""
        super().test_to_db()
        type_instance = self.get_instance()

        assert EnumTest.age.value == type_instance.to_db(EnumTest.age)
        assert EnumTest.hello.value == type_instance.to_db(EnumTest.hello)

        try:
            type_instance.to_db("asasas")
        except ValueError:
            assert True

    def test_to_python(self) -> None:
        super().test_to_python()
        type_instance = self.get_instance()

        assert EnumTest.age == type_instance.to_python(EnumTest.age.value)
        assert EnumTest.hello == type_instance.to_python(EnumTest.hello.value)


class TestEnumHide(MixinCommon):
    """Tests for enum type."""

    column_type = EnumHide

    def get_type_name(self) -> str:
        """."""
        return self.get_instance().data

    def get_instance(self, *args, **kwargs) -> EnumHide:
        """."""
        return super().get_instance(EnumTest, *args, **kwargs)

    def test_to_db(self) -> None:
        """."""
        super().test_to_db()
        type_instance = self.get_instance()

        assert EnumTest.hello.name == type_instance.to_db(EnumTest.hello.name)
        assert EnumTest.hello.name == type_instance.to_db(EnumTest.hello)

        assert EnumTest.age.name == type_instance.to_db(EnumTest.age.name)
        assert EnumTest.age.name == type_instance.to_db(EnumTest.age)

        try:
            type_instance.to_db("asasas")
        except ValueError:
            assert True

    def test_to_python(self) -> None:
        """."""
        super().test_to_python()
        type_instance = self.get_instance()

        assert EnumTest.hello.value == type_instance.to_python(
            EnumTest.hello.name
        )

        assert EnumTest.age.value == type_instance.to_python(
            EnumTest.age.name
        )


@dataclass
class DataClsTest:
    """Test data class."""

    title: str
    lead: str


class TestDateClassJson(MixinCommon):
    """Tests for json type."""

    column_type = DataClassJson

    def get_type_name(self) -> str:
        """."""
        return self.get_instance().data

    def get_instance(self, *args, **kwargs) -> EnumHide:
        """."""
        kwargs["data_class"] = DataClsTest
        return super().get_instance(*args, **kwargs)

    def test_to_db(self) -> None:
        """."""
        super().test_to_db()
        type_instance = self.get_instance()

        test_data = DataClsTest("hello", "world")
        test_data_dict = asdict(test_data)
        test_str = "hello"

        assert test_str == type_instance.to_db(test_str)
        assert json.dumps(test_data_dict) == type_instance.to_db(test_data)

    def test_to_python(self) -> None:
        """."""
        super().test_to_python()
        type_instance = self.get_instance()
        test_data_str = '{"title": "hello", "lead": "world"}'
        test_data = DataClsTest("hello", "world")

        assert test_data == type_instance.to_python(test_data_str)
