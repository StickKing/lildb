"""Module contains lib enum cls."""
from enum import Enum


__all__ = (
    "ResultFetch",
    "UpdateAction",
    "DeleteAction",
)


class ResultFetch(Enum):
    """Enum for fetching data from DB."""

    fetchmany = "fetchmany"
    fetchall = "fetchall"
    fetchone = "fetchone"


class UpdateAction(Enum):
    """On update action for foreign key column."""

    cascade = "cascade"
    set_null = "set null"
    restrict = "restrict"


class DeleteAction(Enum):
    """On delete action for foreign key column."""

    set_default = "set default"
    cascade = "cascade"
    set_null = "set null"
    restrict = "restrict"
