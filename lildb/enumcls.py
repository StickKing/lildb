"""Module contains lib enum cls."""
from enum import Enum


__all__ = (
    "ResultFetch",
)


class ResultFetch(Enum):
    """Enum for fetching data from DB."""

    fetchmany = "fetchmany"
    fetchall = "fetchall"
    fetchone = "fetchone"
