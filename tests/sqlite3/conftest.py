from __future__ import annotations

from typing import TYPE_CHECKING, BinaryIO

import pytest

from tests._util import absolute_path

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def sqlite_db() -> BinaryIO:
    return absolute_path("_data/sqlite3/test.sqlite").open("rb")


@pytest.fixture
def sqlite_wal() -> BinaryIO:
    return absolute_path("_data/sqlite3/test.sqlite-wal").open("rb")


@pytest.fixture
def empty_db() -> BinaryIO:
    return absolute_path("_data/sqlite3/empty.sqlite").open("rb")
