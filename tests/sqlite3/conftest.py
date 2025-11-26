from __future__ import annotations

from pathlib import Path

import pytest

from tests._util import absolute_path


@pytest.fixture
def sqlite_db() -> Path:
    return absolute_path("_data/sqlite3/test.sqlite")


@pytest.fixture
def sqlite_wal() -> Path:
    return absolute_path("_data/sqlite3/test.sqlite-wal")


@pytest.fixture
def empty_db() -> Path:
    return absolute_path("_data/sqlite3/empty.sqlite")
