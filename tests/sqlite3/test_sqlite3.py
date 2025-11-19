from __future__ import annotations

from io import BytesIO
from typing import Any, BinaryIO

import pytest

from dissect.database.sqlite3 import sqlite3


def test_sqlite(sqlite_db: BinaryIO) -> None:
    s = sqlite3.SQLite3(sqlite_db)

    assert s.header.magic == sqlite3.SQLITE3_HEADER_MAGIC

    tables = list(s.tables())
    assert len(tables) == 2

    table = tables[0]
    assert table.name == "test"
    assert table.page == 2
    assert [column.name for column in table.columns] == ["id", "name", "value"]
    assert table.primary_key == "id"
    assert s.table("test").__dict__ == table.__dict__

    rows = list(table.rows())
    assert len(rows) == 10
    assert rows[0].id == 1
    assert rows[0].name == "testing"
    assert rows[0].value == 1337
    assert rows[1].id == 2
    assert rows[1].name == "omg"
    assert rows[1].value == 7331
    assert rows[2].id == 3
    assert rows[2].name == "A" * 4100
    assert rows[2].value == 4100
    assert rows[3].id == 4
    assert rows[3].name == "B" * 4100
    assert rows[3].value == 4100
    assert rows[4].id == 5
    assert rows[4].name == "negative"
    assert rows[4].value == -11644473429

    assert len(rows) == len(list(table))
    assert table.row(0).__dict__ == rows[0].__dict__
    assert list(rows[0]) == [("id", 1), ("name", "testing"), ("value", 1337)]


def test_sqlite_wal(sqlite_db: BinaryIO, sqlite_wal: BinaryIO) -> None:
    # After the first checkpoint the "after checkpoint" entries are present
    s = sqlite3.SQLite3(sqlite_db, sqlite_wal, wal_checkpoint=2)

    table = list(s.tables())[0]

    rows = list(table.rows())
    assert len(rows) == 9

    assert rows[0].id == 1
    assert rows[0].name == "testing"
    assert rows[0].value == 1337
    assert rows[1].id == 2
    assert rows[1].name == "omg"
    assert rows[1].value == 7331
    assert rows[2].id == 3
    assert rows[2].name == "A" * 4100
    assert rows[2].value == 4100
    assert rows[3].id == 4
    assert rows[3].name == "B" * 4100
    assert rows[3].value == 4100
    assert rows[4].id == 5
    assert rows[4].name == "negative"
    assert rows[4].value == -11644473429
    assert rows[5].id == 6
    assert rows[5].name == "after checkpoint"
    assert rows[5].value == 42
    assert rows[6].id == 7
    assert rows[6].name == "after checkpoint"
    assert rows[6].value == 43
    assert rows[7].id == 8
    assert rows[7].name == "after checkpoint"
    assert rows[7].value == 44
    assert rows[8].id == 9
    assert rows[8].name == "after checkpoint"
    assert rows[8].value == 45

    sqlite_wal.seek(0)
    sqlite_db.seek(0)

    # After the second checkpoint two more entries are present ("second checkpoint")
    s = sqlite3.SQLite3(sqlite_db, sqlite_wal, wal_checkpoint=1)

    table = list(s.tables())[0]

    rows = list(table.rows())
    assert len(rows) == 11

    assert rows[0].id == 1
    assert rows[0].name == "testing"
    assert rows[0].value == 1337
    assert rows[1].id == 2
    assert rows[1].name == "omg"
    assert rows[1].value == 7331
    assert rows[2].id == 3
    assert rows[2].name == "A" * 4100
    assert rows[2].value == 4100
    assert rows[3].id == 4
    assert rows[3].name == "B" * 4100
    assert rows[3].value == 4100
    assert rows[4].id == 5
    assert rows[4].name == "negative"
    assert rows[4].value == -11644473429
    assert rows[5].id == 6
    assert rows[5].name == "after checkpoint"
    assert rows[5].value == 42
    assert rows[6].id == 7
    assert rows[6].name == "after checkpoint"
    assert rows[6].value == 43
    assert rows[7].id == 8
    assert rows[7].name == "after checkpoint"
    assert rows[7].value == 44
    assert rows[8].id == 9
    assert rows[8].name == "after checkpoint"
    assert rows[8].value == 45
    assert rows[9].id == 10
    assert rows[9].name == "second checkpoint"
    assert rows[9].value == 100
    assert rows[10].id == 11
    assert rows[10].name == "second checkpoint"
    assert rows[10].value == 101

    sqlite_wal.seek(0)
    sqlite_db.seek(0)

    # After the third checkpoint the deletion and update of one "after checkpoint" are reflected
    s = sqlite3.SQLite3(sqlite_db, sqlite_wal, wal_checkpoint=0)

    table = list(s.tables())[0]
    rows = list(table.rows())

    assert len(rows) == 10

    assert rows[0].id == 1
    assert rows[0].name == "testing"
    assert rows[0].value == 1337
    assert rows[1].id == 2
    assert rows[1].name == "omg"
    assert rows[1].value == 7331
    assert rows[2].id == 3
    assert rows[2].name == "A" * 4100
    assert rows[2].value == 4100
    assert rows[3].id == 4
    assert rows[3].name == "B" * 4100
    assert rows[3].value == 4100
    assert rows[4].id == 5
    assert rows[4].name == "negative"
    assert rows[4].value == -11644473429
    assert rows[5].id == 6
    assert rows[5].name == "after checkpoint"
    assert rows[5].value == 42
    assert rows[6].id == 8
    assert rows[6].name == "after checkpoint"
    assert rows[6].value == 44
    assert rows[7].id == 9
    assert rows[7].name == "wow"
    assert rows[7].value == 1234
    assert rows[8].id == 10
    assert rows[8].name == "second checkpoint"
    assert rows[8].value == 100
    assert rows[9].id == 11
    assert rows[9].name == "second checkpoint"
    assert rows[9].value == 101


@pytest.mark.parametrize(
    ("input", "encoding", "expected_output"),
    [
        (b"\x04\x00\x1b\x02testing\x059", "utf-8", ([0, 27, 2], [None, "testing", 1337])),
        (b"\x02\x65\x80\x81\x82\x83", "utf-8", ([101], [b"\x80\x81\x82\x83"])),
    ],
)
def test_sqlite_read_record(input: bytes, encoding: str, expected_output: tuple[list[int], list[Any]]) -> None:
    assert sqlite3.read_record(BytesIO(input), encoding) == expected_output


def test_empty(empty_db: BinaryIO) -> None:
    s = sqlite3.SQLite3(empty_db)

    assert s.encoding == "utf-8"
    assert len(list(s.tables())) == 0
