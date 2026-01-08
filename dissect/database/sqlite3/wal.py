from __future__ import annotations

import logging
import os
import struct
from functools import cached_property, lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO

from dissect.database.sqlite3.c_sqlite3 import c_sqlite3
from dissect.database.sqlite3.exception import InvalidDatabase

if TYPE_CHECKING:
    from collections.abc import Iterator

log = logging.getLogger(__name__)
log.setLevel(os.getenv("DISSECT_LOG_SQLITE3", "CRITICAL"))

# See https://sqlite.org/fileformat2.html#wal_file_format
WAL_HEADER_MAGIC_LE = 0x377F0682
WAL_HEADER_MAGIC_BE = 0x377F0683
WAL_HEADER_MAGIC = {WAL_HEADER_MAGIC_LE, WAL_HEADER_MAGIC_BE}


class WAL:
    def __init__(self, fh: Path | BinaryIO):
        # Use the provided WAL file handle or try to open a sidecar WAL file.
        if isinstance(fh, Path):
            path = fh
            fh = path.open("rb")
        else:
            path = None

        self.fh = fh
        self.path = path
        self.header = c_sqlite3.wal_header(fh)

        if self.header.magic not in WAL_HEADER_MAGIC:
            raise InvalidDatabase("Invalid WAL header magic")

        self.checksum_endian = "<" if self.header.magic == WAL_HEADER_MAGIC_LE else ">"

        self.frame = lru_cache(1024)(self.frame)

    def close(self) -> None:
        """Close the WAL."""
        # Only close WAL handle if we opened it using a path
        if self.path is not None:
            self.fh.close()

    def frame(self, frame_idx: int) -> Frame:
        frame_size = len(c_sqlite3.wal_frame) + self.header.page_size
        offset = len(c_sqlite3.wal_header) + frame_idx * frame_size
        return Frame(self, offset)

    def frames(self) -> Iterator[Frame]:
        frame_idx = 0
        while True:
            try:
                yield self.frame(frame_idx)
                frame_idx += 1
            except EOFError:  # noqa: PERF203
                break

    @cached_property
    def commits(self) -> list[Commit]:
        """Return all commits in the WAL file.

        Commits are frames where ``header.page_count`` specifies the size of the
        database file in pages after the commit. For all other frames it is 0.

        References:
            - https://sqlite.org/fileformat2.html#wal_file_format
        """
        commits = []
        frames = []

        for frame in self.frames():
            frames.append(frame)

            # A commit record has a page_count header greater than zero
            if frame.page_count > 0:
                commits.append(Commit(self, frames))
                frames = []

        if frames:
            # TODO: Do we want to track these somewhere?
            log.warning("Found leftover %d frames after the last WAL commit", len(frames))

        return commits

    @cached_property
    def checkpoints(self) -> list[Checkpoint]:
        """Return deduplicated checkpoints, oldest first.

        Deduplicate commits by the ``salt1`` value of their first frame. Later
        commits overwrite earlier ones so the returned list contains the most
        recent commit for each ``salt1``, sorted ascending.

        References:
            - https://sqlite.org/fileformat2.html#wal_file_format
            - https://sqlite.org/wal.html#checkpointing
        """
        checkpoints_map: dict[int, Checkpoint] = {}
        for commit in self.commits:
            if not commit.frames:
                continue
            salt1 = commit.frames[0].header.salt1
            # Keep the most recent commit for each salt1 (later commits overwrite).
            checkpoints_map[salt1] = commit

        return [checkpoints_map[salt] for salt in sorted(checkpoints_map.keys())]


class Frame:
    def __init__(self, wal: WAL, offset: int):
        self.wal = wal
        self.offset = offset

        self.fh = wal.fh

        self.fh.seek(offset)
        self.header = c_sqlite3.wal_frame(self.fh)

    def __repr__(self) -> str:
        return f"<Frame page_number={self.page_number} page_count={self.page_count}>"

    # @property
    def valid(self, checksum: bool = False) -> bool:
        """Check if the frame is valid by comparing its salt values and verifying the checksum.

        A frame is valid if:
            - Its salt1 and salt2 values match those in the WAL header.
            - Its checksum matches the calculated checksum.

        References:
            - https://sqlite.org/fileformat2.html#wal_file_format
        """
        print(f"checksum: {checksum}")

        return self.validate_salt() and self.validate_checksum() if checksum else self.validate_salt()

    def validate_salt(self) -> bool:
        """Check if the frame's salt values match those in the WAL header.

        References:
            - https://sqlite.org/fileformat2.html#wal_file_format
        """
        salt1_match = self.header.salt1 == self.wal.header.salt1
        salt2_match = self.header.salt2 == self.wal.header.salt2

        return salt1_match and salt2_match

    def validate_checksum(self) -> bool:
        """Check if the frame's checksum matches the calculated checksum.

        The checksum values in the final 8 bytes of the frame-header (checksum-1 and checksum-2)
        exactly match the computed checksum over:

            1. the first 24 bytes of the WAL header
            2. the first 8 bytes of each frame header (up to and including this frame)
            3. the page data of each frame (up to and including this frame)
        
        References:
            - https://sqlite.org/fileformat2.html#wal_file_format
        """
        checksum_match = False
        pos = self.fh.tell()
        try:
            # Read the WAL header bytes from the beginning of the file
            wal_hdr_size = len(c_sqlite3.wal_header)
            wal_hdr_bytes = self.wal.header.dumps()
            # log.info(f"WAL Header Bytes: {wal_hdr_bytes.hex()}")
            if len(wal_hdr_bytes) < wal_hdr_size:
                raise EOFError("WAL header too small for checksum calculation")

            # Start seed with checksum over first 24 bytes of WAL header
            seed = self.calculate_checksum(wal_hdr_bytes[:24], endian=self.wal.checksum_endian)

            # Iterate frames from the first frame up to and including this frame
            frame_size = len(c_sqlite3.wal_frame) + self.wal.header.page_size
            first_frame_offset = len(c_sqlite3.wal_header)
            offset = first_frame_offset

            while offset <= self.offset:
                # Read frame header
                self.fh.seek(offset)
                frame_hdr_bytes = self.fh.read(len(c_sqlite3.wal_frame))
                if len(frame_hdr_bytes) < len(c_sqlite3.wal_frame):
                    raise EOFError("Incomplete frame header while calculating checksum")

                # Checksum first 8 bytes of frame header
                seed = self.calculate_checksum(frame_hdr_bytes[:8], seed=seed, endian=self.wal.checksum_endian)

                # Read and checksum page data
                page_offset = offset + len(c_sqlite3.wal_frame)
                self.fh.seek(page_offset)
                page_data = self.fh.read(self.wal.header.page_size)
                if len(page_data) < self.wal.header.page_size:
                    raise EOFError("Incomplete page data while calculating checksum")
                seed = self.calculate_checksum(page_data, seed=seed, endian=self.wal.checksum_endian)

                offset += frame_size

                # Compare calculated checksum to stored checksum in this frame header
                checksum_match = (seed[0], seed[1]) == (self.header.checksum1, self.header.checksum2)
                # log.info(f"Frame at offset {self.offset}: calculated checksum {seed}, "
                #       f"stored checksum ({self.header.checksum1}, {self.header.checksum2}), "
                #       f"match: {checksum_match}")

        finally:
            # restore file position
            try:
                self.fh.seek(pos)
            except Exception:
                pass

        return checksum_match

    def calculate_checksum(buf: bytes, seed: tuple[int, int] = (0, 0), endian: str = ">") -> tuple[int, int]:
        """Calculate the checksum of a WAL header or frame.
        References:
            - https://sqlite.org/fileformat2.html#checksum_algorithm
        """

        s0, s1 = seed
        num_ints = len(buf) // 4
        arr = struct.unpack(f"{endian}{num_ints}I", buf)

        for int_num in range(0, num_ints, 2):
            s0 = (s0 + (arr[int_num] + s1)) & 0xFFFFFFFF
            s1 = (s1 + (arr[int_num + 1] + s0)) & 0xFFFFFFFF

        return s0, s1

    @property
    def data(self) -> bytes:
        self.fh.seek(self.offset + len(c_sqlite3.wal_frame))
        return self.fh.read(self.wal.header.page_size)

    @property
    def page_number(self) -> int:
        return self.header.page_number

    @property
    def page_count(self) -> int:
        return self.header.page_count


class _FrameCollection:
    """Convenience class to keep track of a collection of frames that were committed together."""

    def __init__(self, wal: WAL, frames: list[Frame]):
        self.wal = wal
        self.frames = frames

    def __contains__(self, page: int) -> bool:
        return page in self.page_map

    def __getitem__(self, page: int) -> Frame:
        return self.page_map[page]

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} frames={len(self.frames)}>"

    @cached_property
    def page_map(self) -> dict[int, Frame]:
        return {frame.page_number: frame for frame in self.frames}

    def get(self, page: int, default: Any = None) -> Frame:
        return self.page_map.get(page, default)


class Checkpoint(_FrameCollection):
    """A checkpoint is an operation that transfers all committed transactions from
    the WAL file back into the main database file.

    References:
        - https://sqlite.org/fileformat2.html#wal_file_format
    """


class Commit(_FrameCollection):
    """A commit is a collection of frames that were committed together.

    References:
        - https://sqlite.org/fileformat2.html#wal_file_format
    """
