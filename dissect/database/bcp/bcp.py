from __future__ import annotations

from pathlib import Path
from struct import unpack
from typing import TYPE_CHECKING, Any, BinaryIO

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

    from typing_extensions import Self

class BCP:
    def __init__(self, fh: Path | BinaryIO):
        if isinstance(fh, Path):
            path = fh
            fh = path.open("rb")
        else:
            path = None
        
        self.fh = fh
        self.path = path

    def __enter__(self) -> BCP:
        return self
    
    def __exit__(self, _: type[BaseException] | None, __: BaseException | None, ___: TracebackType | None) -> bool:
        self.close()
        return False

    def close(self) -> None:
        if self.path is not None:
            self.fh.close()

    def values(self) -> Iterator[Any]:
        fh = self.fh
        fh.seek(0)
        while True:
            length_byte = fh.read(1)
            if not length_byte:
                break
            length = unpack("B", length_byte)[0]
            if length == 0:
                continue
            value = fh.read(length + 1)
            if not value or len(value) < length + 1:
                break
            yield value