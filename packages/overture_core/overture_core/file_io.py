"""Readers for CSV, TSV and JSON files shipped as package data, plus stream helpers.

Stdlib only (``importlib.resources``, ``csv``, ``json``): no Spark session or
pandas, so the readers are safe in UDF setup on workers. Resources are read
through the ``Traversable`` API, so they resolve whether the package is
installed as a directory or inside a zipped wheel (how Glue installs it).
"""

import csv
import io
import json
from importlib import resources
from typing import IO, Any

# utf-8-sig reads plain UTF-8 identically and also strips a BOM, which
# spreadsheet exports commonly add and which would otherwise corrupt the first
# CSV header or fail json.loads.
_ENCODING = "utf-8-sig"


def read_package_text(path: str, pkg: str, *, encoding: str = _ENCODING) -> str:
    """Text of the resource ``path`` (``/``-separated) inside package ``pkg``.

    Decoded from the raw bytes, so line endings are preserved as stored.
    """
    return resources.files(pkg).joinpath(path).read_bytes().decode(encoding)


def read_package_csv(
    csv_path: str, pkg: str, *, delimiter: str = ",", encoding: str = _ENCODING
) -> list[dict[str, str | None]]:
    """Rows of a packaged delimited file as dicts keyed by the header row.

    Values are strings; no type inference is applied. A row shorter than the
    header yields ``None`` for its missing columns.
    """
    text = read_package_text(csv_path, pkg, encoding=encoding)
    return list(csv.DictReader(io.StringIO(text, newline=""), delimiter=delimiter))


def read_package_tsv(
    tsv_path: str, pkg: str, *, encoding: str = _ENCODING
) -> list[dict[str, str | None]]:
    """Rows of a packaged tab-separated file as dicts. See `read_package_csv`."""
    return read_package_csv(tsv_path, pkg, delimiter="\t", encoding=encoding)


def read_package_json(json_path: str, pkg: str, *, encoding: str = _ENCODING) -> Any:
    """Parsed contents of a packaged JSON file."""
    return json.loads(read_package_text(json_path, pkg, encoding=encoding))


class NonSeekableReader:
    """Read-only wrapper that hides ``seek``/``tell`` from the wrapped file object.

    A ``zipfile.ZipFile.open`` entry reports ``seekable()`` as true, so boto3
    ``upload_fileobj`` seeks to the end to learn the size, decompressing the
    whole entry, then rewinds and decompresses it again. This wrapper reports
    ``seekable()`` as false, so s3transfer takes its streaming multipart path
    and reads the entry once.

    Args:
        fileobj: Binary file object to read from. Closed by the caller, not here.
    """

    def __init__(self, fileobj: IO[bytes]) -> None:
        self._fileobj = fileobj

    def read(self, size: int = -1) -> bytes:
        """Read up to ``size`` bytes (all remaining bytes if negative)."""
        return self._fileobj.read(size)

    def readinto(self, buffer: bytearray | memoryview) -> int:
        """Read bytes into ``buffer`` and return the number read."""
        return self._fileobj.readinto(buffer)

    def readable(self) -> bool:
        """Return True: the wrapper is read-only."""
        return True

    def seekable(self) -> bool:
        """Return False so consumers never seek or rewind."""
        return False

    def writable(self) -> bool:
        """Return False: the wrapper is read-only."""
        return False
