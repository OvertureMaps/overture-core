"""Readers for CSV, TSV and JSON files shipped as package data.

Stdlib only (``importlib.resources``, ``csv``, ``json``): no Spark session or
pandas, so the readers are safe in UDF setup on workers. Resources are read
through the ``Traversable`` API, so they resolve whether the package is
installed as a directory or inside a zipped wheel (how Glue installs it).
"""

import csv
import io
import json
from importlib import resources
from typing import Any

# utf-8-sig reads plain UTF-8 identically and also strips a BOM, which
# spreadsheet exports commonly add and which would otherwise corrupt the first
# CSV header or fail json.loads.
_ENCODING = "utf-8-sig"


def read_package_text(path: str, pkg: str, *, encoding: str = _ENCODING) -> str:
    """Text of the resource ``path`` (``/``-separated) inside package ``pkg``."""
    return resources.files(pkg).joinpath(path).read_text(encoding)


def read_package_csv(
    csv_path: str, pkg: str, *, delimiter: str = ",", encoding: str = _ENCODING
) -> list[dict[str, str | None]]:
    """Rows of a packaged delimited file as dicts keyed by the header row.

    Values are strings; no type inference is applied. A row shorter than the
    header yields ``None`` for its missing columns.
    """
    text = read_package_text(csv_path, pkg, encoding=encoding)
    return list(csv.DictReader(io.StringIO(text), delimiter=delimiter))


def read_package_tsv(
    tsv_path: str, pkg: str, *, encoding: str = _ENCODING
) -> list[dict[str, str | None]]:
    """Rows of a packaged tab-separated file as dicts. See `read_package_csv`."""
    return read_package_csv(tsv_path, pkg, delimiter="\t", encoding=encoding)


def read_package_json(json_path: str, pkg: str, *, encoding: str = _ENCODING) -> Any:
    """Parsed contents of a packaged JSON file."""
    return json.loads(read_package_text(json_path, pkg, encoding=encoding))
