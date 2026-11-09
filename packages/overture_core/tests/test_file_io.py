"""Unit tests for overture_core.file_io: packaged CSV/TSV/JSON readers."""

import importlib
import json
import sys
import zipfile

import pytest

from overture_core.file_io import (
    read_package_csv,
    read_package_json,
    read_package_text,
    read_package_tsv,
)

_FILES = {
    "lookup.csv": "country,alias,code\nUS,America,us\nES,,es\n",
    "sub/lookup.tsv": "a\tb\nx\ty\n",
    "sub/data.json": json.dumps({"k": [1, 2], "ü": "v"}),
    "bom.csv": "\ufeffid,name\n1,one\n",
    "bom.json": '\ufeff{"k": 1}',
    "short.csv": "a,b\n1\n",
    "multiline.csv": 'id,note\r\n1,"a\r\nb"\r\n2,"c\nd"\r\n',
}


def _write_files(root, files):
    for name, content in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content.encode("utf-8"))


@pytest.fixture(params=["directory", "zip"])
def pkg(request, tmp_path, monkeypatch):
    """A fresh importable package holding ``_FILES``, as a directory or a zip."""
    name = f"pkgdata_{request.param}"
    if request.param == "directory":
        root = tmp_path / name
        root.mkdir()
        (root / "__init__.py").write_text("", encoding="utf-8")
        _write_files(root, _FILES)
        monkeypatch.syspath_prepend(str(tmp_path))
    else:
        staging = tmp_path / "staging"
        staging.mkdir()
        _write_files(staging, _FILES)
        archive = tmp_path / "wheel.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr(f"{name}/__init__.py", "")
            for path in staging.rglob("*"):
                if path.is_file():
                    zf.write(path, f"{name}/{path.relative_to(staging).as_posix()}")
        monkeypatch.syspath_prepend(str(archive))
    importlib.invalidate_caches()
    yield name
    sys.modules.pop(name, None)


def test_read_package_csv(pkg):
    assert read_package_csv("lookup.csv", pkg) == [
        {"country": "US", "alias": "America", "code": "us"},
        {"country": "ES", "alias": "", "code": "es"},
    ]


def test_read_package_csv_custom_delimiter(pkg):
    assert read_package_csv("sub/lookup.tsv", pkg, delimiter="\t") == [
        {"a": "x", "b": "y"}
    ]


def test_read_package_csv_strips_bom(pkg):
    assert read_package_csv("bom.csv", pkg) == [{"id": "1", "name": "one"}]


def test_read_package_csv_preserves_embedded_line_endings(pkg):
    assert [r["note"] for r in read_package_csv("multiline.csv", pkg)] == [
        "a\r\nb",
        "c\nd",
    ]


def test_read_package_csv_short_row_is_none(pkg):
    assert read_package_csv("short.csv", pkg) == [{"a": "1", "b": None}]


def test_read_package_tsv(pkg):
    assert read_package_tsv("sub/lookup.tsv", pkg) == [{"a": "x", "b": "y"}]


def test_read_package_json(pkg):
    assert read_package_json("sub/data.json", pkg) == {"k": [1, 2], "ü": "v"}


def test_read_package_json_strips_bom(pkg):
    assert read_package_json("bom.json", pkg) == {"k": 1}


def test_read_package_text(pkg):
    assert read_package_text("lookup.csv", pkg) == _FILES["lookup.csv"]


def test_read_package_text_preserves_line_endings(pkg):
    assert read_package_text("multiline.csv", pkg) == _FILES["multiline.csv"]


def test_read_package_text_strips_bom_by_default(pkg):
    assert read_package_text("bom.csv", pkg) == "id,name\n1,one\n"


def test_read_package_text_explicit_encoding_keeps_bom(pkg):
    assert read_package_text("bom.csv", pkg, encoding="utf-8") == _FILES["bom.csv"]


def test_missing_resource_raises(pkg):
    with pytest.raises(FileNotFoundError):
        read_package_csv("nope.csv", pkg)


def test_missing_package_raises():
    with pytest.raises(ModuleNotFoundError):
        read_package_csv("lookup.csv", "no_such_package_xyz")
