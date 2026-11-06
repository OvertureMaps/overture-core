"""Unit tests for overture_core.deflate64: reading Deflate64 zips with zipfile."""

import io
import subprocess
import sys
import zipfile
from pathlib import Path
from unittest.mock import patch

import inflate64
import pytest

from overture_core.deflate64 import ZIP_DEFLATE64, enable_zipfile_deflate64

# Larger than the 64 KiB Deflate64 window and than one zipfile read chunk.
PAYLOAD = b"".join(f"{i},NAD_r24,street {i % 97}\n".encode() for i in range(20_000))
SMALL = b"<metadata/>"


@pytest.fixture(autouse=True)
def _stock_zipfile(monkeypatch):
    """Start each test on stock ``zipfile`` and undo the process-wide patch after."""
    monkeypatch.setattr(zipfile, "_get_decompressor", zipfile._get_decompressor)
    monkeypatch.setattr(zipfile, "_overture_deflate64", False, raising=False)


class _Deflate64Compressor:
    def __init__(self) -> None:
        self._d = inflate64.Deflater()

    def compress(self, data: bytes) -> bytes:
        return self._d.deflate(data)

    def flush(self) -> bytes:
        return self._d.flush()


def _write_deflate64_zip(path: Path, entries: dict[str, bytes]) -> None:
    """Write *entries* as Deflate64 members, which ``zipfile`` can't do itself."""

    def _get_compressor(compress_type, compresslevel=None):
        assert compress_type == ZIP_DEFLATE64
        return _Deflate64Compressor()

    orig_check = zipfile._check_compression

    def _check_compression(compress_type):
        if compress_type != ZIP_DEFLATE64:
            orig_check(compress_type)

    with (
        patch("zipfile._check_compression", _check_compression),
        patch("zipfile._get_compressor", _get_compressor),
    ):
        with zipfile.ZipFile(path, "w") as zf:
            for name, data in entries.items():
                zf.writestr(zipfile.ZipInfo(name), data, compress_type=ZIP_DEFLATE64)


@pytest.fixture
def deflate64_zip(tmp_path: Path) -> Path:
    path = tmp_path / "source.zip"
    _write_deflate64_zip(
        path,
        {"TXT/NAD_r24.txt": PAYLOAD, "NationalAddressDatabaseMetadata.xml": SMALL},
    )
    with zipfile.ZipFile(path) as zf:
        assert {i.compress_type for i in zf.infolist()} == {ZIP_DEFLATE64}
    return path


def test_stock_zipfile_rejects_deflate64(deflate64_zip: Path):
    with zipfile.ZipFile(deflate64_zip) as zf:
        with pytest.raises(NotImplementedError):
            zf.read("NationalAddressDatabaseMetadata.xml")


def test_zipfile_reads_deflate64_entry(deflate64_zip: Path):
    enable_zipfile_deflate64()
    with zipfile.ZipFile(deflate64_zip) as zf:
        with zf.open("TXT/NAD_r24.txt") as f:
            assert f.read() == PAYLOAD
        chunk_size = zipfile.ZipExtFile.MIN_READ_SIZE
        assert zf.getinfo("TXT/NAD_r24.txt").compress_size > chunk_size
        with zf.open("TXT/NAD_r24.txt") as f:
            assert b"".join(iter(lambda: f.read(chunk_size), b"")) == PAYLOAD
        assert zf.read("NationalAddressDatabaseMetadata.xml") == SMALL


def test_zipfile_still_reads_plain_deflate():
    enable_zipfile_deflate64()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("a.txt", PAYLOAD)
    with zipfile.ZipFile(io.BytesIO(buf.getvalue())) as zf:
        assert zf.read("a.txt") == PAYLOAD


def test_enable_is_idempotent():
    enable_zipfile_deflate64()
    patched = zipfile._get_decompressor
    assert zipfile._overture_deflate64 is True

    enable_zipfile_deflate64()

    assert zipfile._get_decompressor is patched


def test_flag_set_by_another_copy_skips_patch(monkeypatch):
    # e.g. an older copy of this patch elsewhere in the process ran first.
    monkeypatch.setattr(zipfile, "_overture_deflate64", True)
    stock = zipfile._get_decompressor

    enable_zipfile_deflate64()

    assert zipfile._get_decompressor is stock


def test_missing_extra_raises_and_leaves_zipfile_unpatched(monkeypatch):
    monkeypatch.setitem(sys.modules, "inflate64", None)
    stock = zipfile._get_decompressor

    with pytest.raises(ImportError, match=r"overture-core\[deflate64\]") as info:
        enable_zipfile_deflate64()

    assert isinstance(info.value.__cause__, ModuleNotFoundError)
    assert zipfile._get_decompressor is stock
    assert zipfile._overture_deflate64 is False


def test_import_does_not_need_inflate64():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys\n"
            "sys.modules['inflate64'] = None\n"
            "from overture_core.deflate64 import ZIP_DEFLATE64, enable_zipfile_deflate64\n"
            "assert ZIP_DEFLATE64 == 9",
        ],
        check=True,
    )
