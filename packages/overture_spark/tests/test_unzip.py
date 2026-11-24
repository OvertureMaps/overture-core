"""unzip_to_s3 must read Deflate64 (method 9) archives, which the stdlib zipfile
rejects (NAD r24 ships that way), and read each entry exactly once."""

from __future__ import annotations

import zipfile
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import inflate64
import pytest

from overture_core.deflate64 import ZIP_DEFLATE64, enable_zipfile_deflate64
from overture_spark.unzip import unzip_to_s3

# Larger than the 64 KiB Deflate64 window and than one zipfile read chunk.
PAYLOAD = b"".join(f"{i},NAD_r24,street {i % 97}\n".encode() for i in range(20_000))
SMALL = b"<metadata/>"


class _Deflate64Compressor:
    def __init__(self) -> None:
        self._d = inflate64.Deflater()

    def compress(self, data: bytes) -> bytes:
        return self._d.deflate(data)

    def flush(self) -> bytes:
        return self._d.flush()


def _write_deflate64_zip(path: Path, entries: dict[str, bytes]) -> None:
    enable_zipfile_deflate64()

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
        zipfile.ZipFile(path, "w") as zf,
    ):
        for name, data in entries.items():
            zf.writestr(zipfile.ZipInfo(name), data, compress_type=ZIP_DEFLATE64)


class _FakeS3:
    def __init__(self, zip_path: Path) -> None:
        self._zip_path = zip_path
        self.downloaded: list[str] = []
        self.uploaded: dict[str, bytes] = {}
        self.configs: list = []

    def download_file(self, bucket, key, dest):
        self.downloaded.append(f"s3://{bucket}/{key}")
        Path(dest).write_bytes(self._zip_path.read_bytes())

    def upload_fileobj(self, fileobj, bucket, key, Config=None):
        # Mirror s3transfer: a seekable stream would be read to the end first.
        assert not fileobj.seekable()
        self.uploaded[f"s3://{bucket}/{key}"] = fileobj.read()
        self.configs.append(Config)


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


def _run(zip_path: Path, src_uri: str, dst_uri: str, **kwargs):
    spark = MagicMock(name="spark")
    # Distinct access and client per bucket, so mixing them up fails.
    src, dst = _FakeS3(zip_path), _FakeS3(zip_path)
    clients = {f"access:{src_uri}": src, f"access:{dst_uri}": dst}
    with (
        patch(
            "overture_spark.unzip.resolve_s3_access",
            side_effect=lambda _spark, uri: f"access:{uri}",
        ) as resolve,
        patch(
            "overture_spark.unzip.s3_client",
            side_effect=lambda access, **_: clients[access],
        ),
        patch("overture_spark.unzip.delete_prefix") as delete,
    ):
        out = unzip_to_s3(spark, src_uri, dst_uri, **kwargs)
    return spark, out, src, dst, resolve, delete


def test_unzip_to_s3_streams_deflate64_entries(deflate64_zip: Path):
    src_uri, dst_uri = "s3://src/nad/source.zip", "s3://dst/tmp/source"
    spark, out, src, dst, resolve, delete = _run(
        deflate64_zip,
        src_uri,
        dst_uri,
        entry_filter=lambda n: (
            n.rsplit("/", 1)[-1].startswith("NAD_") and n.endswith(".txt")
        ),
        max_workers=2,
    )

    assert out == dst_uri
    resolve.assert_has_calls(
        [call(spark, src_uri), call(spark, dst_uri)], any_order=True
    )
    assert resolve.call_count == 2
    delete.assert_called_once_with(spark, dst_uri)
    assert src.downloaded == [src_uri]
    assert src.uploaded == {}
    assert dst.downloaded == []
    assert dst.uploaded == {"s3://dst/tmp/source/TXT/NAD_r24.txt": PAYLOAD}


def test_unzip_to_s3_uses_64_mib_parts(deflate64_zip: Path):
    *_, dst, _, _ = _run(deflate64_zip, "s3://src/a.zip", "s3://dst/out")

    assert len(dst.configs) == 2
    for config in dst.configs:
        assert config.multipart_chunksize == 64 * 1024 * 1024
        assert config.multipart_threshold == 64 * 1024 * 1024


def test_unzip_to_s3_accepts_s3a_and_skips_directories(tmp_path: Path):
    path = tmp_path / "plain.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("dir/", b"")
        zf.writestr("dir/a.txt", SMALL)
    *_, dst, _, _ = _run(path, "s3a://src/a.zip", "s3a://dst/out/")

    assert dst.uploaded == {"s3://dst/out/dir/a.txt": SMALL}


def test_unzip_to_s3_decompresses_each_entry_once(
    deflate64_zip: Path, monkeypatch: pytest.MonkeyPatch
):
    count = {"bytes": 0}
    original = zipfile.ZipExtFile.read

    def counting_read(self, n=-1):
        data = original(self, n)
        count["bytes"] += len(data)
        return data

    monkeypatch.setattr(zipfile.ZipExtFile, "read", counting_read)
    _run(deflate64_zip, "s3://src/a.zip", "s3://dst/out")

    assert count["bytes"] == len(PAYLOAD) + len(SMALL)


def test_unzip_to_s3_propagates_upload_errors_and_removes_temp_zip(
    deflate64_zip: Path,
):
    tmp_paths: list[str] = []
    original = _FakeS3.download_file

    def recording_download(self, bucket, key, dest):
        tmp_paths.append(dest)
        original(self, bucket, key, dest)

    with (
        patch.object(_FakeS3, "download_file", recording_download),
        patch.object(_FakeS3, "upload_fileobj", side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError, match="boom"),
    ):
        _run(deflate64_zip, "s3://src/a.zip", "s3://dst/out")

    assert not Path(tmp_paths[0]).exists()
