"""Unit tests for overture_core.streams."""

import io
import zipfile

import boto3
import pytest
from moto import mock_aws

from overture_core.streams import NonSeekableReader

PAYLOAD = b"".join(f"{i},row\n".encode() for i in range(200_000))


class CountingReader:
    """Wrap a file object, counting bytes returned by read."""

    def __init__(self, fileobj):
        self._fileobj = fileobj
        self.bytes_read = 0

    def read(self, size=-1):
        data = self._fileobj.read(size)
        self.bytes_read += len(data)
        return data

    def __getattr__(self, name):
        return getattr(self._fileobj, name)


@pytest.fixture
def zip_entry():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("data.csv", PAYLOAD)
    buf.seek(0)
    with zipfile.ZipFile(buf) as zf, zf.open("data.csv") as entry:
        yield entry


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="bucket")
        yield client


def test_not_seekable():
    reader = NonSeekableReader(io.BytesIO(b"abc"))
    assert reader.readable() and not reader.seekable() and not reader.writable()


def test_read_and_readinto_delegate():
    reader = NonSeekableReader(io.BytesIO(b"abcdef"))
    assert reader.read(2) == b"ab"
    buf = bytearray(2)
    assert reader.readinto(buf) == 2
    assert bytes(buf) == b"cd"
    assert reader.read() == b"ef"


def test_wrapped_zip_entry_is_read_once_by_upload_fileobj(s3, zip_entry, decompressed):
    s3.upload_fileobj(NonSeekableReader(zip_entry), "bucket", "out.csv")

    assert decompressed["bytes"] == len(PAYLOAD)
    assert s3.get_object(Bucket="bucket", Key="out.csv")["Body"].read() == PAYLOAD


def test_unwrapped_zip_entry_is_read_twice(s3, zip_entry, decompressed):
    s3.upload_fileobj(zip_entry, "bucket", "out.csv")

    assert decompressed["bytes"] == 2 * len(PAYLOAD)
