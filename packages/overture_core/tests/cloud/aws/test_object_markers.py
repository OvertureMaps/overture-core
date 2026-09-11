"""Unit tests for the success-file, validation, console-URL, and parquet helpers in object.py."""

import io
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest

from overture_core.cloud.aws.object import (
    console_url,
    delete_success_file,
    read_parquet_prefix,
    success_file_exists,
    success_file_key,
    validate_location,
    write_success_file,
)

PATCH_CLIENT = "overture_core.cloud.aws.object.boto3.client"


def _client_error(code: str, operation: str) -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": code}}, operation
    )


class TestSuccessFileKey:
    def test_with_prefix(self):
        assert (
            success_file_key("feeds/x/ds=2024-01-01/")
            == "feeds/x/ds=2024-01-01/success"
        )

    def test_strips_leading_slash(self):
        assert success_file_key("/feeds/x") == "feeds/x/success"

    def test_empty_prefix(self):
        assert success_file_key("") == "success"


class TestWriteSuccessFile:
    def test_writes_marker(self):
        s3 = MagicMock()
        with patch(PATCH_CLIENT, return_value=s3):
            uri = write_success_file("s3://bucket/feeds/x/")
        assert uri == "s3://bucket/feeds/x/success"
        s3.put_object.assert_called_once_with(
            Bucket="bucket", Key="feeds/x/success", Body=b""
        )


class TestDeleteSuccessFile:
    def test_deletes_when_present(self):
        s3 = MagicMock()
        with patch(PATCH_CLIENT, return_value=s3):
            assert delete_success_file("s3://bucket/feeds/x") is True
        s3.delete_object.assert_called_once_with(Bucket="bucket", Key="feeds/x/success")

    def test_noop_when_missing(self):
        s3 = MagicMock()
        s3.head_object.side_effect = _client_error("404", "HeadObject")
        with patch(PATCH_CLIENT, return_value=s3):
            assert delete_success_file("s3://bucket/feeds/x") is False
        s3.delete_object.assert_not_called()


class TestSuccessFileExists:
    def test_delegates_to_head_object(self):
        s3 = MagicMock()
        with patch(PATCH_CLIENT, return_value=s3):
            assert success_file_exists("bucket", "feeds/x/") is True
        s3.head_object.assert_called_once_with(Bucket="bucket", Key="feeds/x/success")


class TestValidateLocation:
    def test_passes_exists_and_writable(self):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {"KeyCount": 1}
        with patch(PATCH_CLIENT, return_value=s3):
            validate_location(
                "bucket", "prefix", check_exists=True, check_writable=True
            )
        s3.head_bucket.assert_called_once_with(Bucket="bucket")
        s3.put_object.assert_called_once()

    def test_bucket_missing(self):
        s3 = MagicMock()
        s3.head_bucket.side_effect = _client_error("404", "HeadBucket")
        with patch(PATCH_CLIENT, return_value=s3):
            with pytest.raises(
                ValueError, match="source bucket does not exist: bucket"
            ):
                validate_location("bucket", "prefix", label="source")

    def test_other_head_bucket_error(self):
        s3 = MagicMock()
        s3.head_bucket.side_effect = _client_error("403", "HeadBucket")
        with patch(PATCH_CLIENT, return_value=s3):
            with pytest.raises(ValueError, match="Failed to validate location"):
                validate_location("bucket", "prefix")

    def test_empty_prefix_fails_when_check_exists(self):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {"KeyCount": 0}
        with patch(PATCH_CLIENT, return_value=s3):
            with pytest.raises(ValueError, match="does not exist or is empty"):
                validate_location("bucket", "prefix")

    def test_empty_prefix_ok_when_not_checking(self):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {"KeyCount": 0}
        with patch(PATCH_CLIENT, return_value=s3):
            validate_location("bucket", "prefix", check_exists=False)

    def test_not_writable(self):
        s3 = MagicMock()
        s3.put_object.side_effect = _client_error("AccessDenied", "PutObject")
        with patch(PATCH_CLIENT, return_value=s3):
            with pytest.raises(ValueError, match="destination bucket is not writable"):
                validate_location(
                    "bucket",
                    "prefix",
                    check_exists=False,
                    check_writable=True,
                    label="destination",
                )


class TestConsoleUrl:
    def test_prefix_url(self):
        assert console_url("s3://bucket/a/b c/", region="us-west-2") == (
            "https://us-west-2.console.aws.amazon.com/s3/buckets/bucket"
            "?region=us-west-2&prefix=a/b%20c/"
        )

    def test_object_url(self):
        assert console_url(
            "s3://bucket/a/file.txt", is_object=True, region="eu-west-1"
        ) == (
            "https://eu-west-1.console.aws.amazon.com/s3/object/bucket"
            "?region=eu-west-1&prefix=a/file.txt"
        )

    def test_defaults_region(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "ap-south-1")
        assert console_url("s3://bucket/a").startswith("https://ap-south-1.")


def _parquet_bytes(rows: list[dict]) -> bytes:
    pyarrow = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    pq.write_table(pyarrow.Table.from_pylist(rows), buf)
    return buf.getvalue()


class TestReadParquetPrefix:
    def _s3(self, objects: dict[str, bytes]):
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": key} for key in objects]}
        ]
        s3.get_object.side_effect = lambda Bucket, Key: {
            "Body": io.BytesIO(objects[Key])
        }
        return s3

    def test_concatenates_files_and_ignores_non_parquet(self):
        s3 = self._s3(
            {
                "out/part-0.parquet": _parquet_bytes([{"id": 1, "name": "a"}]),
                "out/part-1.parquet": _parquet_bytes([{"id": 2, "name": "b"}]),
                "out/_SUCCESS": b"",
            }
        )
        with patch(PATCH_CLIENT, return_value=s3):
            rows = read_parquet_prefix("s3://bucket/out", columns=["id"])
        assert rows == [{"id": 1}, {"id": 2}]
        s3.get_paginator.return_value.paginate.assert_called_once_with(
            Bucket="bucket", Prefix="out/"
        )

    def test_none_when_no_parquet(self):
        s3 = self._s3({"out/_SUCCESS": b""})
        with patch(PATCH_CLIENT, return_value=s3):
            assert read_parquet_prefix("s3://bucket/out/") is None
