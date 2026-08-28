"""Unit tests for S3 object and prefix utilities."""

from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest

from overture_core.s3 import (
    CopyResult,
    build_s3_uri,
    copy_prefix,
    delete_object,
    delete_prefix,
    object_exists,
    parse_s3_uri,
    write_marker,
)


class TestParseS3Uri:
    def test_splits_bucket_and_key(self):
        assert parse_s3_uri("s3://my-bucket/some/prefix/file.txt") == (
            "my-bucket",
            "some/prefix/file.txt",
        )

    def test_bucket_only(self):
        assert parse_s3_uri("s3://my-bucket") == ("my-bucket", "")

    def test_rejects_non_s3_uri(self):
        with pytest.raises(ValueError, match="Not an S3 URI"):
            parse_s3_uri("https://example.com/file.txt")


class TestBuildS3Uri:
    def test_builds_uri(self):
        assert build_s3_uri("my-bucket", "some/prefix/file.txt") == (
            "s3://my-bucket/some/prefix/file.txt"
        )

    def test_strips_leading_slash_from_key(self):
        assert build_s3_uri("my-bucket", "/file.txt") == "s3://my-bucket/file.txt"

    def test_round_trips_with_parse(self):
        uri = "s3://my-bucket/some/prefix/file.txt"
        bucket, key = parse_s3_uri(uri)
        assert build_s3_uri(bucket, key) == uri


def _not_found_error(operation: str) -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, operation
    )


class TestObjectExists:
    def test_true_when_head_object_succeeds(self):
        s3 = MagicMock()
        with patch("overture_core.s3.boto3.client", return_value=s3):
            assert object_exists("bucket", "key") is True

    def test_false_on_404(self):
        s3 = MagicMock()
        s3.head_object.side_effect = _not_found_error("HeadObject")
        with patch("overture_core.s3.boto3.client", return_value=s3):
            assert object_exists("bucket", "key") is False

    def test_reraises_other_client_errors(self):
        s3 = MagicMock()
        s3.head_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject"
        )
        with patch("overture_core.s3.boto3.client", return_value=s3):
            with pytest.raises(botocore.exceptions.ClientError):
                object_exists("bucket", "key")


class TestDeleteObject:
    def test_deletes_and_returns_true_when_present(self):
        s3 = MagicMock()
        with patch("overture_core.s3.boto3.client", return_value=s3):
            assert delete_object("bucket", "key") is True
        s3.delete_object.assert_called_once_with(Bucket="bucket", Key="key")

    def test_returns_false_when_absent(self):
        s3 = MagicMock()
        s3.head_object.side_effect = _not_found_error("HeadObject")
        with patch("overture_core.s3.boto3.client", return_value=s3):
            assert delete_object("bucket", "key") is False
        s3.delete_object.assert_not_called()


class TestWriteMarker:
    def test_writes_empty_object(self):
        s3 = MagicMock()
        with patch("overture_core.s3.boto3.client", return_value=s3):
            write_marker("bucket", "path/_SUCCESS")
        s3.put_object.assert_called_once_with(
            Bucket="bucket", Key="path/_SUCCESS", Body=b""
        )


class TestCopyPrefix:
    def test_copies_all_objects_under_prefix(self):
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "src/a.parquet", "Size": 100},
                    {"Key": "src/nested/b.parquet", "Size": 200},
                    {"Key": "src/dir-marker/", "Size": 0},
                ]
            }
        ]
        with patch("overture_core.s3.boto3.client", return_value=s3):
            result = copy_prefix("source-bucket", "src", "dest-bucket", "dst")

        assert result == CopyResult(files_copied=2, total_bytes=300)
        assert s3.copy.call_count == 2
        s3.copy.assert_any_call(
            CopySource={"Bucket": "source-bucket", "Key": "src/a.parquet"},
            Bucket="dest-bucket",
            Key="dst/a.parquet",
            Config=s3.copy.call_args_list[0].kwargs["Config"],
        )

    def test_no_objects_returns_zero_result(self):
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [{}]
        with patch("overture_core.s3.boto3.client", return_value=s3):
            result = copy_prefix("source-bucket", "src", "dest-bucket", "dst")
        assert result == CopyResult(files_copied=0, total_bytes=0)


class TestDeletePrefix:
    def test_deletes_all_objects_under_prefix(self):
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "src/a.parquet"}, {"Key": "src/b.parquet"}]}
        ]
        with patch("overture_core.s3.boto3.client", return_value=s3):
            deleted = delete_prefix("bucket", "src")

        assert deleted == 2
        s3.delete_objects.assert_called_once_with(
            Bucket="bucket",
            Delete={"Objects": [{"Key": "src/a.parquet"}, {"Key": "src/b.parquet"}]},
        )

    def test_no_objects_skips_delete_call(self):
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [{}]
        with patch("overture_core.s3.boto3.client", return_value=s3):
            deleted = delete_prefix("bucket", "src")
        assert deleted == 0
        s3.delete_objects.assert_not_called()
