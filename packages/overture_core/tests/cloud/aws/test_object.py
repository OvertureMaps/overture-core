"""Unit tests for S3 object and prefix utilities."""

from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest

from overture_core.cloud.aws.object import (
    ConditionalWriteConflictError,
    ConditionalWriteUnsupportedError,
    CopyResult,
    build_s3_uri,
    bucket_writable,
    copy_prefix,
    delete_object,
    delete_prefix,
    get_object_bytes,
    get_object_with_etag,
    list_common_prefixes,
    object_exists,
    parse_s3_uri,
    prefix_exists,
    put_object,
    put_object_if_absent,
    put_object_if_unchanged,
    update_object,
    upload_directory,
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

    def test_rejects_empty_bucket(self):
        with pytest.raises(ValueError, match="Not an S3 URI"):
            parse_s3_uri("s3:///key")


class TestBuildS3Uri:
    def test_builds_uri(self):
        assert build_s3_uri("my-bucket", "some/prefix/file.txt") == (
            "s3://my-bucket/some/prefix/file.txt"
        )

    def test_strips_leading_slash_from_key(self):
        assert build_s3_uri("my-bucket", "/file.txt") == "s3://my-bucket/file.txt"

    def test_omits_trailing_slash_for_empty_key(self):
        assert build_s3_uri("my-bucket", "") == "s3://my-bucket"

    def test_round_trips_with_parse(self):
        uri = "s3://my-bucket/some/prefix/file.txt"
        bucket, key = parse_s3_uri(uri)
        assert build_s3_uri(bucket, key) == uri

    def test_round_trips_bucket_only(self):
        uri = "s3://my-bucket"
        bucket, key = parse_s3_uri(uri)
        assert build_s3_uri(bucket, key) == uri


def _not_found_error(operation: str) -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, operation
    )


class TestObjectExists:
    def test_true_when_head_object_succeeds(self):
        s3 = MagicMock()
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert object_exists("bucket", "key") is True

    def test_false_on_404(self):
        s3 = MagicMock()
        s3.head_object.side_effect = _not_found_error("HeadObject")
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert object_exists("bucket", "key") is False

    def test_reraises_other_client_errors(self):
        s3 = MagicMock()
        s3.head_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject"
        )
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            with pytest.raises(botocore.exceptions.ClientError):
                object_exists("bucket", "key")


class TestPrefixExists:
    def test_true_when_objects_present(self):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {"KeyCount": 1}
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert prefix_exists("bucket", "some/prefix") is True
        s3.list_objects_v2.assert_called_once_with(
            Bucket="bucket", Prefix="some/prefix", MaxKeys=1
        )

    def test_false_when_no_objects(self):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {"KeyCount": 0}
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert prefix_exists("bucket", "empty/prefix") is False

    def test_false_when_key_count_missing(self):
        s3 = MagicMock()
        s3.list_objects_v2.return_value = {}
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert prefix_exists("bucket", "empty/prefix") is False


class TestBucketWritable:
    def test_true_on_successful_put_and_delete(self):
        s3 = MagicMock()
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert bucket_writable("bucket") is True
        put_key = s3.put_object.call_args.kwargs["Key"]
        assert put_key.startswith(".overture_core_write_test/")
        s3.put_object.assert_called_once_with(Bucket="bucket", Key=put_key, Body=b"")
        s3.delete_object.assert_called_once_with(Bucket="bucket", Key=put_key)

    def test_default_key_is_unique_per_call(self):
        s3 = MagicMock()
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            bucket_writable("bucket")
            bucket_writable("bucket")
        first_key = s3.put_object.call_args_list[0].kwargs["Key"]
        second_key = s3.put_object.call_args_list[1].kwargs["Key"]
        assert first_key != second_key

    def test_uses_custom_test_key(self):
        s3 = MagicMock()
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            bucket_writable("bucket", test_key="custom/probe")
        s3.put_object.assert_called_once_with(
            Bucket="bucket", Key="custom/probe", Body=b""
        )

    def test_false_on_put_access_denied(self):
        s3 = MagicMock()
        s3.put_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "PutObject"
        )
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert bucket_writable("bucket") is False
        s3.delete_object.assert_not_called()

    def test_false_on_delete_access_denied_but_cleans_up_via_finally(self):
        s3 = MagicMock()
        s3.delete_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "DeleteObject"
        )
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert bucket_writable("bucket") is False
        # First delete attempt (in the try) raised; finally retries cleanup once.
        assert s3.delete_object.call_count == 2

    def test_finally_swallows_cleanup_failure(self):
        s3 = MagicMock()
        s3.delete_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "DeleteObject"
        )
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            # Should not raise even though every delete_object call fails.
            assert bucket_writable("bucket") is False


class TestDeleteObject:
    def test_calls_delete_object(self):
        s3 = MagicMock()
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert delete_object("bucket", "key") is None
        s3.delete_object.assert_called_once_with(Bucket="bucket", Key="key")
        # No pre-check: DeleteObject is idempotent, so a missing key costs
        # exactly one API call, not a head_object round trip first.
        s3.head_object.assert_not_called()


class TestWriteMarker:
    def test_writes_empty_object(self):
        s3 = MagicMock()
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            write_marker("bucket", "path/_SUCCESS")
        s3.put_object.assert_called_once_with(
            Bucket="bucket", Key="path/_SUCCESS", Body=b""
        )


class TestGetObjectBytes:
    def test_returns_body_bytes(self):
        s3 = MagicMock()
        s3.get_object.return_value = {"Body": MagicMock(read=lambda: b"contents")}
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert get_object_bytes("bucket", "key") == b"contents"
        s3.get_object.assert_called_once_with(Bucket="bucket", Key="key")

    def test_none_on_missing_key(self):
        s3 = MagicMock()
        s3.get_object.side_effect = _not_found_error("GetObject")
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert get_object_bytes("bucket", "missing") is None

    def test_reraises_other_client_errors(self):
        s3 = MagicMock()
        s3.get_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}}, "GetObject"
        )
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            with pytest.raises(botocore.exceptions.ClientError):
                get_object_bytes("bucket", "key")


class TestPutObject:
    def test_writes_body_without_content_type(self):
        s3 = MagicMock()
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            put_object("bucket", "key", b"contents")
        s3.put_object.assert_called_once_with(
            Bucket="bucket", Key="key", Body=b"contents"
        )

    def test_writes_body_with_content_type(self):
        s3 = MagicMock()
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            put_object("bucket", "key", b"{}", content_type="application/json")
        s3.put_object.assert_called_once_with(
            Bucket="bucket",
            Key="key",
            Body=b"{}",
            ContentType="application/json",
        )


def _conflict_error(code="PreconditionFailed"):
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": "conflict"}}, "PutObject"
    )


def _s3_with_conditional_support(supported=True):
    s3 = MagicMock()
    members = {"IfMatch": object()} if supported else {}
    s3.meta.service_model.operation_model.return_value.input_shape.members = members
    return s3


class TestGetObjectWithEtag:
    def test_returns_body_and_etag(self):
        s3 = MagicMock()
        s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: b"x"),
            "ETag": '"abc"',
        }
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert get_object_with_etag("b", "k") == (b"x", '"abc"')

    def test_missing_returns_none_pair(self):
        s3 = MagicMock()
        s3.get_object.side_effect = _not_found_error("GetObject")
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert get_object_with_etag("b", "k") == (None, None)

    def test_reraises_other_errors(self):
        s3 = MagicMock()
        s3.get_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}}, "GetObject"
        )
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            with pytest.raises(botocore.exceptions.ClientError):
                get_object_with_etag("b", "k")


class TestPutObjectIfUnchanged:
    def test_if_match_with_etag(self):
        s3 = _s3_with_conditional_support()
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert put_object_if_unchanged("b", "k", b"x", '"e"', "text/plain")
        s3.put_object.assert_called_once_with(
            Bucket="b", Key="k", Body=b"x", IfMatch='"e"', ContentType="text/plain"
        )

    def test_if_none_match_when_no_etag(self):
        s3 = _s3_with_conditional_support()
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert put_object_if_unchanged("b", "k", b"x", None)
        s3.put_object.assert_called_once_with(
            Bucket="b", Key="k", Body=b"x", IfNoneMatch="*"
        )

    @pytest.mark.parametrize(
        "code", ["PreconditionFailed", "412", "ConditionalRequestConflict", "409"]
    )
    @pytest.mark.parametrize("etag", ['"e"', None])
    def test_conflict_returns_false(self, code, etag):
        s3 = _s3_with_conditional_support()
        s3.put_object.side_effect = _conflict_error(code)
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert put_object_if_unchanged("b", "k", b"x", etag) is False

    @pytest.mark.parametrize("code", ["404", "NoSuchKey"])
    def test_deleted_since_read_returns_false(self, code):
        s3 = _s3_with_conditional_support()
        s3.put_object.side_effect = _conflict_error(code)
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert put_object_if_unchanged("b", "k", b"x", '"e"') is False

    def test_missing_on_create_only_write_is_reraised(self):
        s3 = _s3_with_conditional_support()
        s3.put_object.side_effect = _conflict_error("NoSuchBucket")
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            with pytest.raises(botocore.exceptions.ClientError):
                put_object_if_unchanged("b", "k", b"x", None)

    def test_reraises_other_client_errors(self):
        s3 = _s3_with_conditional_support()
        s3.put_object.side_effect = _conflict_error("AccessDenied")
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            with pytest.raises(botocore.exceptions.ClientError):
                put_object_if_unchanged("b", "k", b"x", '"e"')

    def test_old_boto3_raises_clear_error(self):
        s3 = _s3_with_conditional_support(supported=False)
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            with pytest.raises(ConditionalWriteUnsupportedError, match="IfMatch"):
                put_object_if_unchanged("b", "k", b"x", '"e"')
        s3.put_object.assert_not_called()


class TestPutObjectIfAbsent:
    def test_creates_with_if_none_match(self):
        s3 = _s3_with_conditional_support()
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert put_object_if_absent("b", "k", b"x")
        s3.put_object.assert_called_once_with(
            Bucket="b", Key="k", Body=b"x", IfNoneMatch="*"
        )

    def test_false_when_object_exists(self):
        s3 = _s3_with_conditional_support()
        s3.put_object.side_effect = _conflict_error()
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            assert put_object_if_absent("b", "k", b"x") is False


class TestUpdateObject:
    def test_reapplies_fn_after_concurrent_write(self):
        calls = []

        def fn(body):
            calls.append(body)
            return (body or b"") + b"+"

        reads = [(b"a", '"1"'), (b"ab", '"2"')]
        with (
            patch(
                "overture_core.cloud.aws.object.get_object_with_etag",
                side_effect=reads,
            ),
            patch(
                "overture_core.cloud.aws.object.put_object_if_unchanged",
                side_effect=[False, True],
            ) as put,
            patch("overture_core.cloud.aws.object.time.sleep"),
        ):
            assert update_object("b", "k", fn) == b"ab+"
        assert calls == [b"a", b"ab"]
        assert put.call_count == 2

    def test_gives_up_after_max_attempts(self):
        with (
            patch(
                "overture_core.cloud.aws.object.get_object_with_etag",
                return_value=(None, None),
            ),
            patch(
                "overture_core.cloud.aws.object.put_object_if_unchanged",
                return_value=False,
            ) as put,
            patch("overture_core.cloud.aws.object.time.sleep"),
        ):
            with pytest.raises(ConditionalWriteConflictError):
                update_object("b", "k", lambda body: b"x", max_attempts=3)
        assert put.call_count == 3


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
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
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
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            result = copy_prefix("source-bucket", "src", "dest-bucket", "dst")
        assert result == CopyResult(files_copied=0, total_bytes=0)

    def test_empty_prefixes_copy_whole_bucket(self):
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "a.parquet", "Size": 100}]}
        ]
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            result = copy_prefix("source-bucket", "", "dest-bucket", "")

        assert result == CopyResult(files_copied=1, total_bytes=100)
        s3.get_paginator.return_value.paginate.assert_called_once_with(
            Bucket="source-bucket", Prefix=""
        )
        s3.copy.assert_called_once_with(
            CopySource={"Bucket": "source-bucket", "Key": "a.parquet"},
            Bucket="dest-bucket",
            Key="a.parquet",
            Config=s3.copy.call_args.kwargs["Config"],
        )


class TestDeletePrefix:
    def test_deletes_all_objects_under_prefix(self):
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "src/a.parquet"}, {"Key": "src/b.parquet"}]}
        ]
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            deleted = delete_prefix("bucket", "src")

        assert deleted == 2
        s3.delete_objects.assert_called_once_with(
            Bucket="bucket",
            Delete={"Objects": [{"Key": "src/a.parquet"}, {"Key": "src/b.parquet"}]},
        )

    def test_no_objects_skips_delete_call(self):
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [{}]
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            deleted = delete_prefix("bucket", "src")
        assert deleted == 0
        s3.delete_objects.assert_not_called()

    def test_empty_prefix_deletes_whole_bucket(self):
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "a.parquet"}]}
        ]
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            deleted = delete_prefix("bucket", "")

        assert deleted == 1
        s3.get_paginator.return_value.paginate.assert_called_once_with(
            Bucket="bucket", Prefix=""
        )


class TestListCommonPrefixes:
    def test_returns_immediate_segment_names(self):
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "root/version=1/"},
                    {"Prefix": "root/version=2/"},
                ]
            }
        ]
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            names = list_common_prefixes("bucket", "root/")

        assert names == ["version=1", "version=2"]
        s3.get_paginator.return_value.paginate.assert_called_once_with(
            Bucket="bucket", Prefix="root/", Delimiter="/"
        )

    def test_paginates_across_pages(self):
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [
            {"CommonPrefixes": [{"Prefix": "root/a/"}]},
            {"CommonPrefixes": [{"Prefix": "root/b/"}]},
        ]
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            names = list_common_prefixes("bucket", "root/")

        assert names == ["a", "b"]

    def test_empty_prefix_returns_no_matches(self):
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [{}]
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            names = list_common_prefixes("bucket", "empty/")
        assert names == []

    def test_skips_folder_placeholder(self):
        s3 = MagicMock()
        s3.get_paginator.return_value.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "root/$folder$/"},
                    {"Prefix": "root/real/"},
                ]
            }
        ]
        with patch("overture_core.cloud.aws.object.boto3.client", return_value=s3):
            names = list_common_prefixes("bucket", "root/")
        assert names == ["real"]


class TestUploadDirectory:
    @pytest.fixture()
    def s3_client(self):
        mock_client = MagicMock()
        with patch(
            "overture_core.cloud.aws.object.boto3.client", return_value=mock_client
        ):
            yield mock_client

    def test_uploads_all_files(self, tmp_path, s3_client):
        (tmp_path / "sub").mkdir()
        (tmp_path / "a.txt").write_text("a")
        (tmp_path / "sub" / "b.txt").write_text("b")

        result = upload_directory(str(tmp_path), "my-bucket", prefix="prefix")

        assert sorted(result) == [
            "s3://my-bucket/prefix/a.txt",
            "s3://my-bucket/prefix/sub/b.txt",
        ]
        assert s3_client.upload_file.call_count == 2

    def test_raises_on_failure(self, tmp_path, s3_client):
        (tmp_path / "a.txt").write_text("a")
        s3_client.upload_file.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            upload_directory(str(tmp_path), "my-bucket")

    def test_raises_on_missing_directory(self, tmp_path, s3_client):
        missing = tmp_path / "does-not-exist"

        with pytest.raises(NotADirectoryError, match="Not a directory"):
            upload_directory(str(missing), "my-bucket")
        s3_client.upload_file.assert_not_called()
