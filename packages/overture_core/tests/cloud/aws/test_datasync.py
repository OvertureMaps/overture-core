"""Unit tests for DataSync helpers."""

from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from overture_core.cloud.aws.datasync import (
    ObjectTagsMode,
    PreserveDeletedFilesMode,
    TaskExecutionSummary,
    VerifyMode,
    azure_blob_task_options,
    build_azure_blob_location_uri,
    describe_azure_blob_location,
    describe_task_execution_summary,
    find_location_arn,
    format_bytes,
    format_task_execution_summary,
    refresh_azure_blob_location,
    s3_task_options,
    summarize_task_execution,
)

AZURE_URI = "azure-blob://acct.blob.core.windows.net/container/release/v1/"


class TestTaskOptions:
    def test_s3_defaults(self):
        assert s3_task_options() == {
            "PreserveDeletedFiles": "REMOVE",
            "VerifyMode": "ONLY_FILES_TRANSFERRED",
        }

    def test_s3_custom(self):
        assert s3_task_options(PreserveDeletedFilesMode.PRESERVE, VerifyMode.NONE) == {
            "PreserveDeletedFiles": "PRESERVE",
            "VerifyMode": "NONE",
        }

    def test_azure_forces_preserve_and_no_tags(self):
        assert azure_blob_task_options(VerifyMode.POINT_IN_TIME_CONSISTENT) == {
            "PreserveDeletedFiles": "PRESERVE",
            "VerifyMode": "POINT_IN_TIME_CONSISTENT",
            "ObjectTags": "NONE",
        }

    def test_enum_values_match_botocore_service_model(self):
        """The SDK ships no enums, only model strings; keep ours in lockstep."""
        import botocore.session

        options = (
            botocore.session.get_session()
            .get_service_model("datasync")
            .shape_for("Options")
        )
        for name, enum in (
            ("VerifyMode", VerifyMode),
            ("PreserveDeletedFiles", PreserveDeletedFilesMode),
            ("ObjectTags", ObjectTagsMode),
        ):
            assert {m.value for m in enum} == set(options.members[name].enum)


class TestBuildAzureBlobLocationUri:
    def test_with_subdirectory(self):
        assert build_azure_blob_location_uri("acct", "container", "/release/v1/") == (
            AZURE_URI
        )

    def test_without_subdirectory(self):
        assert (
            build_azure_blob_location_uri("acct", "container")
            == "azure-blob://acct.blob.core.windows.net/container/"
        )


class TestFormatBytes:
    def test_units(self):
        assert format_bytes(512) == "512 B"
        assert format_bytes(2048) == "2.00 KB"
        assert format_bytes(3 * 1024**2) == "3.00 MB"
        assert format_bytes(5 * 1024**3) == "5.00 GB"


DESCRIPTION = {
    "Status": "SUCCESS",
    "FilesTransferred": 10,
    "FilesVerified": 10,
    "FilesSkipped": 1,
    "FilesDeleted": 0,
    "BytesTransferred": 2048,
    "BytesWritten": 2048,
    "Result": {
        "TotalDuration": 1500,
        "TransferDuration": 1000,
        "VerifyDuration": 500,
        "PrepareStatus": "SUCCESS",
        "TransferStatus": "SUCCESS",
        "VerifyStatus": "SUCCESS",
    },
}


class TestSummarizeTaskExecution:
    def test_full_description(self):
        summary = summarize_task_execution(DESCRIPTION)
        assert summary.status == "SUCCESS"
        assert summary.files_transferred == 10
        assert summary.total_duration_ms == 1500
        assert summary.bytes_transferred_human == "2.00 KB"
        assert summary.total_duration_human == "1.50s"
        assert summary.to_dict()["bytes_transferred_human"] == "2.00 KB"
        assert summary.to_dict()["verify_status"] == "SUCCESS"

    def test_empty_description_defaults(self):
        summary = summarize_task_execution({})
        assert summary == TaskExecutionSummary(
            status=None,
            files_transferred=0,
            files_verified=0,
            files_skipped=0,
            files_deleted=0,
            bytes_transferred=0,
            bytes_written=0,
            total_duration_ms=0,
            transfer_duration_ms=0,
            verify_duration_ms=0,
            prepare_status=None,
            transfer_status=None,
            verify_status=None,
        )

    def test_format_report(self):
        report = format_task_execution_summary(
            "my_dataset", summarize_task_execution(DESCRIPTION)
        )
        assert "DataSync Summary for: my_dataset" in report
        assert "Data Transferred: 2.00 KB (2,048 bytes)" in report
        assert "Duration: 1.50s" in report


class TestDescribeTaskExecutionSummary:
    def test_fetches_and_summarizes(self):
        ds = MagicMock()
        ds.describe_task_execution.return_value = DESCRIPTION
        with patch("overture_core.cloud.aws.datasync.boto3.client", return_value=ds):
            summary = describe_task_execution_summary("arn:exec", region="us-west-2")
        assert summary.files_transferred == 10
        ds.describe_task_execution.assert_called_once_with(TaskExecutionArn="arn:exec")


def _datasync_with_locations(*uris):
    ds = MagicMock()
    ds.get_paginator.return_value.paginate.return_value = [
        {
            "Locations": [
                {"LocationUri": uri, "LocationArn": f"arn:loc:{i}"}
                for i, uri in enumerate(uris)
            ]
        }
    ]
    return ds


class TestFindLocationArn:
    def test_found_ignoring_trailing_slash(self):
        ds = _datasync_with_locations("s3://other/", AZURE_URI)
        with patch("overture_core.cloud.aws.datasync.boto3.client", return_value=ds):
            assert find_location_arn(AZURE_URI.rstrip("/")) == "arn:loc:1"

    def test_not_found(self):
        ds = _datasync_with_locations("s3://other/")
        with patch("overture_core.cloud.aws.datasync.boto3.client", return_value=ds):
            assert find_location_arn(AZURE_URI) is None


class TestDescribeAzureBlobLocation:
    def test_returns_description(self):
        ds = _datasync_with_locations(AZURE_URI)
        ds.describe_location_azure_blob.return_value = {"AccessTier": "HOT"}
        with patch("overture_core.cloud.aws.datasync.boto3.client", return_value=ds):
            assert describe_azure_blob_location(AZURE_URI) == {"AccessTier": "HOT"}
        ds.describe_location_azure_blob.assert_called_once_with(LocationArn="arn:loc:0")

    def test_none_when_missing(self):
        ds = _datasync_with_locations()
        with patch("overture_core.cloud.aws.datasync.boto3.client", return_value=ds):
            assert describe_azure_blob_location(AZURE_URI) is None


class TestRefreshAzureBlobLocation:
    def _refresh(self, ds):
        with patch("overture_core.cloud.aws.datasync.boto3.client", return_value=ds):
            return refresh_azure_blob_location(
                "acct", "container", "/release/v1", "sv=tok", region="us-west-2"
            )

    def test_creates_when_missing(self):
        ds = _datasync_with_locations()
        ds.create_location_azure_blob.return_value = {"LocationArn": "arn:new"}
        assert self._refresh(ds) == "arn:new"
        ds.create_location_azure_blob.assert_called_once_with(
            ContainerUrl="https://acct.blob.core.windows.net/container",
            AuthenticationType="SAS",
            SasConfiguration={"Token": "sv=tok"},
            Subdirectory="release/v1/",
        )
        ds.update_location_azure_blob.assert_not_called()

    def test_refreshes_existing(self):
        ds = _datasync_with_locations(AZURE_URI)
        assert self._refresh(ds) == "arn:loc:0"
        ds.update_location_azure_blob.assert_called_once_with(
            LocationArn="arn:loc:0",
            AuthenticationType="SAS",
            SasConfiguration={"Token": "sv=tok"},
        )
        ds.create_location_azure_blob.assert_not_called()

    def test_recreates_when_update_fails(self):
        ds = _datasync_with_locations(AZURE_URI)
        ds.update_location_azure_blob.side_effect = ClientError(
            {"Error": {"Code": "InvalidRequestException", "Message": "stale"}},
            "UpdateLocationAzureBlob",
        )
        ds.create_location_azure_blob.return_value = {"LocationArn": "arn:new"}
        assert self._refresh(ds) == "arn:new"
        ds.delete_location.assert_called_once_with(LocationArn="arn:loc:0")
        ds.create_location_azure_blob.assert_called_once()
