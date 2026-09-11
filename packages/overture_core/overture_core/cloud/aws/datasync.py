"""AWS DataSync helpers built on boto3: task options, execution summaries, Azure Blob locations."""

import logging
from dataclasses import asdict, dataclass
from enum import Enum

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class PreserveDeletedFilesMode(str, Enum):
    """DataSync ``PreserveDeletedFiles`` option.

    ``REMOVE`` deletes destination files absent from the source (one-way
    mirror); ``PRESERVE`` leaves them alone (safe when several tasks write
    to one destination).
    """

    REMOVE = "REMOVE"
    PRESERVE = "PRESERVE"


class VerifyMode(str, Enum):
    """DataSync ``VerifyMode`` option for post-transfer integrity checks.

    ``ONLY_FILES_TRANSFERRED`` verifies just the transferred files (default);
    ``POINT_IN_TIME_CONSISTENT`` verifies the entire destination; ``NONE``
    skips verification, useful when transfer time dominates for large files.
    """

    ONLY_FILES_TRANSFERRED = "ONLY_FILES_TRANSFERRED"
    POINT_IN_TIME_CONSISTENT = "POINT_IN_TIME_CONSISTENT"
    NONE = "NONE"


def s3_task_options(
    preserve_deleted_files: PreserveDeletedFilesMode = PreserveDeletedFilesMode.REMOVE,
    verify_mode: VerifyMode = VerifyMode.ONLY_FILES_TRANSFERRED,
) -> dict:
    """Build the ``Options`` block for an S3-to-S3 DataSync task."""
    return {
        "PreserveDeletedFiles": preserve_deleted_files.value,
        "VerifyMode": verify_mode.value,
    }


def azure_blob_task_options(
    verify_mode: VerifyMode = VerifyMode.ONLY_FILES_TRANSFERRED,
) -> dict:
    """Build the ``Options`` block for an S3-to-Azure-Blob DataSync task.

    Two settings are forced regardless of caller preference because Azure
    Data Lake Storage Gen2 (hierarchical namespace) destinations reject them:

    - ``PreserveDeletedFiles=PRESERVE``: HNS directories must be empty before
      deletion, and DataSync surfaces the attempt as a hard failure
      (``DirectoryIsNotEmpty``) rather than a skip. See
      https://docs.aws.amazon.com/datasync/latest/userguide/creating-azure-blob-location.html#azure-blob-considerations-deleted-files
    - ``ObjectTags=NONE``: Enhanced mode defaults to preserving tags, which HNS
      accounts don't support ("Azure Blob Cannot Preserve Tags In Hierarchical
      Account").
    """
    return {
        "PreserveDeletedFiles": PreserveDeletedFilesMode.PRESERVE.value,
        "VerifyMode": verify_mode.value,
        "ObjectTags": "NONE",
    }


def build_azure_blob_location_uri(
    storage_account: str, container: str, subdirectory: str = ""
) -> str:
    """Build the ``azure-blob://`` LocationUri DataSync reports for a Blob location.

    Always trailing-slash terminated, matching what ``list_locations`` returns.
    """
    subdir = subdirectory.strip("/")
    subdir = f"{subdir}/" if subdir else ""
    return f"azure-blob://{storage_account}.blob.core.windows.net/{container}/{subdir}"


@dataclass(frozen=True)
class TaskExecutionSummary:
    """Headline statistics from a DataSync ``DescribeTaskExecution`` response."""

    status: str | None
    files_transferred: int
    files_verified: int
    files_skipped: int
    files_deleted: int
    bytes_transferred: int
    bytes_written: int
    total_duration_ms: int
    transfer_duration_ms: int
    verify_duration_ms: int
    prepare_status: str | None
    transfer_status: str | None
    verify_status: str | None

    @property
    def bytes_transferred_human(self) -> str:
        return format_bytes(self.bytes_transferred)

    @property
    def total_duration_human(self) -> str:
        return f"{self.total_duration_ms / 1000:.2f}s"

    def to_dict(self) -> dict:
        data = asdict(self)
        data["bytes_transferred_human"] = self.bytes_transferred_human
        data["total_duration_human"] = self.total_duration_human
        return data


def format_bytes(size: int) -> str:
    """Render a byte count as a human-readable B/KB/MB/GB string (binary units)."""
    for threshold, unit in ((1 << 30, "GB"), (1 << 20, "MB"), (1 << 10, "KB")):
        if size >= threshold:
            return f"{size / threshold:.2f} {unit}"
    return f"{size} B"


def summarize_task_execution(description: dict) -> TaskExecutionSummary:
    """Reduce a ``DescribeTaskExecution`` response to a :class:`TaskExecutionSummary`."""
    result = description.get("Result", {})
    return TaskExecutionSummary(
        status=description.get("Status"),
        files_transferred=description.get("FilesTransferred", 0),
        files_verified=description.get("FilesVerified", 0),
        files_skipped=description.get("FilesSkipped", 0),
        files_deleted=description.get("FilesDeleted", 0),
        bytes_transferred=description.get("BytesTransferred", 0),
        bytes_written=description.get("BytesWritten", 0),
        total_duration_ms=result.get("TotalDuration", 0),
        transfer_duration_ms=result.get("TransferDuration", 0),
        verify_duration_ms=result.get("VerifyDuration", 0),
        prepare_status=result.get("PrepareStatus"),
        transfer_status=result.get("TransferStatus"),
        verify_status=result.get("VerifyStatus"),
    )


def format_task_execution_summary(name: str, summary: TaskExecutionSummary) -> str:
    """Render *summary* as a multi-line report suitable for task logs."""
    rule = "=" * 80
    return "\n".join(
        [
            rule,
            f"DataSync Summary for: {name}",
            rule,
            f"Status: {summary.status}",
            f"Files Transferred: {summary.files_transferred}",
            f"Files Verified: {summary.files_verified}",
            f"Files Skipped: {summary.files_skipped}",
            f"Files Deleted: {summary.files_deleted}",
            f"Data Transferred: {summary.bytes_transferred_human} ({summary.bytes_transferred:,} bytes)",
            f"Duration: {summary.total_duration_human}",
            f"Transfer Status: {summary.transfer_status}",
            f"Verify Status: {summary.verify_status}",
            rule,
        ]
    )


def describe_task_execution_summary(
    task_execution_arn: str, region: str | None = None
) -> TaskExecutionSummary:
    """Fetch a task execution from DataSync and summarize it."""
    ds = boto3.client("datasync", region_name=region)
    response = ds.describe_task_execution(TaskExecutionArn=task_execution_arn)
    return summarize_task_execution(response)


def find_location_arn(location_uri: str, region: str | None = None) -> str | None:
    """Return the ARN of the DataSync location whose URI matches, or ``None``.

    Trailing slashes are ignored on both sides.
    """
    ds = boto3.client("datasync", region_name=region)
    target = location_uri.rstrip("/")
    for page in ds.get_paginator("list_locations").paginate():
        for loc in page.get("Locations", []):
            if loc.get("LocationUri", "").rstrip("/") == target:
                return loc["LocationArn"]
    return None


def describe_azure_blob_location(
    location_uri: str, region: str | None = None
) -> dict | None:
    """Return ``DescribeLocationAzureBlob`` for the location at *location_uri*.

    Returns ``None`` if no location matches. Handy in failure callbacks for
    debugging ``UnableToReadDestination``-style errors without the console.
    """
    arn = find_location_arn(location_uri, region=region)
    if arn is None:
        return None
    ds = boto3.client("datasync", region_name=region)
    return ds.describe_location_azure_blob(LocationArn=arn)


def refresh_azure_blob_location(
    storage_account: str,
    container: str,
    subdirectory: str,
    sas_token: str,
    region: str | None = None,
) -> str:
    """Idempotently create or refresh an Azure Blob DataSync location; return its ARN.

    Looks for an existing location whose URI matches the (account, container,
    subdirectory) tuple. If found, its SAS is refreshed with *sas_token*; if the
    update fails (a stale or broken location), the location is deleted and
    recreated. Otherwise a new location is created.

    Args:
        storage_account: Azure Storage account name.
        container: Blob container name.
        subdirectory: Path within the container; may be empty.
        sas_token: SAS token DataSync will use to write. The caller owns how
            it's fetched (e.g. :func:`overture_core.cloud.aws.secrets.get_secret_string`).
        region: Region of the DataSync agentless location. Defaults to boto3's own resolution.
    """
    subdir = subdirectory.strip("/")
    subdir = f"{subdir}/" if subdir else ""
    expected_uri = build_azure_blob_location_uri(storage_account, container, subdir)
    sas_config = {"Token": sas_token}

    ds = boto3.client("datasync", region_name=region)
    existing_arn = find_location_arn(expected_uri, region=region)
    if existing_arn is not None:
        try:
            ds.update_location_azure_blob(
                LocationArn=existing_arn,
                AuthenticationType="SAS",
                SasConfiguration=sas_config,
            )
            logger.info("Refreshed SAS on Azure location %s", existing_arn)
            return existing_arn
        except ClientError as exc:
            logger.warning(
                "update_location_azure_blob failed for %s (%s); recreating",
                existing_arn,
                exc,
            )
            ds.delete_location(LocationArn=existing_arn)

    response = ds.create_location_azure_blob(
        ContainerUrl=f"https://{storage_account}.blob.core.windows.net/{container}",
        AuthenticationType="SAS",
        SasConfiguration=sas_config,
        Subdirectory=subdir,
    )
    logger.info(
        "Created Azure location %s for %s", response["LocationArn"], expected_uri
    )
    return response["LocationArn"]
