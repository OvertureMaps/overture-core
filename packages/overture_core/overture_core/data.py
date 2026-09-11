"""Cloud storage path-building dataclasses for data sync operations.

DataLocation composes a bucket/container plus prefix, partition, and suffix
components (or an arbitrary full_path) into S3/Azure paths. DatasyncSpec
pairs a source and destination DataLocation for a single named data sync.
"""

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from overture_core.cloud.cloud import CloudProvider, Partition


@dataclass
class DataLocation:
    """Cloud storage location configuration for data sync operations.

    Supports both AWS S3 and Azure Blob Storage. Builds storage paths from components:
    prefix, partition, suffix, and optional data directory.
    Supports both Hive-style partitioning (key=value) and simple partition values.

    Attributes:
        bucket: S3 bucket name or Azure Blob container name
            (can be Jinja2 template, e.g., "{{ var.value.my_bucket }}")
        full_path: Complete path as a plain string. If set, ignores prefix/partition/suffix components.
            Useful for arbitrary path structures or multiple partitions.
            Example: "release/v1.0.0/us-west/2024-08-01/data"
        prefix: Base path within bucket or container
        partition: Partition object containing key, delimiter, and value
        suffix: Additional path after partition
        cloud_provider: Cloud provider type (AWS or AZURE). Defaults to AWS.
        storage_account: Azure Storage account name. Required when cloud_provider is AZURE.
        data_dir: If True, appends '/data/' to the path (only applies when using components, not full_path)

    Path Structure (Component Mode):
        prefix/[partition]/suffix/[data/]

        With Hive-style:      prefix/ds=2024-08-01/suffix/data/
        Without key/delimiter: prefix/2024-08-01/suffix/data/

    Path Structure (Full Path Mode):
        full_path/[data/]

    Examples:
        # AWS S3 with Hive-style partition
        DataLocation(
            bucket="my-bucket",
            prefix="feeds/overture",
            partition=Partition(key="ds", delimiter="=", value="2024-08-01"),
            data_dir=True
        ).build_path -> "feeds/overture/ds=2024-08-01/data/"

        # AWS S3 with simple partition and suffix
        DataLocation(
            bucket="releases",
            prefix="v1",
            partition=Partition(value="Run_12345"),
            suffix="changelog",
            data_dir=False
        ).build_path -> "v1/Run_12345/changelog/"

        # Full path mode (arbitrary structure, multi-partition, etc.)
        DataLocation(
            bucket="releases",
            full_path="release/v1.0.0/us-west/2024-08-01/data"
        ).build_path -> "release/v1.0.0/us-west/2024-08-01/data/"

        # Azure Blob Storage
        DataLocation(
            bucket="my-container",  # container name
            prefix="feeds/overture",
            partition=Partition(value="2024-08-01"),
            cloud_provider=CloudProvider.AZURE,
            storage_account="mystorageaccount",
            data_dir=True
        ).build_path -> "feeds/overture/2024-08-01/data/"
    """

    bucket: str
    full_path: str = ""
    prefix: str = ""
    partition: Partition | None = None
    suffix: str = ""
    cloud_provider: CloudProvider = CloudProvider.AWS
    storage_account: str = ""
    data_dir: bool = False

    def __post_init__(self):
        """Validate field requirements based on cloud provider and path mode."""
        # Validate that full_path and component fields are not mixed
        component_fields_used = any([self.prefix, self.partition, self.suffix])
        if self.full_path and component_fields_used:
            raise ValueError(
                "Cannot use full_path together with prefix/partition/suffix. "
                "Use either full_path OR component fields, not both."
            )

        # Azure requires storage_account
        if self.cloud_provider == CloudProvider.AZURE and not self.storage_account:
            raise ValueError("storage_account is required when cloud_provider is AZURE")

    @property
    def build_path(self) -> str:
        """Construct the full S3 path based on the configuration.

        Uses full_path if provided, otherwise builds from components.
        """
        # Full path mode: use the provided path directly
        if self.full_path:
            path = PurePosixPath(self.full_path.strip("/"))
            # Add data directory if needed
            if self.data_dir:
                path = path / "data"
            return str(path) + "/"

        # Component mode: build from prefix/partition/suffix (existing logic)
        # Start with an empty path
        path = PurePosixPath()

        # Add prefix (strip leading/trailing slashes to avoid absolute paths)
        if self.prefix:
            path = path / self.prefix.strip("/")

        # Add partition component
        if self.partition and not self.partition.is_empty():
            partition_part = self.partition.build()
            path = path / partition_part.strip("/")

        # Add suffix (strip to avoid absolute paths)
        if self.suffix:
            path = path / self.suffix.strip("/")

        # Add data directory if needed
        if self.data_dir:
            path = path / "data"

        return str(path) + "/"

    def build_path_with_namespace(self, namespace: str = "") -> str:
        """
        Construct the full S3 path with an optional namespace prefix.

        Args:
            namespace: Optional namespace to prepend to the path (e.g., username for dev environments)

        Returns:
            Full S3 path with namespace prepended if provided

        Examples:
            # Without namespace
            build_path_with_namespace("") → "feeds/overture/ds=2024-08-01/data/"

            # With namespace
            build_path_with_namespace("joebloggs") → "joebloggs/feeds/overture/ds=2024-08-01/data/"
        """
        base_path = self.build_path
        if namespace:
            return f"{namespace.strip('/')}/" + base_path
        return base_path

    def build_s3_uri(self, namespace: str = "") -> str:
        """
        Construct the full S3 URI including the s3:// scheme and bucket name.

        Args:
            namespace: Optional namespace to prepend to the path (e.g., username for dev environments)

        Returns:
            Full S3 URI

        Examples:
            # Without namespace
            build_s3_uri("") → "s3://my-bucket/feeds/overture/ds=2024-08-01/data/"

            # With namespace
            build_s3_uri("joebloggs") → "s3://my-bucket/joebloggs/feeds/overture/ds=2024-08-01/data/"
        """
        return f"s3://{self.bucket}/{self.build_path_with_namespace(namespace)}"

    def build_success_file_path(self, namespace: str = "") -> str:
        """
        Construct the S3 path for a success file at partition level (not in data/ directory).

        When data_dir=True, the success file is created at the partition level alongside
        the data/ directory, not inside it.

        Args:
            namespace: Optional namespace to prepend to the path

        Returns:
            Full S3 path for success file without trailing slash

        Examples:
            # With data_dir=True
            build_success_file_path("joebloggs") → "joebloggs/feeds/overture/ds=2024-08-01"

            # With data_dir=False
            build_success_file_path("joebloggs") → "joebloggs/feeds/overture/ds=2024-08-01"
        """
        path_with_namespace = self.build_path_with_namespace(namespace).rstrip("/")

        # Remove /data suffix if present (when data_dir=True)
        if path_with_namespace.endswith("/data"):
            path_with_namespace = path_with_namespace[:-5]

        return path_with_namespace


@dataclass
class DatasyncSpec:
    """Defines the specification for a data synchronization operation, including source and destination locations"""

    name: str
    ds_param_description: str
    source: DataLocation
    destination: DataLocation
    ds_param_default: Any = ""

    @property
    def param_name(self) -> str:
        """Get the parameter name for this dataset."""
        return f"{self.name}_ds"

    @property
    def param_title(self) -> str:
        """Get the parameter title for this dataset."""
        return f"INPUT: {self.name} DS"
