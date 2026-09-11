"""Tests for DataLocation/DatasyncSpec path-building dataclasses."""

import pytest

from overture_core.cloud.cloud import CloudProvider, Partition
from overture_core.data import DataLocation, DatasyncSpec


class TestDataLocationValidation:
    def test_rejects_full_path_with_prefix(self):
        with pytest.raises(ValueError, match="Cannot use full_path"):
            DataLocation(bucket="b", full_path="x", prefix="y")

    def test_rejects_full_path_with_partition(self):
        with pytest.raises(ValueError, match="Cannot use full_path"):
            DataLocation(bucket="b", full_path="x", partition=Partition(value="v"))

    def test_rejects_full_path_with_suffix(self):
        with pytest.raises(ValueError, match="Cannot use full_path"):
            DataLocation(bucket="b", full_path="x", suffix="y")

    def test_azure_requires_storage_account(self):
        with pytest.raises(ValueError, match="storage_account is required"):
            DataLocation(bucket="b", cloud_provider=CloudProvider.AZURE)

    def test_azure_with_storage_account_ok(self):
        loc = DataLocation(
            bucket="b", cloud_provider=CloudProvider.AZURE, storage_account="acct"
        )
        assert loc.storage_account == "acct"


class TestBuildPathComponentMode:
    def test_hive_style_partition_with_data_dir(self):
        loc = DataLocation(
            bucket="my-bucket",
            prefix="feeds/overture",
            partition=Partition(key="ds", delimiter="=", value="2024-08-01"),
            data_dir=True,
        )
        assert loc.build_path == "feeds/overture/ds=2024-08-01/data/"

    def test_simple_partition_with_suffix(self):
        loc = DataLocation(
            bucket="releases",
            prefix="v1",
            partition=Partition(value="Run_12345"),
            suffix="changelog",
            data_dir=False,
        )
        assert loc.build_path == "v1/Run_12345/changelog/"

    def test_no_components_returns_dot_slash(self):
        # PurePosixPath() stringifies to "." when empty, so this is a quirk
        # of the empty-path case, not a meaningful path.
        assert DataLocation(bucket="b").build_path == "./"

    def test_strips_leading_and_trailing_slashes(self):
        loc = DataLocation(bucket="b", prefix="/a/", suffix="/b/")
        assert loc.build_path == "a/b/"

    def test_empty_partition_is_skipped(self):
        loc = DataLocation(bucket="b", prefix="a", partition=Partition())
        assert loc.build_path == "a/"


class TestBuildPathFullPathMode:
    def test_full_path_alone(self):
        loc = DataLocation(
            bucket="releases", full_path="release/v1.0.0/us-west/2024-08-01/data"
        )
        assert loc.build_path == "release/v1.0.0/us-west/2024-08-01/data/"

    def test_full_path_with_data_dir(self):
        loc = DataLocation(bucket="b", full_path="release/v1", data_dir=True)
        assert loc.build_path == "release/v1/data/"

    def test_full_path_strips_slashes(self):
        loc = DataLocation(bucket="b", full_path="/release/v1/")
        assert loc.build_path == "release/v1/"


class TestBuildPathWithNamespace:
    def test_without_namespace(self):
        loc = DataLocation(
            bucket="b",
            prefix="feeds/overture",
            partition=Partition(value="2024-08-01"),
            data_dir=True,
        )
        assert loc.build_path_with_namespace("") == "feeds/overture/2024-08-01/data/"

    def test_with_namespace(self):
        loc = DataLocation(
            bucket="b",
            prefix="feeds/overture",
            partition=Partition(value="2024-08-01"),
            data_dir=True,
        )
        assert (
            loc.build_path_with_namespace("joebloggs")
            == "joebloggs/feeds/overture/2024-08-01/data/"
        )

    def test_namespace_slashes_stripped(self):
        loc = DataLocation(bucket="b", prefix="a")
        assert loc.build_path_with_namespace("/ns/") == "ns/a/"


class TestBuildS3Uri:
    def test_without_namespace(self):
        loc = DataLocation(
            bucket="my-bucket",
            prefix="feeds/overture",
            partition=Partition(value="2024-08-01"),
            data_dir=True,
        )
        assert loc.build_s3_uri("") == "s3://my-bucket/feeds/overture/2024-08-01/data/"

    def test_with_namespace(self):
        loc = DataLocation(
            bucket="my-bucket",
            prefix="feeds/overture",
            partition=Partition(value="2024-08-01"),
            data_dir=True,
        )
        assert loc.build_s3_uri("joebloggs") == (
            "s3://my-bucket/joebloggs/feeds/overture/2024-08-01/data/"
        )


class TestBuildSuccessFilePath:
    def test_strips_data_suffix_when_data_dir(self):
        loc = DataLocation(
            bucket="b",
            prefix="feeds/overture",
            partition=Partition(value="2024-08-01"),
            data_dir=True,
        )
        assert (
            loc.build_success_file_path("joebloggs")
            == "joebloggs/feeds/overture/2024-08-01"
        )

    def test_no_data_suffix_when_data_dir_false(self):
        loc = DataLocation(
            bucket="b",
            prefix="feeds/overture",
            partition=Partition(value="2024-08-01"),
            data_dir=False,
        )
        assert (
            loc.build_success_file_path("joebloggs")
            == "joebloggs/feeds/overture/2024-08-01"
        )


class TestDatasyncSpec:
    def test_param_name_and_title(self):
        spec = DatasyncSpec(
            name="osm",
            ds_param_description="OSM dataset",
            source=DataLocation(bucket="src"),
            destination=DataLocation(bucket="dst"),
        )
        assert spec.param_name == "osm_ds"
        assert spec.param_title == "INPUT: osm DS"

    def test_default_param_default_is_empty_string(self):
        spec = DatasyncSpec(
            name="osm",
            ds_param_description="d",
            source=DataLocation(bucket="src"),
            destination=DataLocation(bucket="dst"),
        )
        assert spec.ds_param_default == ""

    def test_custom_param_default(self):
        spec = DatasyncSpec(
            name="osm",
            ds_param_description="d",
            source=DataLocation(bucket="src"),
            destination=DataLocation(bucket="dst"),
            ds_param_default="2024-08-01",
        )
        assert spec.ds_param_default == "2024-08-01"
