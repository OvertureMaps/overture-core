"""Unit tests for CloudProvider and Partition."""

from overture_core.cloud import CloudProvider, Partition


class TestCloudProvider:
    def test_aws_value(self):
        assert CloudProvider.AWS.value == "aws"

    def test_azure_value(self):
        assert CloudProvider.AZURE.value == "azure"


class TestPartition:
    def test_hive_style_build(self):
        partition = Partition(key="ds", delimiter="=", value="2024-08-01")
        assert partition.build() == "ds=2024-08-01"
        assert partition.is_hive_style() is True
        assert partition.is_empty() is False

    def test_simple_build(self):
        partition = Partition(value="2024-08-01")
        assert partition.build() == "2024-08-01"
        assert partition.is_hive_style() is False
        assert partition.is_empty() is False

    def test_empty_build(self):
        partition = Partition()
        assert partition.build() == ""
        assert partition.is_hive_style() is False
        assert partition.is_empty() is True

    def test_key_without_delimiter_is_not_hive_style(self):
        partition = Partition(key="ds", value="2024-08-01")
        assert partition.is_hive_style() is False
        # Falls back to a simple partition since key/delimiter isn't a complete pair.
        assert partition.build() == "2024-08-01"
