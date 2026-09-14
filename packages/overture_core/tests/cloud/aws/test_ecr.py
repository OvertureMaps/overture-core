"""Unit tests for ECR image URI helpers."""

from unittest.mock import MagicMock, patch

import pytest

from overture_core.cloud.aws.ecr import (
    build_ecr_image_uri,
    get_image_tags,
    is_ecr_image_uri,
    parse_ecr_image_uri,
)

URI = "123456789012.dkr.ecr.us-west-2.amazonaws.com/my-repo:v1"


class TestBuildEcrImageUri:
    def test_default_region(self):
        assert build_ecr_image_uri("123456789012", "my-repo", "v1") == URI

    def test_custom_region(self):
        assert (
            build_ecr_image_uri("123456789012", "my-repo", "v1", region="eu-west-1")
            == "123456789012.dkr.ecr.eu-west-1.amazonaws.com/my-repo:v1"
        )


class TestIsEcrImageUri:
    def test_true_for_ecr(self):
        assert is_ecr_image_uri(URI) is True

    def test_false_for_docker_hub(self):
        assert is_ecr_image_uri("alpine:latest") is False

    def test_false_when_marker_only_in_repo_path(self):
        assert (
            is_ecr_image_uri(
                "docker.io/team/123456789012.dkr.ecr.us-west-2.amazonaws.com/repo:tag"
            )
            is False
        )

    def test_true_for_china_partition(self):
        assert is_ecr_image_uri(
            "123456789012.dkr.ecr.cn-north-1.amazonaws.com.cn/repo:tag"
        )


class TestParseEcrImageUri:
    def test_tagged(self):
        assert parse_ecr_image_uri(URI) == ("123456789012", "my-repo")

    def test_digest(self):
        uri = "123456789012.dkr.ecr.us-west-2.amazonaws.com/team/repo@sha256:abc"
        assert parse_ecr_image_uri(uri) == ("123456789012", "team/repo")

    def test_with_scheme(self):
        assert parse_ecr_image_uri(f"https://{URI}") == ("123456789012", "my-repo")

    def test_rejects_non_ecr(self):
        with pytest.raises(ValueError, match="Not an ECR image URI"):
            parse_ecr_image_uri("alpine:latest")


class TestGetImageTags:
    def test_returns_sorted_tags(self):
        ecr = MagicMock()
        ecr.describe_images.return_value = {
            "imageDetails": [{"imageTags": ["v1", "latest", "abc123"]}]
        }
        with patch("overture_core.cloud.aws.ecr.boto3.client", return_value=ecr):
            tags = get_image_tags(URI, "sha256:abc", region="us-west-2")
        assert tags == ["abc123", "latest", "v1"]
        ecr.describe_images.assert_called_once_with(
            registryId="123456789012",
            repositoryName="my-repo",
            imageIds=[{"imageDigest": "sha256:abc"}],
        )

    def test_untagged_image(self):
        ecr = MagicMock()
        ecr.describe_images.return_value = {"imageDetails": [{}]}
        with patch("overture_core.cloud.aws.ecr.boto3.client", return_value=ecr):
            assert get_image_tags(URI, "sha256:abc") == []
