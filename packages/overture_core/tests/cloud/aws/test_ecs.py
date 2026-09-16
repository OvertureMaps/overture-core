"""Unit tests for ECS helpers."""

from unittest.mock import MagicMock, patch

import pytest

from overture_core.cloud.aws.ecs import (
    ContainerImageInfo,
    build_awsvpc_network_configuration,
    get_container_image_info,
)

NETWORKING = {
    "security_group": "sg-1",
    "vpc_public_subnets": ["subnet-pub-a", "subnet-pub-b"],
    "vpc_private_subnets": ["subnet-priv-a"],
}
ECR_URI = "123456789012.dkr.ecr.us-west-2.amazonaws.com/my-repo:v1"


class TestBuildAwsvpcNetworkConfiguration:
    def test_public_by_default(self):
        assert build_awsvpc_network_configuration(NETWORKING) == {
            "awsvpcConfiguration": {
                "subnets": ["subnet-pub-a", "subnet-pub-b"],
                "securityGroups": ["sg-1"],
                "assignPublicIp": "ENABLED",
            }
        }

    def test_private(self):
        config = build_awsvpc_network_configuration(NETWORKING, public=False)
        assert config["awsvpcConfiguration"]["subnets"] == ["subnet-priv-a"]
        assert config["awsvpcConfiguration"]["assignPublicIp"] == "DISABLED"

    def test_missing_security_group(self):
        with pytest.raises(ValueError, match="security_group"):
            build_awsvpc_network_configuration({"vpc_public_subnets": ["s"]})

    def test_missing_subnets(self):
        with pytest.raises(ValueError, match="vpc_public_subnets"):
            build_awsvpc_network_configuration({"security_group": "sg-1"})

    def test_empty_subnets(self):
        with pytest.raises(ValueError, match="vpc_private_subnets"):
            build_awsvpc_network_configuration(
                {"security_group": "sg-1", "vpc_private_subnets": []}, public=False
            )


def _describe_tasks_response(containers):
    return {"tasks": [{"containers": containers}]}


class TestGetContainerImageInfo:
    def _run(self, ecs_response, ecr_response=None, ecr_raises=None):
        ecs = MagicMock()
        ecs.describe_tasks.return_value = ecs_response
        ecr = MagicMock()
        if ecr_raises:
            ecr.describe_images.side_effect = ecr_raises
        else:
            ecr.describe_images.return_value = ecr_response or {
                "imageDetails": [{"imageTags": ["v1"]}]
            }

        def client(service, region_name=None):
            return {"ecs": ecs, "ecr": ecr}[service]

        with (
            patch("overture_core.cloud.aws.ecs.boto3.client", side_effect=client),
            patch("overture_core.cloud.aws.ecr.boto3.client", side_effect=client),
        ):
            return get_container_image_info("cluster", "arn:task", "app")

    def test_ecr_image_with_tags(self):
        info = self._run(
            _describe_tasks_response(
                [{"name": "app", "image": ECR_URI, "imageDigest": "sha256:abc"}]
            )
        )
        assert info == ContainerImageInfo(uri=ECR_URI, digest="sha256:abc", tags=["v1"])
        assert info.to_dict() == {
            "uri": ECR_URI,
            "digest": "sha256:abc",
            "tags": ["v1"],
        }

    def test_non_ecr_image_skips_tag_lookup(self):
        info = self._run(
            _describe_tasks_response(
                [{"name": "app", "image": "alpine:latest", "imageDigest": "sha256:abc"}]
            )
        )
        assert info == ContainerImageInfo(
            uri="alpine:latest", digest="sha256:abc", tags=[]
        )

    def test_missing_digest_skips_tag_lookup(self):
        info = self._run(_describe_tasks_response([{"name": "app", "image": ECR_URI}]))
        assert info.digest == ""
        assert info.tags == []

    def test_tag_lookup_failure_is_swallowed(self):
        info = self._run(
            _describe_tasks_response(
                [{"name": "app", "image": ECR_URI, "imageDigest": "sha256:abc"}]
            ),
            ecr_raises=RuntimeError("boom"),
        )
        assert info.uri == ECR_URI
        assert info.tags == []

    def test_no_tasks_returns_none(self):
        assert self._run({"tasks": []}) is None

    def test_container_not_found_returns_none(self):
        assert self._run(_describe_tasks_response([{"name": "other"}])) is None
