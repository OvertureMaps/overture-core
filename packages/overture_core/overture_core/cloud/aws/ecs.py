"""ECS task and network configuration helpers built on boto3."""

import logging
from dataclasses import asdict, dataclass, field

import boto3

from overture_core.cloud.aws.ecr import get_image_tags, is_ecr_image_uri

logger = logging.getLogger(__name__)


def build_awsvpc_network_configuration(
    networking_info: dict, *, public: bool = True
) -> dict:
    """Build an ECS ``networkConfiguration`` block from a networking-info dict.

    *networking_info* is the shape typically stored in a single config value:

    .. code-block:: json

        {
          "security_group": "sg-xxxxx",
          "vpc_public_subnets": ["subnet-xxx", "subnet-yyy"],
          "vpc_private_subnets": ["subnet-aaa", "subnet-bbb"]
        }

    Args:
        networking_info: Parsed networking config.
        public: Use ``vpc_public_subnets`` with a public IP (default), or
            ``vpc_private_subnets`` without one.

    Raises:
        ValueError: If the security group or the selected subnet list is missing.
    """
    subnet_key = "vpc_public_subnets" if public else "vpc_private_subnets"
    security_group = networking_info.get("security_group")
    subnets = networking_info.get(subnet_key) or []
    if not security_group:
        raise ValueError("'security_group' not found in networking info")
    if not subnets:
        raise ValueError(f"'{subnet_key}' not found or empty in networking info")
    return {
        "awsvpcConfiguration": {
            "subnets": list(subnets),
            "securityGroups": [security_group],
            "assignPublicIp": "ENABLED" if public else "DISABLED",
        }
    }


@dataclass(frozen=True)
class ContainerImageInfo:
    """Resolved image provenance for one container in a running/finished ECS task."""

    uri: str
    digest: str
    tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def get_container_image_info(
    cluster: str,
    task_arn: str,
    container_name: str,
    region: str | None = None,
) -> ContainerImageInfo | None:
    """Resolve the image URI, digest, and ECR tags for a container in an ECS task.

    Calls ``ecs describe-tasks`` and, when the image lives in ECR and a
    digest was reported, ``ecr describe-images`` for its tags. Returns
    ``None`` (and logs why) if the task or container can't be found, since
    provenance capture shouldn't fail a pipeline that already ran. A tag
    lookup failure is likewise logged and leaves ``tags`` empty.

    Args:
        cluster: ECS cluster name or ARN.
        task_arn: ARN of the task to inspect.
        container_name: Name of the container within the task definition.
        region: Region of the cluster. Defaults to boto3's own resolution.
    """
    ecs = boto3.client("ecs", region_name=region)
    tasks = ecs.describe_tasks(cluster=cluster, tasks=[task_arn]).get("tasks", [])
    if not tasks:
        logger.warning("describe_tasks returned no results for %s", task_arn)
        return None

    container = next(
        (c for c in tasks[0].get("containers", []) if c.get("name") == container_name),
        None,
    )
    if container is None:
        logger.warning("Container %r not found in task %s", container_name, task_arn)
        return None

    uri = container.get("image", "")
    digest = container.get("imageDigest", "")
    tags: list[str] = []
    if uri and digest and is_ecr_image_uri(uri):
        try:
            tags = get_image_tags(uri, digest, region=region)
        except Exception as exc:
            logger.warning("Could not retrieve ECR tags for %s: %s", uri, exc)
    return ContainerImageInfo(uri=uri, digest=digest, tags=tags)
