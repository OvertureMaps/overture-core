"""ECR image URI helpers built on boto3."""

import re

import boto3

_ECR_HOST_MARKER = ".dkr.ecr."
# 12-digit account, region, amazonaws.com with optional China partition suffix.
_ECR_HOST_RE = re.compile(r"^\d{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com(\.cn)?$")


def _registry_host(image_uri: str) -> str:
    return image_uri.split("//")[-1].partition("/")[0]


def build_ecr_image_uri(
    account_id: str, repository: str, tag: str, region: str = "us-west-2"
) -> str:
    """Build a fully qualified ECR image URI.

    >>> build_ecr_image_uri("123456789012", "my-repo", "v1")
    '123456789012.dkr.ecr.us-west-2.amazonaws.com/my-repo:v1'
    """
    return f"{account_id}{_ECR_HOST_MARKER}{region}.amazonaws.com/{repository}:{tag}"


def is_ecr_image_uri(image_uri: str) -> bool:
    """Return whether *image_uri*'s registry host is an ECR registry.

    Checks the host only, so a Docker Hub image whose repository path happens
    to contain an ECR-looking segment isn't misclassified.
    """
    return _ECR_HOST_RE.match(_registry_host(image_uri)) is not None


def parse_ecr_image_uri(image_uri: str) -> tuple[str, str]:
    """Split an ECR image URI into ``(registry_id, repository_name)``.

    Accepts ``registry/repo:tag``, ``registry/repo@sha256:...``, and an
    optional ``https://`` scheme prefix.

    Raises:
        ValueError: If *image_uri* isn't an ECR URI.
    """
    if not is_ecr_image_uri(image_uri):
        raise ValueError(f"Not an ECR image URI: {image_uri!r}")
    host, _, repo_ref = image_uri.split("//")[-1].partition("/")
    registry_id = host.split(".", 1)[0]
    repository = repo_ref.split("@", 1)[0].split(":", 1)[0]
    return registry_id, repository


def get_image_tags(
    image_uri: str, image_digest: str, region: str | None = None
) -> list[str]:
    """Return the sorted tags on the ECR image identified by *image_digest*.

    Args:
        image_uri: Any ECR URI for the repository (used to resolve registry and repo).
        image_digest: ``sha256:...`` digest of the image to look up.
        region: Region of the registry. Defaults to boto3's own resolution.
    """
    registry_id, repository = parse_ecr_image_uri(image_uri)
    ecr = boto3.client("ecr", region_name=region)
    response = ecr.describe_images(
        registryId=registry_id,
        repositoryName=repository,
        imageIds=[{"imageDigest": image_digest}],
    )
    return sorted(response["imageDetails"][0].get("imageTags", []))
