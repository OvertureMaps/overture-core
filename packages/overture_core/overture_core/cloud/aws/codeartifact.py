"""AWS CodeArtifact helpers built on boto3.

Two repository formats are covered: PyPI (``CodeArtifactPyPiClient``, version
resolution plus a pip index URL) and Maven (``CodeArtifactMavenClient``, JAR
lookup for Scala projects that publish one artifact per Spark platform line).
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum

import boto3
import requests
from packaging.version import InvalidVersion, Version


def get_codeartifact_token(domain: str, domain_owner: str, region: str) -> str:
    """Mint a short-lived CodeArtifact authorization token.

    The returned token is a bearer credential valid for the domain's default
    TTL (12 hours) usable as the password half of a CodeArtifact repository
    URL (``https://aws:<token>@...``). Callers that mint tokens frequently
    should cache the result themselves; this function always makes a fresh
    STS-backed API call.

    Args:
        domain: CodeArtifact domain name (e.g. "overture-pypi").
        domain_owner: AWS account ID that owns the domain.
        region: Region for the CodeArtifact API endpoint.

    Returns:
        The authorization token string.
    """
    return boto3.client("codeartifact", region_name=region).get_authorization_token(
        domain=domain, domainOwner=domain_owner
    )["authorizationToken"]


class PackageVersionStrategy(Enum):
    LATEST_IN_BRANCH = "LATEST_IN_BRANCH"  # assumes poetry-dynamic-versioning in use
    LATEST_STABLE = "LATEST_STABLE"
    CUSTOM = "CUSTOM"


class CodeArtifactClient:
    """Repository coordinates plus a cached auth token, shared by the per-format clients."""

    def __init__(
        self,
        domain_owner: str,
        repository: str,
        domain: str,
        region_name: str,
    ):
        self.domain_owner = domain_owner
        self.repository = repository
        self.domain = domain
        self.region_name = region_name
        self.auth_token = None
        self.auth_token_expiration = None

    @property
    def host(self) -> str:
        return (
            f"{self.domain}-{self.domain_owner}"
            f".d.codeartifact.{self.region_name}.amazonaws.com"
        )

    def get_auth_token(self) -> str:
        if (
            not self.auth_token
            or not self.auth_token_expiration
            or (
                self.auth_token_expiration.replace(tzinfo=UTC) - datetime.now(UTC)
            ).total_seconds()
            < 3600
        ):
            response = boto3.client(
                "codeartifact", region_name=self.region_name
            ).get_authorization_token(domain=self.domain, domainOwner=self.domain_owner)
            self.auth_token = response["authorizationToken"]
            self.auth_token_expiration = response["expiration"]

        return self.auth_token


class CodeArtifactPyPiClient(CodeArtifactClient):
    """Lightweight client for AWS CodeArtifact (pip index URL + auth token)."""

    def resolve_package_version(
        self,
        package_name: str,
        strategy: PackageVersionStrategy,
        custom_version: str | None = None,
        branch: str | None = None,
    ) -> str | None:
        """Resolve a package version per the requested strategy.

        Args:
            package_name: Package to resolve.
            strategy: Version-resolution strategy.
                - ``LATEST_IN_BRANCH``: latest version in ``branch`` (relies on
                  poetry-dynamic-versioning local-version encoding).
                - ``LATEST_STABLE``: latest non-prerelease, non-dev version.
                - ``CUSTOM``: returns ``custom_version`` verbatim.
            custom_version: Required when strategy is ``CUSTOM``.
            branch: Required when strategy is ``LATEST_IN_BRANCH``.
        """
        if strategy == PackageVersionStrategy.LATEST_IN_BRANCH:
            if not branch:
                raise ValueError(
                    "branch must be specified when using "
                    "PackageVersionStrategy.LATEST_IN_BRANCH"
                )
            return self.get_latest_package_version_in_branch(package_name, branch)
        if strategy == PackageVersionStrategy.LATEST_STABLE:
            return self.get_latest_stable_package_version(package_name)
        if strategy == PackageVersionStrategy.CUSTOM:
            if not custom_version:
                raise ValueError(
                    "custom_version must be specified when using "
                    "PackageVersionStrategy.CUSTOM"
                )
            return custom_version
        raise ValueError(f"Unknown version resolution strategy: {strategy}")

    def get_latest_stable_package_version(self, package_name: str) -> str | None:
        """Latest version that is not a pre-release or dev-release, or ``None``."""
        version = self.get_latest_package_using_filter(
            package_name, lambda v: not v.is_prerelease and not v.is_devrelease
        )
        return str(version) if version is not None else None

    def get_latest_package_version_in_branch(
        self, package_name: str, branch_name: str
    ) -> str | None:
        """Latest version in ``branch_name``, or ``None`` if none match.

        Assumes the package uses poetry-dynamic-versioning and the branch is
        encoded in the local version (e.g. ``0.0.1.dev0+mybranch.2996.20250622165115``).
        """

        def matches_branch(version: Version) -> bool:
            if version.local:
                return version.local.split(".")[0] == branch_name.replace("-", "")
            return False

        version = self.get_latest_package_using_filter(package_name, matches_branch)
        return str(version) if version is not None else None

    def get_latest_package_version(self, package_name: str) -> str:
        """Latest available version (may be a prerelease from any branch)."""
        return str(self.get_package_versions(package_name)[0])

    def get_latest_package_using_filter(
        self, package_name: str, cond: Callable[[Version], bool]
    ) -> Version | None:
        """Latest ``Version`` satisfying ``cond``, or ``None`` if none match."""
        versions = self.get_package_versions(package_name)
        for v in versions:
            if cond(v):
                return v
        return None

    def get_package_versions(self, package_name: str) -> list[Version]:
        """List all valid ``Version`` objects for ``package_name`` in this repo."""
        client = boto3.client("codeartifact", region_name=self.region_name)
        response = client.list_package_versions(
            domain=self.domain,
            # Pinning domainOwner ensures this always looks up the account
            # that owns the domain, not the caller's own AWS account.
            domainOwner=self.domain_owner,
            repository=self.repository,
            format="pypi",
            package=package_name,
            sortBy="PUBLISHED_TIME",  # descending order
        )
        raw_versions = response.get("versions", [])
        if not raw_versions:
            raise Exception(f"No versions found for {package_name}")

        versions: list[Version] = []
        for v in raw_versions:
            try:
                versions.append(Version(v["version"]))
            except InvalidVersion:
                continue

        if not versions:
            raise Exception(f"No valid versions found for {package_name}")

        return versions

    def get_url(self) -> str:
        return f"https://aws:{self.get_auth_token()}@{self.host}/pypi/{self.repository}/simple/"


@dataclass(frozen=True)
class MavenCoordinates:
    """Base Maven coordinates of a Scala project that publishes one JAR per Spark
    platform line, with the line encoded in the artifact ID the same way Sedona and
    Iceberg do: ``corpus-spark-3.5_2.12``, ``corpus-spark-4.1_2.13``.
    """

    group_id: str
    base_artifact_id: str

    def artifact_id(self, spark_version: str, scala_version: str) -> str:
        """Artifact ID for a Spark version (any precision, truncated to major.minor)
        and Scala binary version."""
        spark_compat = ".".join(spark_version.split(".")[:2])
        return f"{self.base_artifact_id}-spark-{spark_compat}_{scala_version}"

    def jar_path(self, artifact_id: str, version: str) -> str:
        """Repository-relative path of the JAR, per the Maven layout."""
        group_path = self.group_id.replace(".", "/")
        return f"{group_path}/{artifact_id}/{version}/{artifact_id}-{version}.jar"


class CodeArtifactMavenClient(CodeArtifactClient):
    """Locate JARs in a CodeArtifact Maven repository for a given Spark runtime."""

    def get_url(self) -> str:
        return f"https://{self.host}/maven/{self.repository}/"

    def exists(self, path: str) -> bool:
        """Whether ``path`` (repository-relative) is present, via an authenticated HEAD."""
        response = requests.head(
            f"{self.get_url()}{path}", auth=("aws", self.get_auth_token()), timeout=30
        )
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    def resolve_jar_url(
        self,
        coordinates: MavenCoordinates,
        version: str,
        spark_version: str,
        scala_version: str,
    ) -> str:
        """Authenticated download URL of the JAR published for this runtime's platform line.

        Versions published before platform lines exist only under the bare artifact ID
        (Spark 3.5 / Scala 2.12), so a Scala 2.12 runtime falls back to that path when
        the per-line JAR is missing. Anything else missing raises ``ValueError`` naming
        the coordinates that were looked up.
        """
        artifact_id = coordinates.artifact_id(spark_version, scala_version)
        jar_path = coordinates.jar_path(artifact_id, version)
        if not self.exists(jar_path):
            legacy = coordinates.jar_path(coordinates.base_artifact_id, version)
            if scala_version == "2.12" and self.exists(legacy):
                jar_path = legacy
            else:
                raise ValueError(
                    f"No {artifact_id}:{version} in CodeArtifact repository "
                    f"{self.repository} for Spark {spark_version} / Scala {scala_version}. "
                    "Either that platform line isn't published, or the version predates it."
                )
        return f"https://aws:{self.get_auth_token()}@{self.host}/maven/{self.repository}/{jar_path}"
