"""Unit tests for AWS CodeArtifact helpers."""

from unittest.mock import MagicMock, patch

import pytest
from packaging.version import Version

from overture_core.cloud.aws.codeartifact import (
    CodeArtifactMavenClient,
    CodeArtifactPyPiClient,
    MavenCoordinates,
    PackageVersionStrategy,
    get_codeartifact_token,
)


class TestGetCodeartifactToken:
    def test_returns_authorization_token(self):
        codeartifact = MagicMock()
        codeartifact.get_authorization_token.return_value = {
            "authorizationToken": "token-value"
        }
        with patch(
            "overture_core.cloud.aws.codeartifact.boto3.client",
            return_value=codeartifact,
        ) as client:
            token = get_codeartifact_token("overture-pypi", "123456789012", "us-west-2")

        assert token == "token-value"
        client.assert_called_once_with("codeartifact", region_name="us-west-2")
        codeartifact.get_authorization_token.assert_called_once_with(
            domain="overture-pypi", domainOwner="123456789012"
        )


@pytest.fixture
def client():
    return CodeArtifactPyPiClient(
        domain_owner="123", domain="dom", repository="repo", region_name="us-east-1"
    )


class TestResolvePackageVersion:
    def test_custom_returns_version_as_is(self, client):
        result = client.resolve_package_version(
            "pkg", PackageVersionStrategy.CUSTOM, custom_version="1.2.3"
        )
        assert result == "1.2.3"

    def test_custom_without_version_raises(self, client):
        with pytest.raises(ValueError, match="custom_version must be specified"):
            client.resolve_package_version("pkg", PackageVersionStrategy.CUSTOM)

    @pytest.mark.parametrize("non_stable", ["1.1.0a1", "2.0.0.dev1"])
    def test_latest_stable_skips_non_stable(self, client, non_stable):
        stable = Version("1.0.0")
        with patch.object(
            client, "get_package_versions", return_value=[Version(non_stable), stable]
        ):
            result = client.resolve_package_version(
                "pkg", PackageVersionStrategy.LATEST_STABLE
            )
        assert result == "1.0.0"

    def test_latest_in_branch_matches(self, client):
        versions = [
            Version("0.0.1.dev0+mybranch.1"),
            Version("0.0.1.dev0+otherbranch.1"),
            Version("1.0.0"),
        ]
        with patch.object(client, "get_package_versions", return_value=versions):
            result = client.resolve_package_version(
                "pkg", PackageVersionStrategy.LATEST_IN_BRANCH, branch="mybranch"
            )
        assert result == "0.0.1.dev0+mybranch.1"

    def test_latest_in_branch_hyphen_normalised(self, client):
        """Branch names with hyphens are normalised (hyphens stripped) for local-version matching."""
        versions = [Version("0.0.1.dev0+mybranch.1")]
        with patch.object(client, "get_package_versions", return_value=versions):
            result = client.resolve_package_version(
                "pkg", PackageVersionStrategy.LATEST_IN_BRANCH, branch="my-branch"
            )
        assert result == "0.0.1.dev0+mybranch.1"

    def test_latest_in_branch_no_match_returns_none(self, client):
        versions = [Version("1.0.0")]
        with patch.object(client, "get_package_versions", return_value=versions):
            result = client.resolve_package_version(
                "pkg", PackageVersionStrategy.LATEST_IN_BRANCH, branch="missing"
            )
        assert result is None

    def test_latest_in_branch_without_branch_raises(self, client):
        with pytest.raises(ValueError, match="branch must be specified"):
            client.resolve_package_version(
                "pkg", PackageVersionStrategy.LATEST_IN_BRANCH
            )

    def test_latest_stable_no_match_returns_none(self, client):
        with patch.object(
            client, "get_package_versions", return_value=[Version("1.1.0a1")]
        ):
            result = client.resolve_package_version(
                "pkg", PackageVersionStrategy.LATEST_STABLE
            )
        assert result is None

    def test_unknown_strategy_raises(self, client):
        with pytest.raises(ValueError, match="Unknown version resolution strategy"):
            client.resolve_package_version("pkg", "not-a-real-strategy")


class TestGetLatestPackageVersion:
    def test_returns_first_version_as_str(self, client):
        with patch.object(
            client, "get_package_versions", return_value=[Version("2.0.0")]
        ):
            assert client.get_latest_package_version("pkg") == "2.0.0"


class TestGetPackageVersions:
    def test_passes_domain_owner_to_list_package_versions(self, client):
        """CodeArtifact defaults to the caller's own AWS account unless
        domainOwner is passed explicitly, so this call must always include it
        to resolve against the account that owns the domain."""
        mock_boto_client = MagicMock()
        mock_boto_client.list_package_versions.return_value = {
            "versions": [{"version": "1.0.0"}]
        }
        with patch(
            "overture_core.cloud.aws.codeartifact.boto3.client",
            return_value=mock_boto_client,
        ):
            client.get_package_versions("pkg")

        mock_boto_client.list_package_versions.assert_called_once_with(
            domain="dom",
            domainOwner="123",
            repository="repo",
            format="pypi",
            package="pkg",
            sortBy="PUBLISHED_TIME",
        )

    def test_raises_when_no_versions_found(self, client):
        mock_boto_client = MagicMock()
        mock_boto_client.list_package_versions.return_value = {"versions": []}
        with patch(
            "overture_core.cloud.aws.codeartifact.boto3.client",
            return_value=mock_boto_client,
        ):
            with pytest.raises(Exception, match="No versions found"):
                client.get_package_versions("pkg")

    def test_skips_invalid_versions(self, client):
        mock_boto_client = MagicMock()
        mock_boto_client.list_package_versions.return_value = {
            "versions": [{"version": "not-a-version"}, {"version": "1.0.0"}]
        }
        with patch(
            "overture_core.cloud.aws.codeartifact.boto3.client",
            return_value=mock_boto_client,
        ):
            versions = client.get_package_versions("pkg")
        assert versions == [Version("1.0.0")]

    def test_raises_when_all_versions_invalid(self, client):
        mock_boto_client = MagicMock()
        mock_boto_client.list_package_versions.return_value = {
            "versions": [{"version": "not-a-version"}]
        }
        with patch(
            "overture_core.cloud.aws.codeartifact.boto3.client",
            return_value=mock_boto_client,
        ):
            with pytest.raises(Exception, match="No valid versions found"):
                client.get_package_versions("pkg")


class TestAuthTokenAndUrl:
    def test_get_auth_token_fetches_and_caches(self, client):
        from datetime import UTC, datetime, timedelta

        mock_boto_client = MagicMock()
        mock_boto_client.get_authorization_token.return_value = {
            "authorizationToken": "tok",
            "expiration": datetime.now(UTC) + timedelta(hours=2),
        }
        with patch(
            "overture_core.cloud.aws.codeartifact.boto3.client",
            return_value=mock_boto_client,
        ):
            token = client.get_auth_token()
            assert token == "tok"
            mock_boto_client.get_authorization_token.assert_called_once_with(
                domain="dom", domainOwner="123"
            )

            # Second call reuses the cached token instead of re-fetching.
            client.get_auth_token()
            mock_boto_client.get_authorization_token.assert_called_once()

    def test_get_auth_token_refreshes_when_near_expiry(self, client):
        from datetime import UTC, datetime, timedelta

        mock_boto_client = MagicMock()
        mock_boto_client.get_authorization_token.return_value = {
            "authorizationToken": "tok",
            "expiration": datetime.now(UTC) + timedelta(minutes=30),
        }
        with patch(
            "overture_core.cloud.aws.codeartifact.boto3.client",
            return_value=mock_boto_client,
        ):
            client.get_auth_token()
            # Expiration is within the 3600s refresh window, so a second call
            # re-fetches instead of reusing the near-expiry token.
            client.get_auth_token()
        assert mock_boto_client.get_authorization_token.call_count == 2

    def test_get_url_embeds_token_and_repo_info(self, client, monkeypatch):
        monkeypatch.setattr(client, "get_auth_token", lambda: "tok")
        assert client.get_url() == (
            "https://aws:tok@dom-123.d.codeartifact.us-east-1.amazonaws.com/pypi/repo/simple/"
        )


CORPUS = MavenCoordinates(group_id="com.overturemaps", base_artifact_id="corpus")


class TestMavenCoordinates:
    @pytest.mark.parametrize(
        "spark, scala, expected",
        [
            ("3.5.4", "2.12", "corpus-spark-3.5_2.12"),
            ("3.3", "2.12", "corpus-spark-3.3_2.12"),
            ("4.1.3", "2.13", "corpus-spark-4.1_2.13"),
        ],
    )
    def test_artifact_id_uses_spark_minor_and_scala(self, spark, scala, expected):
        assert CORPUS.artifact_id(spark, scala) == expected

    def test_jar_path(self):
        divisions = MavenCoordinates("org.overturemaps", "overture-divisions")
        path = divisions.jar_path(
            "overture-divisions-spark-3.5_2.12", "0.1.0-SNAPSHOT-dev-abc12345"
        )
        assert path == (
            "org/overturemaps/overture-divisions-spark-3.5_2.12/0.1.0-SNAPSHOT-dev-abc12345/"
            "overture-divisions-spark-3.5_2.12-0.1.0-SNAPSHOT-dev-abc12345.jar"
        )


class _Head:
    def __init__(self, present: set, status: int = 404):
        self.present = present
        self.status = status
        self.urls = []
        self.auths = []

    def __call__(self, url, **kwargs):
        self.urls.append(url)
        self.auths.append(kwargs.get("auth"))
        found = any(url.endswith(p) for p in self.present)
        response = MagicMock()
        response.status_code = 200 if found else self.status
        return response


@pytest.fixture
def maven(monkeypatch):
    client = CodeArtifactMavenClient(
        domain_owner="123", domain="dom", repository="mvn", region_name="us-east-1"
    )
    monkeypatch.setattr(client, "get_auth_token", lambda: "tok")
    return client


def _resolve(monkeypatch, maven, spark, scala, version, present, status=404):
    head = _Head(present, status)
    monkeypatch.setattr("overture_core.cloud.aws.codeartifact.requests.head", head)
    url = maven.resolve_jar_url(CORPUS, version, spark, scala)
    return url, head


class TestCodeArtifactMavenClient:
    def test_get_url_has_no_credentials(self, maven):
        assert maven.get_url() == (
            "https://dom-123.d.codeartifact.us-east-1.amazonaws.com/maven/mvn/"
        )

    def test_exists_authenticates_head(self, maven, monkeypatch):
        head = _Head({"a.jar"})
        monkeypatch.setattr("overture_core.cloud.aws.codeartifact.requests.head", head)
        assert maven.exists("x/a.jar") is True
        assert head.urls == [
            "https://dom-123.d.codeartifact.us-east-1.amazonaws.com/maven/mvn/x/a.jar"
        ]
        assert head.auths == [("aws", "tok")]

    def test_exists_raises_on_non_404_error(self, maven, monkeypatch):
        response = MagicMock(status_code=500)
        response.raise_for_status.side_effect = RuntimeError("boom")
        monkeypatch.setattr(
            "overture_core.cloud.aws.codeartifact.requests.head",
            lambda *a, **k: response,
        )
        with pytest.raises(RuntimeError, match="boom"):
            maven.exists("x/a.jar")

    @pytest.mark.parametrize("status", [401, 403])
    def test_exists_raises_permission_error_without_token(
        self, maven, monkeypatch, status
    ):
        head = _Head(set(), status)
        monkeypatch.setattr("overture_core.cloud.aws.codeartifact.requests.head", head)
        monkeypatch.setattr(maven, "get_auth_token", lambda: "s3cr3t-token-value")
        with pytest.raises(PermissionError) as exc:
            maven.exists("x/a.jar")
        message = str(exc.value)
        assert f"HTTP {status}" in message
        assert "domain dom" in message and "repository mvn" in message
        assert "s3cr3t" not in message
        assert "s3cr3t" not in head.urls[0]
        assert head.auths == [("aws", "s3cr3t-token-value")]

    def test_prefers_platform_line(self, maven, monkeypatch):
        url, head = _resolve(
            monkeypatch, maven, "3.5.4", "2.12", "v1", {"corpus-spark-3.5_2.12-v1.jar"}
        )
        assert url == (
            "https://aws:tok@dom-123.d.codeartifact.us-east-1.amazonaws.com/maven/mvn/"
            "com/overturemaps/corpus-spark-3.5_2.12/v1/corpus-spark-3.5_2.12-v1.jar"
        )
        assert len(head.urls) == 1

    def test_falls_back_to_legacy_artifact_for_scala_2_12(self, maven, monkeypatch):
        url, head = _resolve(
            monkeypatch, maven, "3.5.4", "2.12", "v0", {"corpus-v0.jar"}
        )
        assert url.endswith("com/overturemaps/corpus/v0/corpus-v0.jar")
        assert len(head.urls) == 2

    def test_no_legacy_fallback_for_other_scala(self, maven, monkeypatch):
        with pytest.raises(ValueError, match="corpus-spark-4.1_2.13:v0"):
            _resolve(monkeypatch, maven, "4.1.3", "2.13", "v0", {"corpus-v0.jar"})

    def test_raises_when_no_jar_for_runtime(self, maven, monkeypatch):
        with pytest.raises(
            ValueError, match=r"corpus-spark-3.3_2.12:v1 .* Spark 3.3.0 / Scala 2.12"
        ):
            _resolve(monkeypatch, maven, "3.3.0", "2.12", "v1", set())
