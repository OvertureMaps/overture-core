"""Unit tests for AWS CodeArtifact helpers."""

from unittest.mock import MagicMock, patch

import pytest
from packaging.version import Version

from overture_core.cloud.aws.codeartifact import (
    CodeArtifactPyPiClient,
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

    def test_latest_in_branch_no_match_returns_none_str(self, client):
        versions = [Version("1.0.0")]
        with patch.object(client, "get_package_versions", return_value=versions):
            result = client.resolve_package_version(
                "pkg", PackageVersionStrategy.LATEST_IN_BRANCH, branch="missing"
            )
        assert result == "None"

    def test_unknown_strategy_raises(self, client):
        with pytest.raises(ValueError, match="Unknown version resolution strategy"):
            client.resolve_package_version("pkg", "not-a-real-strategy")


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
