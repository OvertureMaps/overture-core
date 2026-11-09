"""Tests for update_docs_for_release (GitHub PR automation for docs releases).

Mocks boto3 (S3 + Secrets Manager) and PyGithub entirely -- no real AWS or
GitHub calls. ``github`` is imported lazily inside the function under test, so
tests patch the real `github` module's attributes directly.
"""

from unittest import mock

import pytest
from botocore.exceptions import ClientError

from overture_core.docs import update_docs_for_release


def _no_such_key_error() -> ClientError:
    return ClientError(
        {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "The specified key does not exist.",
            }
        },
        "GetObject",
    )


_DEFAULT_KWARGS = dict(
    docs_repo="OvertureMaps/docs",
    base_branch="main",
    attribution_path="docs/_generated_attribution.mdx",
    config_path="docusaurus.config.js",
    app_slug="test-app",
    app_client_id="client-id-123",
    app_installation_id=42,
    pem_secret_name="/managed-secrets/github/test_app_pem",
    secrets_region="us-west-2",
)


@pytest.fixture
def fake_s3():
    client = mock.MagicMock()
    return client


@pytest.fixture
def fake_secretsmanager():
    client = mock.MagicMock()
    client.get_secret_value.return_value = {"SecretString": "fake-pem-contents"}
    return client


@pytest.fixture
def fake_boto3_client(fake_s3, fake_secretsmanager):
    def _client(service_name, **kwargs):
        if service_name == "s3":
            return fake_s3
        if service_name == "secretsmanager":
            return fake_secretsmanager
        raise AssertionError(f"unexpected boto3 client: {service_name}")

    with mock.patch("overture_core.docs.boto3.client", side_effect=_client):
        yield


@pytest.fixture
def fake_repo():
    return mock.MagicMock()


@pytest.fixture
def fake_github(fake_repo):
    gh_instance = mock.MagicMock()
    gh_instance.get_repo.return_value = fake_repo
    bot = mock.MagicMock(name="bot")
    bot.name = "Test App"
    bot.login = "test-app"
    bot.id = 999
    gh_instance.get_user.return_value = bot

    with (
        mock.patch(
            "overture_core.apis.github.get_github_client", return_value=gh_instance
        ),
        mock.patch(
            "overture_core.apis.github.open_pr",
            return_value="https://github.com/OvertureMaps/docs/pull/1",
        ) as open_pr,
    ):
        yield gh_instance, fake_repo, open_pr


def _contents(text: str, sha: str = "sha1"):
    contents = mock.MagicMock()
    contents.decoded_content = text.encode("utf-8")
    contents.sha = sha
    return contents


class TestMissingArtifact:
    def test_raises_runtime_error_when_s3_object_missing(
        self, fake_boto3_client, fake_s3
    ):
        fake_s3.get_object.side_effect = _no_such_key_error()
        with pytest.raises(RuntimeError, match="Attribution artifact missing"):
            update_docs_for_release("bucket", "key", "2024-08-01.0", **_DEFAULT_KWARGS)

    def test_reraises_other_client_errors(self, fake_boto3_client, fake_s3):
        fake_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "GetObject"
        )
        with pytest.raises(ClientError):
            update_docs_for_release("bucket", "key", "2024-08-01.0", **_DEFAULT_KWARGS)


class TestNoChangesNeeded:
    def test_returns_skip_message_when_nothing_changed(
        self, fake_boto3_client, fake_s3, fake_github
    ):
        _, repo, open_pr = fake_github
        attribution = "same body"
        fake_s3.get_object.return_value = {
            "Body": mock.MagicMock(read=lambda: attribution.encode())
        }
        repo.get_contents.side_effect = [
            _contents(attribution),
            _contents("const fallback = '2024-01-01.0';"),
        ]
        # release_version matches what's already in the config -> no diff.
        result = update_docs_for_release(
            "bucket", "key", "2024-01-01.0", **_DEFAULT_KWARGS
        )
        assert result == "No docs changes detected, skipping PR."
        open_pr.assert_not_called()


class TestMissingFallbackPattern:
    def test_raises_value_error_when_pattern_not_found(
        self, fake_boto3_client, fake_s3, fake_github
    ):
        _, repo, open_pr = fake_github
        fake_s3.get_object.return_value = {
            "Body": mock.MagicMock(read=lambda: b"new attribution body")
        }
        repo.get_contents.side_effect = [
            _contents("old attribution body"),
            _contents("no fallback const here"),
        ]
        with pytest.raises(ValueError, match="Could not find fallback"):
            update_docs_for_release("bucket", "key", "2024-08-01.0", **_DEFAULT_KWARGS)


class TestOpensPullRequest:
    def _run(self, fake_s3, repo, attr_old, config_old, version="2024-08-01.0"):
        fake_s3.get_object.return_value = {
            "Body": mock.MagicMock(read=lambda: b"new attribution body")
        }
        repo.get_contents.side_effect = [_contents(attr_old), _contents(config_old)]
        return update_docs_for_release("bucket", "key", version, **_DEFAULT_KWARGS)

    def test_delegates_both_changes_to_open_pr(
        self, fake_boto3_client, fake_s3, fake_github
    ):
        gh, repo, open_pr = fake_github
        result = self._run(
            fake_s3, repo, "old attribution body", "const fallback = '2024-01-01.0';"
        )

        assert result == "PR created: https://github.com/OvertureMaps/docs/pull/1"
        args, kwargs = open_pr.call_args
        assert args == (repo, gh.get_user.return_value)
        gh.get_user.assert_called_once_with("test-app[bot]")
        assert kwargs["branch"] == "release-update-2024-08-01.0"
        assert kwargs["files"] == {
            "docs/_generated_attribution.mdx": "new attribution body",
            "docusaurus.config.js": "const fallback = '2024-08-01.0';",
        }
        assert kwargs["base"] == "main"
        assert kwargs["draft"] is False
        assert "2024-08-01.0" in kwargs["title"]
        assert "Updated attribution page" in kwargs["body"]
        assert "Updated fallback release version" in kwargs["body"]

    def test_only_includes_changed_files(self, fake_boto3_client, fake_s3, fake_github):
        _, repo, open_pr = fake_github
        self._run(
            fake_s3, repo, "old attribution body", "const fallback = '2024-08-01.0';"
        )

        kwargs = open_pr.call_args.kwargs
        assert list(kwargs["files"]) == ["docs/_generated_attribution.mdx"]
        assert "Updated fallback release version" not in kwargs["body"]
