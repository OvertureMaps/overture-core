"""Tests for update_docs_for_release (GitHub PR automation for docs releases).

Mocks boto3 (S3 + Secrets Manager) and PyGithub entirely -- no real AWS or
GitHub calls. ``github`` is imported lazily inside the function under test, so
tests patch the real `github` module's attributes directly.
"""

from unittest import mock

import pytest
from botocore.exceptions import ClientError

from overture_core.docs import update_docs_for_release


class _FakeGithubException(Exception):
    def __init__(self, status, data=None):
        super().__init__(data)
        self.status = status


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
    repo = mock.MagicMock()
    repo.get_git_ref.side_effect = _FakeGithubException(404)
    return repo


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
        mock.patch("github.Github", return_value=gh_instance),
        mock.patch("github.Auth") as auth_mod,
        mock.patch("github.InputGitAuthor", side_effect=lambda n, e: (n, e)),
        mock.patch("github.GithubException", _FakeGithubException),
    ):
        auth_mod.AppAuth.return_value.get_installation_auth.return_value = "token"
        yield gh_instance, fake_repo


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
        _, repo = fake_github
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
        repo.create_pull.assert_not_called()


class TestMissingFallbackPattern:
    def test_raises_value_error_when_pattern_not_found(
        self, fake_boto3_client, fake_s3, fake_github
    ):
        _, repo = fake_github
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
    def test_creates_pr_with_both_changes(
        self, fake_boto3_client, fake_s3, fake_github
    ):
        _, repo = fake_github
        fake_s3.get_object.return_value = {
            "Body": mock.MagicMock(read=lambda: b"new attribution body")
        }
        repo.get_contents.side_effect = [
            _contents("old attribution body"),
            _contents("const fallback = '2024-01-01.0';"),
        ]
        repo.create_pull.return_value = mock.MagicMock(
            html_url="https://github.com/OvertureMaps/docs/pull/1"
        )

        result = update_docs_for_release(
            "bucket", "key", "2024-08-01.0", **_DEFAULT_KWARGS
        )

        assert result == "PR created: https://github.com/OvertureMaps/docs/pull/1"
        assert repo.update_file.call_count == 2
        repo.create_git_ref.assert_called_once()
        create_pull_kwargs = repo.create_pull.call_args.kwargs
        assert "2024-08-01.0" in create_pull_kwargs["title"]
        assert "Updated attribution page" in create_pull_kwargs["body"]
        assert "Updated fallback release version" in create_pull_kwargs["body"]

    def test_deletes_stale_branch_from_prior_run(
        self, fake_boto3_client, fake_s3, fake_github
    ):
        _, repo = fake_github
        stale_ref = mock.MagicMock()
        repo.get_git_ref.side_effect = None
        repo.get_git_ref.return_value = stale_ref
        fake_s3.get_object.return_value = {
            "Body": mock.MagicMock(read=lambda: b"new attribution body")
        }
        repo.get_contents.side_effect = [
            _contents("old attribution body"),
            _contents("const fallback = '2024-01-01.0';"),
        ]
        repo.create_pull.return_value = mock.MagicMock(
            html_url="https://github.com/OvertureMaps/docs/pull/2"
        )

        update_docs_for_release("bucket", "key", "2024-08-01.0", **_DEFAULT_KWARGS)

        stale_ref.delete.assert_called_once()

    def test_reraises_non_404_github_exception_from_get_git_ref(
        self, fake_boto3_client, fake_s3, fake_github
    ):
        _, repo = fake_github
        repo.get_git_ref.side_effect = _FakeGithubException(500)
        fake_s3.get_object.return_value = {
            "Body": mock.MagicMock(read=lambda: b"new attribution body")
        }
        repo.get_contents.side_effect = [
            _contents("old attribution body"),
            _contents("const fallback = '2024-01-01.0';"),
        ]
        with pytest.raises(_FakeGithubException):
            update_docs_for_release("bucket", "key", "2024-08-01.0", **_DEFAULT_KWARGS)

    def test_only_updates_attribution_when_only_that_changed(
        self, fake_boto3_client, fake_s3, fake_github
    ):
        _, repo = fake_github
        fake_s3.get_object.return_value = {
            "Body": mock.MagicMock(read=lambda: b"new attribution body")
        }
        same_fallback = "const fallback = '2024-08-01.0';"
        repo.get_contents.side_effect = [
            _contents("old attribution body"),
            _contents(same_fallback),
        ]
        repo.create_pull.return_value = mock.MagicMock(
            html_url="https://github.com/OvertureMaps/docs/pull/3"
        )

        update_docs_for_release("bucket", "key", "2024-08-01.0", **_DEFAULT_KWARGS)

        assert repo.update_file.call_count == 1
        create_pull_kwargs = repo.create_pull.call_args.kwargs
        assert "Updated attribution page" in create_pull_kwargs["body"]
        assert "Updated fallback release version" not in create_pull_kwargs["body"]
