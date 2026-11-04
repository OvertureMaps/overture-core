from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from github import Auth, Github, GithubException

from overture_core.apis import github as gh

REQUEST = "github.Requester.Requester.requestJsonAndCheck"


@pytest.fixture(scope="module")
def pem():
    return (
        rsa.generate_private_key(public_exponent=65537, key_size=2048)
        .private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
        .decode()
    )


@pytest.fixture
def secret(pem):
    with patch.object(gh, "get_secret_string", return_value=pem) as m:
        yield m


def test_get_installation_token_exchanges_via_http(secret):
    response = ({}, {"token": "ghs_x", "expires_at": "2099-01-01T00:00:00Z"})
    with patch(REQUEST, return_value=response) as http:
        token = gh.get_installation_token("my/secret", 123, 456, region="us-west-2")

    assert token == "ghs_x"
    secret.assert_called_once_with("my/secret", region="us-west-2")
    assert http.call_args.args[:2] == ("POST", "/app/installations/456/access_tokens")


def test_get_installation_auth_returns_auth_type(secret):
    assert isinstance(gh.get_installation_auth("s", 1, 2), Auth.AppInstallationAuth)


def test_app_id_is_passed_as_string(secret):
    with patch.object(gh.Auth, "AppAuth") as app_auth:
        gh.get_installation_auth("s", 123, 2)
    assert app_auth.call_args.args[0] == "123"


def test_get_github_client(secret):
    assert isinstance(gh.get_github_client("s", 1, 2), Github)


def test_missing_secret_reports_aws_error_code():
    err = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "SECRETPEM"}},
        "GetSecretValue",
    )
    with patch.object(gh, "get_secret_string", side_effect=err):
        with pytest.raises(gh.GitHubAppTokenError) as ei:
            gh.get_installation_token("my/secret", 1, 2)
    assert "my/secret" in str(ei.value)
    assert "ResourceNotFoundException" in str(ei.value)
    assert "SECRETPEM" not in str(ei.value)
    assert ei.value.__cause__ is None and ei.value.__suppress_context__


def test_missing_secret_non_aws_error_reports_type():
    with patch.object(gh, "get_secret_string", side_effect=RuntimeError("SECRETPEM")):
        with pytest.raises(gh.GitHubAppTokenError, match="RuntimeError") as ei:
            gh.get_installation_auth("s", 1, 2)
    assert "SECRETPEM" not in str(ei.value)


def test_invalid_pem_raises_without_leaking():
    with patch.object(gh, "get_secret_string", return_value="not-a-pem"):
        with pytest.raises(gh.GitHubAppTokenError) as ei:
            gh.get_installation_token("s", 1, 2)
    assert "not-a-pem" not in str(ei.value)


def test_rejected_exchange_raises_without_leaking(secret):
    exc = GithubException(401, {"message": "Bad credentials ghs_leak"}, {})
    with patch(REQUEST, side_effect=exc):
        with pytest.raises(gh.GitHubAppTokenError) as ei:
            gh.get_installation_token("s", 1, 2)
    assert "401" in str(ei.value)
    assert "ghs_leak" not in str(ei.value)


def test_unexpected_exchange_error_wrapped(secret):
    with patch(REQUEST, side_effect=ConnectionError("secret-detail")):
        with pytest.raises(gh.GitHubAppTokenError, match="ConnectionError") as ei:
            gh.get_installation_token("s", 1, 2)
    assert "secret-detail" not in str(ei.value)
