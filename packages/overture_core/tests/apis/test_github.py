from unittest.mock import MagicMock, patch

import pytest
from github import Auth, Github, GithubException

from overture_core.apis import github as gh

SECRET_PEM = "-----BEGIN RSA PRIVATE KEY-----SECRETPEM"


def _auth_mock(token=None, exc=None):
    auth = MagicMock()
    if exc:
        type(auth).token = property(lambda self: (_ for _ in ()).throw(exc))
    else:
        type(auth).token = property(lambda self: token)
    return auth


def test_get_installation_auth_passes_args():
    with (
        patch.object(gh, "get_secret_string", return_value=SECRET_PEM) as secret,
        patch.object(gh.Auth, "AppAuth") as app_auth,
    ):
        result = gh.get_installation_auth("my/secret", 123, 456, region="us-west-2")

    secret.assert_called_once_with("my/secret", region="us-west-2")
    app_auth.assert_called_once_with(123, SECRET_PEM)
    app_auth.return_value.get_installation_auth.assert_called_once_with(456)
    assert result is app_auth.return_value.get_installation_auth.return_value


def test_get_installation_auth_returns_real_auth_type():
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    pem = (
        rsa.generate_private_key(public_exponent=65537, key_size=2048)
        .private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
        .decode()
    )
    with patch.object(gh, "get_secret_string", return_value=pem):
        assert isinstance(gh.get_installation_auth("s", 1, 2), Auth.AppInstallationAuth)


def test_get_installation_token_returns_token():
    with patch.object(gh, "get_installation_auth", return_value=_auth_mock("ghs_tok")):
        assert gh.get_installation_token("s", 1, 2) == "ghs_tok"


def test_missing_secret_raises_without_leaking():
    with patch.object(gh, "get_secret_string", side_effect=RuntimeError(SECRET_PEM)):
        with pytest.raises(gh.GitHubAppTokenError) as ei:
            gh.get_installation_auth("my/secret", 1, 2)
    assert "my/secret" in str(ei.value)
    assert "SECRETPEM" not in str(ei.value)
    assert ei.value.__cause__ is None and ei.value.__suppress_context__


def test_invalid_pem_raises_without_leaking():
    with patch.object(gh, "get_secret_string", return_value="not-a-pem"):
        with pytest.raises(gh.GitHubAppTokenError) as ei:
            gh.get_installation_token("s", 1, 2)
    assert "not-a-pem" not in str(ei.value)


def test_rejected_exchange_raises_without_leaking():
    exc = GithubException(401, {"message": "Bad credentials ghs_leak"}, {})
    with patch.object(gh, "get_installation_auth", return_value=_auth_mock(exc=exc)):
        with pytest.raises(gh.GitHubAppTokenError) as ei:
            gh.get_installation_token("s", 1, 2)
    assert "401" in str(ei.value)
    assert "ghs_leak" not in str(ei.value)


def test_unexpected_exchange_error_wrapped():
    with patch.object(
        gh, "get_installation_auth", return_value=_auth_mock(exc=ConnectionError("tok"))
    ):
        with pytest.raises(gh.GitHubAppTokenError, match="ConnectionError"):
            gh.get_installation_token("s", 1, 2)


def test_get_github_client():
    auth = MagicMock(spec=Auth.AppInstallationAuth)
    with patch.object(gh, "get_installation_auth", return_value=auth):
        assert isinstance(gh.get_github_client("s", 1, 2), Github)
