"""GitHub App installation token helpers.

For code that runs outside a GitHub Actions step (Airflow DAGs, ECS tasks,
pipeline scripts) and has to mint its own installation token. The App's PEM
is read from AWS Secrets Manager, signed into a short-lived JWT, and
exchanged for an installation token. See
https://github.com/OvertureMaps/omf-github-terraform/blob/main/docs/github-app-tokens.md

Failures raise :class:`GitHubAppTokenError` with the original exception
suppressed, so the PEM and tokens never reach logs or tracebacks.
"""

from botocore.exceptions import ClientError
from github import Auth, Github, GithubException, GithubIntegration

from overture_core.cloud.aws.secrets import get_secret_string


class GitHubAppTokenError(RuntimeError):
    """Raised when the App PEM cannot be read or the token exchange fails."""


def _read_private_key(secret_name: str, region: str | None) -> str:
    try:
        return get_secret_string(secret_name, region=region)
    except Exception as exc:
        # The AWS error code (e.g. AccessDeniedException) is not secret and
        # tells a missing secret from a missing permission or wrong region.
        detail = (
            exc.response.get("Error", {}).get("Code", "ClientError")
            if isinstance(exc, ClientError)
            else type(exc).__name__
        )
        raise GitHubAppTokenError(
            f"Could not read GitHub App private key from secret {secret_name!r} "
            f"({detail})."
        ) from None


def _sanitized_exchange_error(exc: Exception) -> GitHubAppTokenError:
    if isinstance(exc, GithubException):
        msg = f"GitHub rejected the installation token exchange (HTTP {exc.status})."
    else:
        msg = f"Installation token exchange failed ({type(exc).__name__})."
    return GitHubAppTokenError(msg)


class _InstallationAuth(Auth.AppInstallationAuth):
    """``AppInstallationAuth`` whose token acquisition and refresh raise
    :class:`GitHubAppTokenError` instead of leaking response content."""

    @property
    def token(self) -> str:
        try:
            return super().token
        except Exception as exc:
            raise _sanitized_exchange_error(exc) from None


def get_installation_auth(
    secret_name: str,
    app_id: int | str,
    installation_id: int,
    region: str | None = None,
) -> Auth.AppInstallationAuth:
    """Return a PyGithub ``AppInstallationAuth`` for an installed GitHub App.

    The installation token is minted lazily by PyGithub on first use and
    refreshed on expiry. A failed exchange at either point raises
    :class:`GitHubAppTokenError`. The returned auth only works once handed to
    a ``Github`` client (see :func:`get_github_client`); use
    :func:`get_installation_token` for a plain token string.

    Args:
        secret_name: Secrets Manager name or ARN holding the App's PEM.
        app_id: GitHub App ID or client ID (used as the JWT issuer).
        installation_id: Installation ID of the App on the target org/repo.
        region: Region of the secret. Defaults to boto3's own resolution.
    """
    private_key = _read_private_key(secret_name, region)
    # PyGithub < 2.7 with newer PyJWT rejects a non-string issuer.
    return _InstallationAuth(Auth.AppAuth(str(app_id), private_key), installation_id)


def get_installation_token(
    secret_name: str,
    app_id: int | str,
    installation_id: int,
    region: str | None = None,
) -> str:
    """Return a plain installation token for git HTTPS clones or raw REST calls.

    Tokens last about an hour; mint a new one rather than caching long-term.
    """
    private_key = _read_private_key(secret_name, region)
    try:
        with GithubIntegration(auth=Auth.AppAuth(str(app_id), private_key)) as gi:
            return gi.get_access_token(installation_id).token
    except Exception as exc:
        raise _sanitized_exchange_error(exc) from None


def get_github_client(
    secret_name: str,
    app_id: int | str,
    installation_id: int,
    region: str | None = None,
) -> Github:
    """Return a ``Github`` client authenticated as the App installation."""
    return Github(
        auth=get_installation_auth(secret_name, app_id, installation_id, region)
    )
