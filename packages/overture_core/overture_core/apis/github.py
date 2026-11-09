"""GitHub App installation token helpers.

For code that runs outside a GitHub Actions step (Airflow DAGs, ECS tasks,
pipeline scripts) and has to mint its own installation token. The App's PEM
is read from AWS Secrets Manager, signed into a short-lived JWT, and
exchanged for an installation token. See
https://github.com/OvertureMaps/omf-github-terraform/blob/main/docs/github-app-tokens.md

Failures raise :class:`GitHubAppTokenError` with the original exception
suppressed, so the PEM and tokens never reach logs or tracebacks.
"""

import logging
from collections.abc import Mapping

from botocore.exceptions import ClientError
from github import (
    Auth,
    Github,
    GithubException,
    GithubIntegration,
    InputGitAuthor,
    InputGitTreeElement,
)
from github.NamedUser import NamedUser
from github.Repository import Repository

from overture_core.cloud.aws.secrets import get_secret_string

log = logging.getLogger(__name__)


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


def open_pr(
    repo: Repository,
    bot: NamedUser,
    branch: str,
    files: Mapping[str, str],
    title: str,
    body: str,
    base: str | None = None,
    draft: bool = True,
) -> str:
    """Commit ``files`` to ``branch`` and open (or update) a PR; return its URL.

    Safe to rerun. The branch is created from ``base`` if missing, otherwise
    reused. All files go in one commit, authored and signed off by ``bot``
    (cncf/dco2 requires the trailer email to match the author email); no
    commit is made if the files already match the branch. An open PR for the
    branch gets its title and body updated instead of a duplicate being opened.

    Args:
        repo: Target repository.
        bot: User the commit is attributed to, e.g. ``gh.get_user("app[bot]")``.
        branch: Head branch to create or reuse.
        files: Mapping of repo path to full new text content.
        title: PR title; also the commit subject.
        body: PR body.
        base: Branch to branch from and target. Defaults to the repo default.
        draft: Open a newly created PR as a draft.
    """
    base = base or repo.default_branch
    bot_name = bot.name or bot.login
    bot_email = f"{bot.id}+{bot.login}@users.noreply.github.com"
    author = InputGitAuthor(bot_name, bot_email)

    try:
        ref = repo.get_git_ref(f"heads/{branch}")
    except GithubException as e:
        if e.status != 404:
            raise
        ref = repo.create_git_ref(
            ref=f"refs/heads/{branch}", sha=repo.get_branch(base).commit.sha
        )
        log.info("Created branch %s from %s", branch, base)

    head = repo.get_git_commit(ref.object.sha)
    tree = repo.create_git_tree(
        [
            InputGitTreeElement(path, "100644", "blob", content=content)
            for path, content in files.items()
        ],
        base_tree=head.tree,
    )
    if tree.sha != head.tree.sha:
        commit = repo.create_git_commit(
            f"{title}\n\nSigned-off-by: {bot_name} <{bot_email}>",
            tree,
            [head],
            author=author,
            committer=author,
        )
        ref.edit(commit.sha)

    pulls = repo.get_pulls(state="open", head=f"{repo.owner.login}:{branch}", base=base)
    for pr in pulls:
        pr.edit(title=title, body=body)
        log.info("Updated existing PR: %s", pr.html_url)
        return pr.html_url

    pr = repo.create_pull(title=title, body=body, head=branch, base=base, draft=draft)
    log.info("Created PR: %s", pr.html_url)
    return pr.html_url
