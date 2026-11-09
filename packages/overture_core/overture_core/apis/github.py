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
from collections.abc import Callable, Mapping, Sequence
from typing import Literal

from botocore.exceptions import ClientError
from github import (
    Auth,
    Github,
    GithubException,
    GithubIntegration,
    InputGitAuthor,
    InputGitTreeElement,
)
from github.Commit import Commit
from github.GitRef import GitRef
from github.Issue import Issue
from github.PullRequest import PullRequest
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


def get_app_bot(gh: Github, app_slug: str) -> NamedUser:
    """Return the bot user of the GitHub App ``app_slug`` (e.g. ``my-app``)."""
    return gh.get_user(f"{app_slug}[bot]")


def bot_identity(bot: NamedUser) -> tuple[str, str]:
    """Return the ``(name, email)`` to use as commit author and sign-off.

    cncf/dco2 fails a PR whose ``Signed-off-by`` email differs from the commit
    author email, so both sides must use this one identity.
    """
    return bot.name or bot.login, f"{bot.id}+{bot.login}@users.noreply.github.com"


def bot_author(bot: NamedUser) -> tuple[InputGitAuthor, str]:
    """Return the ``(author, signoff)`` pair for a commit made as ``bot``.

    Pass ``author`` as both ``author=`` and ``committer=``, and append
    ``signoff`` (a ``Signed-off-by:`` line) to the commit message.
    """
    name, email = bot_identity(bot)
    return InputGitAuthor(name, email), f"Signed-off-by: {name} <{email}>"


def is_not_found(exc: BaseException) -> bool:
    """Whether ``exc`` is a GitHub 404."""
    return isinstance(exc, GithubException) and exc.status == 404


def truncate_for_github(text: str | None, max_chars: int = 60000) -> str | None:
    """Keep the tail of ``text`` within GitHub's ~65k char comment body limit."""
    if not text or len(text) <= max_chars:
        return text
    return "...(truncated)\n" + text[-max_chars:]


def find_open_issue(gh: Github, repo: Repository, title: str) -> Issue | None:
    """Return the open issue in ``repo`` whose title is exactly ``title``.

    Matches on title only, not labels: label automation can replace labels
    after triage, and filtering on them would duplicate triaged issues.
    """
    escaped = title.replace('"', " ")
    query = f'is:issue is:open in:title repo:{repo.full_name} "{escaped}"'
    for issue in gh.search_issues(query=query):
        if issue.title == title and issue.state == "open":
            return issue
    return None


def open_or_comment_issue(
    gh: Github,
    repo: Repository,
    title: str,
    body: str,
    comment: str | Callable[[Issue], str] | None = None,
    labels: Sequence[str] = (),
) -> str:
    """Open an issue titled ``title``, or comment on the open one; return its URL.

    Args:
        gh: Client used for the search API.
        repo: Target repository.
        title: Issue title; also the dedupe key (exact match among open issues).
        body: Body for a newly created issue.
        comment: Comment for an existing issue: text, or a callable given the
            issue (e.g. to number occurrences). Defaults to ``body``.
        labels: Labels applied only when creating the issue.
    """
    issue = find_open_issue(gh, repo, title)
    if issue is None:
        issue = repo.create_issue(title=title, body=body, labels=list(labels))
        log.info("Created issue: %s", issue.html_url)
    else:
        text = comment(issue) if callable(comment) else comment or body
        issue.create_comment(text)
        log.info("Commented on issue: %s", issue.html_url)
    return issue.html_url


def upsert_issue(
    repo: Repository,
    title: str,
    body: str,
    labels: Sequence[str] = (),
    creator: str | None = None,
) -> Issue:
    """Create an issue, or replace the body of the open one with this title.

    Unlike :func:`open_or_comment_issue` this uses the issues list API, so it
    can restrict matches to issues opened by ``creator`` (e.g. ``"app[bot]"``)
    and never edits a human's issue that happens to share the title. Pull
    requests are skipped. Labels apply only on creation.
    """
    kwargs = {"creator": creator} if creator else {}
    for issue in repo.get_issues(state="open", **kwargs):
        if issue.pull_request is None and issue.title == title:
            issue.edit(body=body)
            log.info("Updated issue: %s", issue.html_url)
            return issue
    issue = repo.create_issue(title=title, body=body, labels=list(labels))
    log.info("Created issue: %s", issue.html_url)
    return issue


def get_file_text(repo: Repository, path: str, ref: str) -> str:
    """Return the text of ``path`` at ``ref`` (branch, tag or SHA).

    Raises:
        FileNotFoundError: The path does not exist at ``ref``.
    """
    try:
        return repo.get_contents(path, ref=ref).decoded_content.decode("utf-8")
    except GithubException as e:
        if is_not_found(e):
            raise FileNotFoundError(f"{repo.full_name}:{path}@{ref}") from e
        raise


def ensure_branch(
    repo: Repository,
    branch: str,
    base: str | None = None,
    mode: Literal["reuse", "reset", "recreate"] = "reuse",
) -> GitRef:
    """Return the ref of ``branch``, creating it from ``base`` if missing.

    ``mode`` decides what happens when the branch already exists:

    - ``reuse``: keep it and its history.
    - ``reset``: force-move it to the tip of ``base``, discarding its commits.
    - ``recreate``: delete it, then create it fresh. GitHub closes any open PR
      whose head was the deleted branch.
    """
    base = base or repo.default_branch
    try:
        ref = repo.get_git_ref(f"heads/{branch}")
    except GithubException as e:
        if not is_not_found(e):
            raise
    else:
        if mode == "reuse":
            return ref
        if mode == "reset":
            ref.edit(repo.get_branch(base).commit.sha, force=True)
            log.info("Reset branch %s to %s", branch, base)
            return repo.get_git_ref(f"heads/{branch}")
        ref.delete()
        log.info("Deleted stale branch %s", branch)

    ref = repo.create_git_ref(
        ref=f"refs/heads/{branch}", sha=repo.get_branch(base).commit.sha
    )
    log.info("Created branch %s from %s", branch, base)
    return ref


def commit_files(
    repo: Repository,
    ref: GitRef,
    bot: NamedUser,
    files: Mapping[str, str],
    message: str,
    expected_head: str | None = None,
) -> bool:
    """Add one signed-off commit of ``files`` on top of ``ref``; return whether made.

    Appends only: the ref moves without force, so a commit pushed concurrently
    makes the update fail instead of being overwritten. When ``expected_head``
    is given, the commit is refused unless the branch is still at that SHA.
    Nothing is committed if the files already match the branch tip.

    Raises:
        RuntimeError: ``expected_head`` no longer matches the branch tip.
    """
    if expected_head is not None and ref.object.sha != expected_head:
        raise RuntimeError(
            f"{ref.ref} moved: expected {expected_head}, found {ref.object.sha}."
        )
    author, signoff = bot_author(bot)
    head = repo.get_git_commit(ref.object.sha)
    tree = repo.create_git_tree(
        [
            InputGitTreeElement(path, "100644", "blob", content=content)
            for path, content in files.items()
        ],
        base_tree=head.tree,
    )
    if tree.sha == head.tree.sha:
        return False
    commit = repo.create_git_commit(
        f"{message}\n\n{signoff}", tree, [head], author=author, committer=author
    )
    ref.edit(commit.sha)
    return True


def is_bot_commit(commit: Commit, bot: NamedUser) -> bool:
    """Whether ``commit`` was authored and committed by ``bot``.

    Matches the account id and the :func:`bot_identity` email, so a human's
    edit on a bot-owned branch can be detected before overwriting it.
    """
    _, email = bot_identity(bot)
    return (
        commit.author is not None
        and commit.author.id == bot.id
        and commit.commit.author.email == email
        and commit.commit.committer.email == email
    )


def find_prs(
    repo: Repository,
    branch: str,
    base: str | None = None,
    state: Literal["open", "closed", "all"] = "open",
) -> list[PullRequest]:
    """Return PRs whose head is ``branch`` in this repo and that target ``base``."""
    return list(
        repo.get_pulls(
            state=state,
            head=f"{repo.owner.login}:{branch}",
            base=base or repo.default_branch,
        )
    )


def dispatch_workflow_if_idle(
    repo: Repository,
    workflow: str | int,
    ref: str,
    inputs: Mapping[str, str] | None = None,
) -> bool:
    """Dispatch ``workflow`` unless one of its 10 latest runs is still in flight.

    Args:
        workflow: Workflow file name (``"publish.yml"``) or ID.
        ref: Branch or tag to run on.
        inputs: ``workflow_dispatch`` inputs.

    Returns:
        True if dispatched, False if a run was already in flight.

    Raises:
        RuntimeError: GitHub did not accept the dispatch.
    """
    wf = repo.get_workflow(workflow)
    if any(run.status != "completed" for run in wf.get_runs()[:10]):
        log.info("Workflow %s already running; not dispatching", workflow)
        return False
    if not wf.create_dispatch(ref, dict(inputs or {})):
        raise RuntimeError(f"GitHub rejected dispatch of workflow {workflow!r}.")
    return True


def open_pr(
    repo: Repository,
    bot: NamedUser,
    branch: str,
    files: Mapping[str, str],
    title: str,
    body: str,
    base: str | None = None,
    draft: bool = True,
    branch_mode: Literal["reuse", "reset", "recreate"] = "reuse",
) -> str:
    """Commit ``files`` to ``branch`` and open (or update) a PR; return its URL.

    Safe to rerun. Composes :func:`ensure_branch` and :func:`commit_files`
    (one signed-off commit authored by ``bot``, none if nothing changed). An
    open PR for the branch gets its title and body updated instead of a
    duplicate being opened. Callers needing a different PR policy (e.g.
    refusing to reopen a closed PR) should use the building blocks directly.

    Args:
        repo: Target repository.
        bot: User the commit is attributed to, see :func:`get_app_bot`.
        branch: Head branch to create or reuse.
        files: Mapping of repo path to full new text content.
        title: PR title; also the commit subject.
        body: PR body.
        base: Branch to branch from and target. Defaults to the repo default.
        draft: Open a newly created PR as a draft.
        branch_mode: What to do with an existing branch, see :func:`ensure_branch`.
    """
    base = base or repo.default_branch
    ref = ensure_branch(repo, branch, base, branch_mode)
    commit_files(repo, ref, bot, files, title)

    for pr in find_prs(repo, branch, base):
        pr.edit(title=title, body=body)
        log.info("Updated existing PR: %s", pr.html_url)
        return pr.html_url

    pr = repo.create_pull(title=title, body=body, head=branch, base=base, draft=draft)
    log.info("Created PR: %s", pr.html_url)
    return pr.html_url
