"""Tests for overture_core.apis.github.open_pr (PyGithub fully mocked)."""

from unittest import mock

import pytest
from github import GithubException

from overture_core.apis.github import (
    bot_author,
    bot_identity,
    commit_files,
    dispatch_workflow_if_idle,
    ensure_branch,
    find_prs,
    get_app_bot,
    get_file_text,
    is_bot_commit,
    is_not_found,
    open_or_comment_issue,
    open_pr,
    truncate_for_github,
    upsert_issue,
)

PR_URL = "https://github.com/o/r/pull/1"


@pytest.fixture
def bot():
    b = mock.MagicMock()
    b.name = "Test App"
    b.login = "test-app[bot]"
    b.id = 999
    return b


@pytest.fixture
def repo():
    r = mock.MagicMock()
    r.default_branch = "main"
    r.owner.login = "o"
    r.get_branch.return_value.commit.sha = "base-sha"
    r.get_git_ref.side_effect = GithubException(404, {}, None)
    r.get_pulls.return_value = []
    r.create_pull.return_value.html_url = PR_URL
    r.create_git_tree.return_value.sha = "new-tree"
    r.get_git_commit.return_value.tree.sha = "old-tree"
    return r


def _call(repo, bot, files=None, **kwargs):
    return open_pr(
        repo,
        bot,
        branch="feature",
        files=files or {"a.txt": "A"},
        title="Title",
        body="Body",
        **kwargs,
    )


def test_new_branch_commits_and_opens_draft_pr(repo, bot):
    assert _call(repo, bot) == PR_URL

    repo.create_git_ref.assert_called_once_with(
        ref="refs/heads/feature", sha="base-sha"
    )
    repo.get_branch.assert_called_once_with("main")
    commit_args = repo.create_git_commit.call_args
    assert commit_args.args[0] == (
        "Title\n\nSigned-off-by: Test App <999+test-app[bot]@users.noreply.github.com>"
    )
    assert commit_args.kwargs["author"] is commit_args.kwargs["committer"]
    repo.create_pull.assert_called_once_with(
        title="Title", body="Body", head="feature", base="main", draft=True
    )


def test_explicit_base_and_non_draft(repo, bot):
    _call(repo, bot, base="develop", draft=False)

    repo.get_branch.assert_called_once_with("develop")
    kwargs = repo.create_pull.call_args.kwargs
    assert kwargs["base"] == "develop"
    assert kwargs["draft"] is False


def test_multiple_files_in_single_commit(repo, bot):
    _call(repo, bot, files={"a.txt": "A", "dir/b.txt": "B"})

    elements = repo.create_git_tree.call_args.args[0]
    assert [e._identity["path"] for e in elements] == ["a.txt", "dir/b.txt"]
    assert repo.create_git_commit.call_count == 1


def test_existing_branch_is_reused_and_committed_on(repo, bot):
    ref = mock.MagicMock()
    repo.get_git_ref.side_effect = None
    repo.get_git_ref.return_value = ref

    _call(repo, bot)

    repo.create_git_ref.assert_not_called()
    ref.edit.assert_called_once_with(repo.create_git_commit.return_value.sha)


def test_rerun_with_identical_files_makes_no_commit(repo, bot):
    repo.get_git_ref.side_effect = None
    repo.create_git_tree.return_value.sha = "old-tree"

    _call(repo, bot)

    repo.create_git_commit.assert_not_called()
    repo.create_pull.assert_called_once()


def test_existing_pr_is_updated_not_duplicated(repo, bot):
    pr = mock.MagicMock(html_url="https://github.com/o/r/pull/9")
    repo.get_pulls.return_value = [pr]

    assert _call(repo, bot) == "https://github.com/o/r/pull/9"

    pr.edit.assert_called_once_with(title="Title", body="Body")
    repo.create_pull.assert_not_called()
    repo.get_pulls.assert_called_once_with(state="open", head="o:feature", base="main")


def test_non_404_ref_error_is_reraised(repo, bot):
    repo.get_git_ref.side_effect = GithubException(500, {}, None)

    with pytest.raises(GithubException):
        _call(repo, bot)
    repo.create_pull.assert_not_called()


def test_bot_identity_uses_name_then_login(bot):
    assert bot_identity(bot) == (
        "Test App",
        "999+test-app[bot]@users.noreply.github.com",
    )
    bot.name = None
    assert bot_identity(bot)[0] == "test-app[bot]"


def test_get_app_bot_appends_bot_suffix():
    gh = mock.MagicMock()
    assert get_app_bot(gh, "my-app") is gh.get_user.return_value
    gh.get_user.assert_called_once_with("my-app[bot]")


def test_truncate_for_github():
    assert truncate_for_github(None) is None
    assert truncate_for_github("short", 10) == "short"
    assert truncate_for_github("x" * 5 + "tail", 4) == "...(truncated)\ntail"


class TestOpenOrCommentIssue:
    @pytest.fixture
    def gh(self):
        g = mock.MagicMock()
        g.search_issues.return_value = []
        return g

    def test_creates_issue_when_none_open(self, gh, repo):
        repo.full_name = "o/r"
        url = open_or_comment_issue(gh, repo, "T", "B", labels=("bug",))

        assert url == repo.create_issue.return_value.html_url
        repo.create_issue.assert_called_once_with(title="T", body="B", labels=["bug"])
        assert 'repo:o/r "T"' in gh.search_issues.call_args.kwargs["query"]

    def test_comments_on_exact_title_match_only(self, gh, repo):
        near = mock.MagicMock(title="T extra", state="open")
        exact = mock.MagicMock(title="T", state="open", html_url="u")
        gh.search_issues.return_value = [near, exact]

        assert open_or_comment_issue(gh, repo, "T", "B", comment="C") == "u"

        exact.create_comment.assert_called_once_with("C")
        near.create_comment.assert_not_called()
        repo.create_issue.assert_not_called()

    def test_comment_defaults_to_body_and_accepts_callable(self, gh, repo):
        issue = mock.MagicMock(title="T", state="open", number=7)
        gh.search_issues.return_value = [issue]

        open_or_comment_issue(gh, repo, "T", "B")
        issue.create_comment.assert_called_with("B")

        open_or_comment_issue(gh, repo, "T", "B", comment=lambda i: f"#{i.number}")
        issue.create_comment.assert_called_with("#7")


def test_bot_author_and_signoff(bot):
    author, signoff = bot_author(bot)
    assert author._identity["name"] == "Test App"
    assert signoff == (
        "Signed-off-by: Test App <999+test-app[bot]@users.noreply.github.com>"
    )


def test_is_not_found():
    assert is_not_found(GithubException(404, {}, None))
    assert not is_not_found(GithubException(500, {}, None))
    assert not is_not_found(ValueError())


class TestEnsureBranch:
    @pytest.fixture
    def existing(self, repo):
        ref = mock.MagicMock()
        repo.get_git_ref.side_effect = None
        repo.get_git_ref.return_value = ref
        return ref

    def test_reuse_keeps_branch(self, repo, existing):
        assert ensure_branch(repo, "b", "main") is existing
        existing.edit.assert_not_called()
        existing.delete.assert_not_called()
        repo.create_git_ref.assert_not_called()

    def test_reset_forces_to_base(self, repo, existing):
        ensure_branch(repo, "b", "main", mode="reset")
        existing.edit.assert_called_once_with("base-sha", force=True)
        existing.delete.assert_not_called()

    def test_recreate_deletes_then_creates(self, repo, existing):
        ref = ensure_branch(repo, "b", "main", mode="recreate")
        existing.delete.assert_called_once()
        assert ref is repo.create_git_ref.return_value

    def test_missing_branch_is_created_in_every_mode(self, repo):
        for mode in ("reuse", "reset", "recreate"):
            repo.create_git_ref.reset_mock()
            ensure_branch(repo, "b", "main", mode=mode)
            repo.create_git_ref.assert_called_once_with(
                ref="refs/heads/b", sha="base-sha"
            )


class TestCommitFiles:
    def test_refuses_when_branch_moved(self, repo, bot):
        ref = mock.MagicMock()
        ref.object.sha = "other"
        with pytest.raises(RuntimeError, match="moved"):
            commit_files(repo, ref, bot, {"a": "A"}, "m", expected_head="expected")
        repo.create_git_commit.assert_not_called()

    def test_commits_without_force(self, repo, bot):
        ref = mock.MagicMock()
        ref.object.sha = "head"
        assert commit_files(repo, ref, bot, {"a": "A"}, "m", expected_head="head")
        ref.edit.assert_called_once_with(repo.create_git_commit.return_value.sha)


def test_is_bot_commit(bot):
    _, email = "Test App", "999+test-app[bot]@users.noreply.github.com"
    commit = mock.MagicMock()
    commit.author.id = 999
    commit.commit.author.email = email
    commit.commit.committer.email = email
    assert is_bot_commit(commit, bot)

    commit.commit.committer.email = "human@example.com"
    assert not is_bot_commit(commit, bot)
    commit.commit.committer.email = email
    commit.author = None
    assert not is_bot_commit(commit, bot)


def test_find_prs_passes_state_and_head(repo):
    pr = mock.MagicMock()
    repo.get_pulls.return_value = [pr]
    assert find_prs(repo, "b", state="all") == [pr]
    repo.get_pulls.assert_called_once_with(state="all", head="o:b", base="main")


class TestGetFileText:
    def test_returns_text(self, repo):
        repo.get_contents.return_value.decoded_content = b"hi"
        assert get_file_text(repo, "p", "sha") == "hi"

    def test_missing_file_raises_file_not_found(self, repo):
        repo.get_contents.side_effect = GithubException(404, {}, None)
        with pytest.raises(FileNotFoundError):
            get_file_text(repo, "p", "sha")

    def test_other_errors_propagate(self, repo):
        repo.get_contents.side_effect = GithubException(500, {}, None)
        with pytest.raises(GithubException):
            get_file_text(repo, "p", "sha")


class TestUpsertIssue:
    def test_updates_matching_issue_of_creator(self, repo):
        pr_like = mock.MagicMock(title="T", pull_request=object())
        match = mock.MagicMock(title="T", pull_request=None)
        repo.get_issues.return_value = [pr_like, match]

        assert upsert_issue(repo, "T", "B", creator="app[bot]") is match

        repo.get_issues.assert_called_once_with(state="open", creator="app[bot]")
        match.edit.assert_called_once_with(body="B")
        pr_like.edit.assert_not_called()
        repo.create_issue.assert_not_called()

    def test_creates_when_no_match(self, repo):
        repo.get_issues.return_value = []
        issue = upsert_issue(repo, "T", "B", labels=("x",))
        assert issue is repo.create_issue.return_value
        repo.create_issue.assert_called_once_with(title="T", body="B", labels=["x"])
        repo.get_issues.assert_called_once_with(state="open")


class TestDispatchWorkflowIfIdle:
    def test_skips_when_run_in_flight(self, repo):
        wf = repo.get_workflow.return_value
        wf.get_runs.return_value = [
            mock.MagicMock(status="completed"),
            mock.MagicMock(status="in_progress"),
        ]
        assert dispatch_workflow_if_idle(repo, "w.yml", "main") is False
        wf.create_dispatch.assert_not_called()

    def test_dispatches_when_idle(self, repo):
        wf = repo.get_workflow.return_value
        wf.get_runs.return_value = [mock.MagicMock(status="completed")]
        assert dispatch_workflow_if_idle(repo, "w.yml", "main", {"k": "v"}) is True
        wf.create_dispatch.assert_called_once_with("main", {"k": "v"})

    def test_raises_when_dispatch_rejected(self, repo):
        wf = repo.get_workflow.return_value
        wf.get_runs.return_value = []
        wf.create_dispatch.return_value = False
        with pytest.raises(RuntimeError):
            dispatch_workflow_if_idle(repo, "w.yml", "main")


def test_open_pr_branch_mode_is_forwarded(repo, bot):
    ref = mock.MagicMock()
    repo.get_git_ref.side_effect = None
    repo.get_git_ref.return_value = ref
    _call(repo, bot, branch_mode="reset")
    ref.edit.assert_any_call("base-sha", force=True)
