"""Tests for overture_core.apis.github.open_pr (PyGithub fully mocked)."""

from unittest import mock

import pytest
from github import GithubException

from overture_core.apis.github import open_pr

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
