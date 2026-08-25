# Package versioning and publishing

Each top-level package (`overture_core`, `overture_serverless`, ...) is versioned and released independently, and fully automatically — there's no version to bump and no tag to create by hand.

## Version bump

There is no `version` field to edit. Each package's `pyproject.toml` declares `dynamic = ["version"]`; [`setuptools-scm`](https://setuptools-scm.readthedocs.io/) resolves the version from that package's latest `<package_dir>-v<version>` git tag at build time.

What you do control is the version *bump*, via [Conventional Commits](https://www.conventionalcommits.org/) on your PR:

| Commit prefix                                              | Bump  |
| ------------------------------------------------------------ | ----- |
| `fix(<package_dir>): ...`                                   | patch |
| `feat(<package_dir>): ...`                                  | minor |
| `feat(<package_dir>)!: ...` / a `BREAKING CHANGE:` footer   | major |

The `<package_dir>` scope (e.g. `fix(overture_core): ...`) is what tells [python-semantic-release](https://python-semantic-release.readthedocs.io/) (PSR) which package a commit belongs to — commits without a matching scope don't trigger a release for that package.

### PR title = the commit PSR reads

This repo's PRs are squash-merged, and GitHub's default squash-commit subject is the PR title verbatim. So the **PR title itself** must be a properly scoped Conventional Commit — e.g. `fix(overture_core): handle empty release list` — not the org's generic `[TYPE] Description` convention. Get the scope right or the release silently doesn't happen.

## Release

On every push to `main`, [`publish_packages.yml`](.github/workflows/publish_packages.yml) runs PSR once per package. For each package, PSR looks at the commits since its last release tag and, if any warrant a bump:

1. Computes the next version and creates + pushes a `<package_dir>-v<version>` tag (e.g. `overture_core-v0.2.0`) — **no commit is ever made back to `main`**, and no `CHANGELOG.md` is committed to the repo. Release notes are published to the tag's [GitHub Release](https://docs.github.com/en/repositories/releasing-projects-on-github) instead.
2. Builds the package with `uv build` (`setuptools-scm` picks up the version it just tagged).
3. Publishes to PyPI via [trusted publishing](https://docs.pypi.org/trusted-publishers/) — no stored credentials.

A package with no PR-worthy commits since its last release is simply skipped — the workflow runs for every package on every push, but only tags/publishes the ones that changed.

## Trusted publishing setup (one-time per package)

Each package publishes through its own GitHub Environment, named `pypi-<package_dir>` (e.g. `pypi-overture_core`), so PyPI's trusted-publisher config can scope access per package. When adding a new package:

1. Create the PyPI project (or use `--dry-run` on first publish to reserve it).
2. On the project's PyPI settings, add a trusted publisher: this repo, workflow `publish_packages.yml`, environment `pypi-<package_dir>`.
3. Add a matching `pypi-<package_dir>` environment in this repo's GitHub settings (no secrets required).

## Adding a new package

Auto-discovered testable packages need no CI changes — see [`CONTRIBUTING.md`](CONTRIBUTING.md) for the `pyproject.toml` shape (`dynamic = ["version"]`, `[tool.setuptools_scm]`, `[tool.semantic_release]`) each package needs for this to work. Publishing does need the one-time trusted-publisher setup above, since PyPI has no auto-discovery of new projects.
