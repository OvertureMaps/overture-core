# Contributing

This is a monorepo: each top-level directory with a `pyproject.toml` and a `tests/` directory is an independent, publishable Python package, managed with [`uv`](https://docs.astral.sh/uv/).

## Adding a new package

```bash
mkdir my_package && cd my_package
uv init --lib --name my-package
```

A package needs, at minimum:
- `pyproject.toml`: standard `uv`/setuptools project config, plus dynamic versioning and release config (see [Versioning](#versioning) below).
- `.python-version`: the Python version CI installs for this package.
- `my_package/`: the importable package.
- `tests/`: pytest suite (`testpaths = ["tests"]` in `pyproject.toml`).
- `README.md`: what it's for, how to use it.

That's it: [`.github/workflows/test_python.yml`](.github/workflows/test_python.yml) and [`.github/workflows/publish_packages.yml`](.github/workflows/publish_packages.yml) auto-discover any directory matching that shape, no workflow edits required. [`.github/workflows/lint.yml`](.github/workflows/lint.yml) covers the whole repo through the root `pyproject.toml`'s `ruff` config.

### Versioning

There's no `version` field to maintain by hand. Each package uses [`setuptools-scm`](https://setuptools-scm.readthedocs.io/) for dynamic, tag-derived versioning and [python-semantic-release](https://python-semantic-release.readthedocs.io/) to decide *when* and *what* to release from your PR title, see [`PACKAGE_VERSIONING.md`](PACKAGE_VERSIONING.md) for the full flow. **PRs are squash-merged, so the PR title must itself be Conventional Commits** (e.g. `fix: handle empty release list`), that's the line PSR parses, and PSR figures out which package(s) it applies to from which files the commit touched, not from a scope. A new package needs:

```toml
[project]
name = "my-package"
dynamic = ["version"]

[build-system]
requires = ["setuptools>=64.0", "setuptools_scm>=8.0", "wheel"]
build-backend = "setuptools.build_meta"

[tool.setuptools_scm]
root = ".."
tag_regex = "^my_package-v(?P<version>.*)$"
git_describe_command = "git describe --dirty --tags --long --match 'my_package-v*'"

[tool.semantic_release]
commit_parser = "conventional-monorepo"
tag_format = "my_package-v{version}"
allow_zero_version = true

[tool.semantic_release.commit_parser_options]
path_filters = ["."]
```

Publishing a new package to PyPI also needs a one-time trusted-publisher setup, see [`PACKAGE_VERSIONING.md`](PACKAGE_VERSIONING.md).

### Coverage floor

CI fails a package's tests if coverage drops below **95%**. Add `pytest-cov` as a dev dependency and set the floor in `pyproject.toml`:

```toml
[project.optional-dependencies]
dev = ["pytest>=9.1.1", "pytest-cov>=7.0.0"]

[tool.coverage.run]
source = ["my_package"]

[tool.coverage.report]
fail_under = 95
show_missing = true
```

## Cross-package dependencies

Reference a sibling package by version range in `dependencies`, and add a `[tool.uv.sources]` path override so local dev/test resolves it from the checkout instead of PyPI:

```toml
dependencies = ["overture-serverless>=0.1.0"]

[tool.uv.sources]
overture-serverless = { path = "../overture_serverless" }
```

The `tool.uv.sources` override is dev-only: it never ends up in the built package's metadata, so published wheels resolve the dependency from PyPI like anyone else's install would.

## Local development

```bash
cd my_package
uv sync --extra dev
uv run pytest -v
uv run ruff format .
uv run ruff check .
```

Install the pre-commit hook once per clone if you want formatting caught before you push:

```bash
uv tool install pre-commit
pre-commit install
```
