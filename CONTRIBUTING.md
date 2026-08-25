# Contributing

This is a monorepo: each top-level directory with a `pyproject.toml` and a `tests/` directory is an independent, publishable Python package, managed with [`uv`](https://docs.astral.sh/uv/).

## Adding a new package

```bash
mkdir my_package && cd my_package
uv init --lib --name my-package
```

A package needs, at minimum:
- `pyproject.toml` — standard `uv`/setuptools project config. Start `version` at `0.1.0`.
- `.python-version` — the Python version CI installs for this package.
- `my_package/` — the importable package.
- `tests/` — pytest suite (`testpaths = ["tests"]` in `pyproject.toml`).
- `README.md` — what it's for, how to use it.

That's it — [`.github/workflows/test_python.yml`](.github/workflows/test_python.yml) auto-discovers any directory matching that shape and runs `uv run pytest -v` against it, no workflow edits required. [`.github/workflows/lint.yml`](.github/workflows/lint.yml) covers the whole repo through the root `pyproject.toml`'s `ruff` config.

Publishing a new package to PyPI does need a one-time trusted-publisher setup — see [`PACKAGE_VERSIONING.md`](PACKAGE_VERSIONING.md).

## Cross-package dependencies

Reference a sibling package by version range in `dependencies`, and add a `[tool.uv.sources]` path override so local dev/test resolves it from the checkout instead of PyPI:

```toml
dependencies = ["overture-serverless>=0.1.0"]

[tool.uv.sources]
overture-serverless = { path = "../overture_serverless" }
```

The `tool.uv.sources` override is dev-only — it never ends up in the built package's metadata, so published wheels resolve the dependency from PyPI like anyone else's install would.

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
