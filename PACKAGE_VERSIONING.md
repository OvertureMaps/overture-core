# Package versioning and publishing

Each top-level package (`overture_core`, `overture_serverless`, ...) is versioned and released independently.

## Version bump

Bump `version` in the package's `pyproject.toml` in the same PR as the change. Follow [semver](https://semver.org/).

## Release

A release is a git tag of the form `<package_dir>-v<version>`, e.g. `overture_core-v0.2.0`. Pushing that tag runs [`publish_packages.yml`](.github/workflows/publish_packages.yml), which:

1. Parses the package directory and version out of the tag.
2. Verifies the tag version matches `pyproject.toml` in that directory (fails closed if they've drifted).
3. Builds the package with `uv build`.
4. Publishes to PyPI via [trusted publishing](https://docs.pypi.org/trusted-publishers/) — no stored credentials.

Cut a release with:

```bash
git tag overture_core-v0.2.0
git push origin overture_core-v0.2.0
```

## Trusted publishing setup (one-time per package)

Each package publishes through its own GitHub Environment, named `pypi-<package_dir>` (e.g. `pypi-overture_core`), so PyPI's trusted-publisher config can scope access per package. When adding a new package:

1. Create the PyPI project (or use `--dry-run` on first publish to reserve it).
2. On the project's PyPI settings, add a trusted publisher: this repo, workflow `publish_packages.yml`, environment `pypi-<package_dir>`.
3. Add a matching `pypi-<package_dir>` environment in this repo's GitHub settings (no secrets required).

## Adding a new package

Auto-discovered testable packages need no CI changes — see [`CONTRIBUTING.md`](CONTRIBUTING.md). Publishing does need the one-time trusted-publisher setup above, since PyPI has no auto-discovery of new projects.
