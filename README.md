# overture-core

SDK everything builds on: bundle/provenance model, config, check framework, platform abstractions, registry client.

A monorepo of independently versioned, independently published Python packages. Each top-level directory with a `pyproject.toml` and a `tests/` directory is one package.

## Packages

| Package | Description |
| --- | --- |
| [`overture_core`](overture_core) | Shared, framework-agnostic business logic — portable job classes. |
| [`overture_serverless`](overture_serverless) | Base class for portable, framework-agnostic serverless jobs. |

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for how to add a package, wire up cross-package dependencies, and run tests/lint locally. See [`PACKAGE_VERSIONING.md`](PACKAGE_VERSIONING.md) for how a version bump turns into a PyPI release.

## Maintainers

This repository uses `MAINTAINERS.md` files to track ownership for [LFX Insights](https://insights.linuxfoundation.org/docs/introduction/maintainers/) ingestion. LFX scans the full repository tree, so these files can live anywhere.

To add a `MAINTAINERS.md` for a module or package, create it in the relevant directory using this format:

```markdown
# MAINTAINERS

| Name     | GitHub Username | Role            | Affiliation |
| -------- | --------------- | --------------- | ----------- |
| Jane Doe | @janedoe        | Lead Maintainer | Org         |
```
