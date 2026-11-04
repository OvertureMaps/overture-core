![overture-core: shared libraries for the Overture data platform](docs/assets/hero.svg)

[![Test Python](https://github.com/OvertureMaps/overture-core/actions/workflows/test_python.yml/badge.svg?branch=main)](https://github.com/OvertureMaps/overture-core/actions/workflows/test_python.yml)
[![Lint](https://github.com/OvertureMaps/overture-core/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/OvertureMaps/overture-core/actions/workflows/lint.yml)
[![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-4051CC?labelColor=001A39)](.python-version)
[![uv](https://img.shields.io/badge/managed%20with-uv-0EC1BD?labelColor=001A39)](https://docs.astral.sh/uv/)
[![Ruff](https://img.shields.io/badge/lint-ruff-0EC1BD?labelColor=001A39)](https://docs.astral.sh/ruff/)
[![License: MIT](https://img.shields.io/badge/license-MIT-05A5AF?labelColor=001A39)](LICENSE)

Shared Python libraries for the Overture data platform: portable job classes and framework-agnostic helpers for cloud, SQL, STAC and third-party APIs.

A monorepo of independently versioned, independently published packages. Each directory under [`packages/`](packages) with a `pyproject.toml` and a `tests/` directory is one package.

## Packages

| Package | Description | PyPI |
| --- | --- | --- |
| [`overture_core`](packages/overture_core) | Shared, framework-agnostic business logic: helpers and portable job classes. | [![PyPI](https://img.shields.io/pypi/v/overture-core?color=4051CC&labelColor=001A39)](https://pypi.org/project/overture-core/) |
| [`overture_serverless`](packages/overture_serverless) | Base classes for portable, framework-agnostic serverless jobs. | [![PyPI](https://img.shields.io/pypi/v/overture-serverless?color=4051CC&labelColor=001A39)](https://pypi.org/project/overture-serverless/) |
| [`overture_spark`](packages/overture_spark) | Portable, framework-agnostic Spark job runtime helpers. | [![PyPI](https://img.shields.io/pypi/v/overture-spark?color=4051CC&labelColor=001A39)](https://pypi.org/project/overture-spark/) |

```mermaid
%%{init: {"theme": "dark", "flowchart": {"padding": 14}, "themeVariables": {"fontSize": "14px", "fontFamily": "Montserrat, Arial, sans-serif", "mainBkg": "#001A39", "nodeBorder": "#0EC1BD", "lineColor": "#4051CC", "clusterBkg": "#14205A", "clusterBorder": "#4051CC", "primaryTextColor": "#FFFFFF", "edgeLabelBackground": "#001A39"}}}%%
flowchart LR
    classDef default rx:8,ry:8,stroke-width:0.75px
    C[overture_core] -->|depends on| S[overture_serverless]
    P[overture_spark]
```

## What lives here (and what doesn't)

Libraries only: code that runs the same under Airflow, ECS, Glue, Databricks or a laptop. Each package is published to PyPI and consumed as a pinned version.

Does **not** belong here:

- DAGs, schedules and cloud topology: those live in the orchestration repo, `overture-pipelines`.
- Infrastructure as code and deployment config.
- Anything consumed via a git ref. Downstream consumption is published packages only.

## CI

- **PR:** [Test Python](.github/workflows/test_python.yml) and [Lint](.github/workflows/lint.yml) cover every package that matches the shape above; no workflow edits are needed when one is added. [SQL Engines](.github/workflows/sql_engines.yml) runs the Spark/Trino suites, and [MWAA Compatibility](.github/workflows/mwaa_compat.yml) checks that `overture_core` still installs under MWAA's pinned constraints.
- **Merge to main:** [Publish Python Packages](.github/workflows/publish_packages.yml) releases each changed package. PR titles must be Conventional Commits because PRs are squash-merged and the title decides the version bump.

## Local development

Requires [uv](https://docs.astral.sh/uv/) and Python 3.11+.

```sh
cd packages/overture_core
uv sync --extra dev
uv run pytest
```

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for how to add a package, wire up cross-package dependencies, and run tests/lint locally. See [`PACKAGE_VERSIONING.md`](PACKAGE_VERSIONING.md) for how a version bump turns into a PyPI release.
