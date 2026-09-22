# overture-spark

[![PyPI](https://img.shields.io/pypi/v/overture-spark.svg)](https://pypi.org/project/overture-spark/)
[![Python versions](https://img.shields.io/pypi/pyversions/overture-spark.svg)](https://pypi.org/project/overture-spark/)

Portable, framework-agnostic runtime helpers for Overture's Spark jobs. Moved from `tf-data-platform`'s `overture_spark` module (see [ops-team#535](https://github.com/OvertureMaps/ops-team/issues/535)); other `overture_spark` modules follow in later PRs (see [ops-team#532](https://github.com/OvertureMaps/ops-team/issues/532)).

It contains:
- `secret_engines` — a `SecretsInterface` and three implementations (`Databricks`, `DatabricksNoDbutils`, `AwsSecretsManager`) for reading a named secret from a Databricks scope or AWS Secrets Manager, without the caller knowing which backend it's running on.

## Reading a secret

```python
from overture_spark.secret_engines import AwsSecretsManager

secrets = AwsSecretsManager(scope="my-secrets-arn")
token = secrets.get_secret("api_token")
```

Each engine imports its backing SDK (`databricks-sdk` or `boto3`) lazily, inside `__init__`/`get_secret` rather than at module load, so instantiating one engine never pulls in another's SDK. Install the extra matching the engine you use: `pip install overture-spark[databricks]` or `pip install overture-spark[aws]`.

## Publishing

See [`PACKAGE_VERSIONING.md`](../PACKAGE_VERSIONING.md) for how a version bump here turns into a PyPI release.
