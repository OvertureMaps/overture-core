# overture-spark

[![PyPI](https://img.shields.io/pypi/v/overture-spark.svg)](https://pypi.org/project/overture-spark/)
[![Python versions](https://img.shields.io/pypi/pyversions/overture-spark.svg)](https://pypi.org/project/overture-spark/)

Portable, framework-agnostic runtime helpers for Overture's Spark jobs. Moved from `tf-data-platform`'s `overture_spark` module (see [ops-team#535](https://github.com/OvertureMaps/ops-team/issues/535), [ops-team#536](https://github.com/OvertureMaps/ops-team/issues/536)); other `overture_spark` modules follow in later PRs (see [ops-team#532](https://github.com/OvertureMaps/ops-team/issues/532)).

It contains:
- `SparkSedonaJob` (in `job.py`) — cluster-side base class for jobs: platform detection, parameter parsing, logging, and the Sedona `SparkSession` lifecycle. Subclass it and implement `execute_job()`.
- `test_area` — helpers for a job's optional `test_area` param (a bbox or WKT polygon that scopes a dev/test run to a small region instead of the full planet).
- `s3` — S3 credentials for Python clients (boto3, PyArrow) that need the same access as Spark's S3A, e.g. on Wherobots, where only S3A gets the storage-integration role.

## Writing a job

```python
from overture_spark.job import SparkSedonaJob


class CollectionJob(SparkSedonaJob):
    def execute_job(self) -> None:
        path = self.get_param("input_path")
        self.log(f"Processing {path}")
        df = self.apply_test_area_filter(self.spark.read.parquet(path))
        # ... your logic
```

`getSparkSedonaSession` (in `overture_spark/__init__.py`) is the one call in this package that actually needs a real Spark/Sedona session — it, and everything downstream of it, imports `pyspark`/`apache-sedona` lazily and only there. Platform detection, version/JAR helpers, and `SparkSedonaJob`'s parameter/logging logic are plain Python and importable without either installed.

## Reaching S3 from Python

Spark's S3A can have per-bucket credentials (`fs.s3a.bucket.<bucket>.*`) that boto3 and PyArrow don't see. Resolve them on the driver, ship the picklable `S3Access` to Python workers, and build clients there:

```python
from overture_spark.s3 import boto3_session, pyarrow_s3_filesystem, resolve_s3_access

access = resolve_s3_access(spark, "s3a://bucket/prefix")  # driver only


def write_partition(batches):
    filesystem = pyarrow_s3_filesystem(access)  # paths are "bucket/key"
    s3 = boto3_session(access).client("s3")
    ...
```

`resolve_s3_access` returns S3A's assumed role (`assumed.role.arn`), else its static keys (`access.key`/`secret.key`), plus `endpoint.region`, each preferring the per-bucket setting; a bucket's own keys also win over a global role. Clients assume the role with the Wherobots external ID (`WBC_STINT_ASSUME_ROLE_EXTERNAL_ID`, if set) and refresh it; with nothing configured (e.g. Glue) they use the default credential chain. Clients are cached per process. `boto3`/`pyarrow` are imported lazily: install them yourself or with `overture-spark[s3]`.

## Testing locally

```bash
cd packages/overture_spark
uv sync --extra dev
uv run pytest -v -m "not spark"
```

Tests marked `@pytest.mark.spark` need a real Spark/Sedona session (a JVM, plus Sedona's native JAR resolution), so they're excluded from the routine `pytest` run above and from `overture-spark[dev]`. Install the `sql-spark` extra to run them:

```bash
uv sync --extra dev --extra sql-spark
uv run pytest -v --cov --cov-fail-under=95
```

CI mirrors this split: the routine `Test Python` workflow runs `pytest -m "not spark"` for this package (no coverage gate, since the Spark-only code paths aren't exercised), and the `SQL Engines` workflow's `spark (overture_spark)` job installs the `sql-spark` extra and Java, then runs the full suite with the 95% coverage floor.

## Publishing

See [`PACKAGE_VERSIONING.md`](../PACKAGE_VERSIONING.md) for how a version bump here turns into a PyPI release.
