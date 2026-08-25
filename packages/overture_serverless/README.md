# overture-serverless

[![PyPI](https://img.shields.io/pypi/v/overture-serverless.svg)](https://pypi.org/project/overture-serverless/)
[![Python versions](https://img.shields.io/pypi/pyversions/overture-serverless.svg)](https://pypi.org/project/overture-serverless/)

Base class for portable, framework-agnostic "job" classes. A published package that gets installed at run time wherever a job actually executes, and provides the contract your job subclasses.

It contains:
- `ServerlessPythonJob` — abstract base class your jobs subclass (`execute_job()`), with parameter parsing (`get_param()`) and logging (`log()`). Plain Python, no cloud dependencies — runs identically on any backend or on your laptop.

## Writing a job

```python
from overture_serverless.job import ServerlessPythonJob


class CollectionJob(ServerlessPythonJob):
    def execute_job(self) -> None:
        path = self.get_param("input_path")
        self.log(f"Processing {path}")
        # ... your logic
```

## Testing locally

```bash
cd packages/overture_serverless
uv run python -c "
from overture_serverless.job import ServerlessPythonJob

class ExampleJob(ServerlessPythonJob):
    def execute_job(self) -> None:
        self.log(self.get_param('input_path'))

ExampleJob().run('{\"input_path\": \"s3://...\"}')
"
```

## Publishing

See [`PACKAGE_VERSIONING.md`](../PACKAGE_VERSIONING.md) for how a version bump here turns into a PyPI release.
