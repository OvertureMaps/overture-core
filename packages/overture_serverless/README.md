# overture-serverless

[![PyPI](https://img.shields.io/pypi/v/overture-serverless.svg)](https://pypi.org/project/overture-serverless/)
[![Python versions](https://img.shields.io/pypi/pyversions/overture-serverless.svg)](https://pypi.org/project/overture-serverless/)

Base class for portable, framework-agnostic "job" classes, plus the Airflow-facing backends that launch them. A published package that gets installed at run time wherever a job actually executes, and provides the contract your job subclasses.

It contains:
- `ServerlessPythonJob` — abstract base class your jobs subclass (`execute_job()`), with parameter parsing (`get_param()`) and logging (`log()`). Plain Python, no cloud dependencies — runs identically on any backend or on your laptop.
- `backends.fargate.serverless_python_task_group` — an Airflow `TaskGroup` factory that runs a job on AWS ECS Fargate. Requires the `fargate` extra (`pip install overture-serverless[fargate]`), which pulls in `apache-airflow` and `apache-airflow-providers-amazon`; the base install stays dependency-free since `ServerlessPythonJob` runs inside the job container, not inside Airflow.
- `backends.fargate.fargate_task_group` — the generic Fargate lifecycle layer `serverless_python_task_group` is built on: register → run → teardown for *any* container image and command, with no pip-install or module-dispatch assumptions. Same `fargate` extra.

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

## Launching a job on Fargate

`serverless_python_task_group` builds an Airflow `TaskGroup` that provisions an ECS Fargate task, runs your job's runner container, and tears the task definition down afterward. It has no opinion on your AWS account's VPC layout, IAM roles, or container registry — you resolve those and pass them in:

```python
from overture_serverless.backends.fargate import serverless_python_task_group

collect = serverless_python_task_group(
    group_id="collection_job",
    module_name="overture_addresses.collect",
    class_name="CollectionJob",
    python_packages="overture-addresses",
    task_role_arn=my_resolved_role_arn,  # e.g. via STS in your own DAG code
    network_config=my_ecs_network_config,  # ECS `networkConfiguration` dict
    image_uri=my_resolved_runner_image_uri,  # e.g. from your own ECR-URI builder
    ecs_task_builder_factory=MyEcsTaskBuilder,  # your register/run/teardown builder
)
```

See the `serverless_python_task_group` docstring for the full parameter list (sizing presets, CodeArtifact coordinates, retries, etc.).

## Launching any container on Fargate

If your image is pre-baked and doesn't need the pip-install runner contract (e.g. it's invoked by subcommand), use the generic lifecycle layer directly. It owns the same register → run → teardown scaffolding and CPU/memory sizing validation, but takes a plain image + command/environment:

```python
from overture_serverless.backends.fargate import fargate_task_group

diff = fargate_task_group(
    group_id="bundle_diff",
    family="bundle-diff",  # ECS task-definition family name
    image_uri=my_resolved_image_uri,
    command=["diff", "--bundle", "s3://..."],  # optional containerOverrides command
    environment={"LOG_LEVEL": "info"},  # optional env vars
    task_role_arn=my_resolved_role_arn,
    network_config=my_ecs_network_config,
    ecs_task_builder_factory=MyEcsTaskBuilder,
)
```

`serverless_python_task_group` is a thin wrapper over this construct.

## Publishing

See [`PACKAGE_VERSIONING.md`](../PACKAGE_VERSIONING.md) for how a version bump here turns into a PyPI release.
