"""Backend contract implementations for ``serverless_python_task_group``.

A backend is an Airflow-facing function that provisions managed compute,
injects the runner-contract env vars (``MODULE_NAME``, ``CLASS_NAME``,
``PYTHON_PACKAGES``, ``PIP_INDEX_URL``, ``PARAMS``), and runs the container.
:mod:`overture_serverless.backends.fargate` is the AWS ECS Fargate
implementation shipped today. It also exposes the generic lifecycle layer it
is built on -- ``fargate_task_group`` -- for callers that bring their own
container image and command and don't need the pip-install runner contract.

Every cloud- or account-specific value (network configuration, IAM role,
container image URI, ECS task-definition composition) is a parameter the
caller supplies rather than something a backend resolves for itself, so this
package stays free of any one deployment's account IDs, VPC layout, or naming
conventions.

Import from the specific backend module (e.g. ``.fargate``) rather than a
top-level re-export: this keeps ``overture_serverless``'s base install free of
the ``apache-airflow`` / ``apache-airflow-providers-amazon`` dependencies that
only backend users need (see the ``fargate`` extra).
"""
