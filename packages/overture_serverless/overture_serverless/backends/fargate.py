"""AWS ECS Fargate backend for the ``serverless_python`` framework.

One implementation of the ``serverless_python_task_group`` contract. It
launches the reference runner container (``overture-python-runner``) on
Fargate; at run time the container installs the requested Python packages
from a pip index and dispatches to the caller's job class.

The runner container is cloud-agnostic -- it speaks a simple runner contract
(``MODULE_NAME``, ``CLASS_NAME``, ``PYTHON_PACKAGES``, ``PIP_INDEX_URL``,
``PARAMS`` env vars) and takes a single ``PIP_INDEX_URL``. This backend
assembles that URL from CodeArtifact credentials before passing it in; a
hypothetical GCP Cloud Run or Azure Container Apps backend would live
alongside this file and assemble the URL from its own artifact registry.

Every value specific to a deployment's AWS account -- VPC/subnet layout, IAM
role ARNs, container image URI, and how the underlying ECS task definition is
registered/run/torn down -- is supplied by the caller rather than resolved
here, so this module has no dependency on any one deployment's private
helpers. See ``network_config``, ``task_role_arn``, ``image_uri``, and
``ecs_task_builder_factory`` on :func:`serverless_python_task_group`.

Task pipeline::

    register_task_definition -> execute_job -> deregister_task_definition

- ``register_task_definition`` / ``deregister_task_definition`` come from the
  injected ``ecs_task_builder_factory`` and manage the ephemeral Fargate task
  definition.
- ``execute_job`` fetches a short-lived CodeArtifact auth token inside the
  operator's ``execute()`` and injects a fully-formed ``PIP_INDEX_URL`` into
  ``containerOverrides`` in memory before calling ECS ``RunTask``. The token
  never lands in XCom, task logs, or the metadata DB.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Literal, Protocol

from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

from overture_serverless.airflow_compat import TaskGroup, chain, mask_secret

_ECS_CLUSTER = "overture-python-runner"
_LOG_GROUP = "/ecs/overture-python-runner"

log = logging.getLogger(__name__)

_CONTAINER_NAME = "runner"
_CODEARTIFACT_TOKEN_DURATION_SECONDS = (
    3600  # 1 hour, plenty for Fargate cold start + pip install
)

# T-shirt-sized presets. All combos are valid Fargate CPU/memory pairings.
_SIZE_PRESETS: dict[str, tuple[str, str]] = {
    "xs": ("256", "512"),
    "s": ("512", "1024"),
    "m": ("1024", "2048"),
    "l": ("2048", "4096"),
}


class EcsTaskBuilderLike(Protocol):
    """Duck-typed contract for the ECS register -> run -> teardown builder.

    Matches ``EcsTaskBuilder`` from Overture's internal ``ecs_helper`` module
    (and any equivalent a caller wants to substitute) without this package
    importing that class directly.
    """

    def register(self, task_id: str = ...) -> Any: ...

    def run(
        self,
        overrides: dict,
        task_id: str = ...,
        awslogs_stream_prefix: str = ...,
        retries: int = ...,
        reattach: bool = ...,
    ) -> Any: ...

    def teardown(
        self,
        container_name: str,
        run_op: Any | None = ...,
        task_id: str = ...,
    ) -> list: ...


def _build_fargate_combos() -> set[tuple[str, str]]:
    """Enumerate every valid Fargate (cpu_units, memory_mib) combination.

    Sourced from
    https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html - no enum in botocore model
    Used to fail fast at DAG-parse time when a caller passes an invalid
    exact override, instead of surfacing an ECS ``InvalidParameterException``
    at task-run time.
    """
    combos: set[tuple[str, str]] = set()
    combos.update((("256", m) for m in ("512", "1024", "2048")))
    combos.update((("512", m) for m in ("1024", "2048", "3072", "4096")))
    combos.update((("1024", str(m)) for m in range(2048, 8193, 1024)))
    combos.update((("2048", str(m)) for m in range(4096, 16385, 1024)))
    combos.update((("4096", str(m)) for m in range(8192, 30721, 1024)))
    combos.update((("8192", str(m)) for m in range(16384, 61441, 4096)))
    combos.update((("16384", str(m)) for m in range(32768, 122881, 8192)))
    return combos


_FARGATE_VALID_COMBOS = _build_fargate_combos()


def _resolve_cpu_memory(
    size: str | None, cpu: str | None, memory: str | None
) -> tuple[str, str]:
    if size is not None and (cpu is not None or memory is not None):
        raise ValueError("Set either `size` or (`cpu` and `memory`), not both.")
    if cpu is None and memory is None:
        preset = size or "xs"
        if preset not in _SIZE_PRESETS:
            raise ValueError(
                f"Unknown size preset: {preset!r}. Valid: {sorted(_SIZE_PRESETS)}. "
                "If you think this is a valid preset, check "
                "https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html "
                "and update _build_fargate_combos() accordingly."
            )
        return _SIZE_PRESETS[preset]
    if cpu is None or memory is None:
        raise ValueError(
            "Both `cpu` and `memory` must be set when using an exact override."
        )
    if (cpu, memory) not in _FARGATE_VALID_COMBOS:
        raise ValueError(
            f"cpu={cpu!r}, memory={memory!r} is not a valid Fargate combo. "
            "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html "
            "and update _build_fargate_combos() accordingly."
        )
    return cpu, memory


class _CodeArtifactPipIndexUrlEcsOperator(EcsRunTaskOperator):
    """``EcsRunTaskOperator`` that mints a CodeArtifact-backed
    ``PIP_INDEX_URL`` inside ``execute()`` and injects it into
    ``containerOverrides`` in memory just before submitting to ECS.

    The token is registered with Airflow's secrets masker so it is redacted
    from task logs. It never lands in XCom, the Airflow metadata DB, or any
    templated field.
    """

    def __init__(
        self,
        *,
        codeartifact_domain: str,
        codeartifact_owner: str,
        codeartifact_repo: str,
        codeartifact_region: str,
        target_container_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._ca_domain = codeartifact_domain
        self._ca_owner = codeartifact_owner
        self._ca_repo = codeartifact_repo
        self._ca_region = codeartifact_region
        self._target_container_name = target_container_name

    def execute(self, context):
        import boto3

        ca = boto3.client("codeartifact", region_name=self._ca_region)
        token = ca.get_authorization_token(
            domain=self._ca_domain,
            domainOwner=self._ca_owner,
            durationSeconds=_CODEARTIFACT_TOKEN_DURATION_SECONDS,
        )["authorizationToken"]
        mask_secret(token)
        pip_index_url = (
            f"https://aws:{token}"
            f"@{self._ca_domain}-{self._ca_owner}"
            f".d.codeartifact.{self._ca_region}.amazonaws.com"
            f"/pypi/{self._ca_repo}/simple/"
        )
        mask_secret(pip_index_url)
        for container in self.overrides.get("containerOverrides", []):
            if container.get("name") != self._target_container_name:
                continue
            # NativeEnvironment can literal_eval a JSON string back to a dict.
            for env in container.get("environment", []):
                if not isinstance(env.get("value"), str):
                    env["value"] = json.dumps(env["value"])
            container.setdefault("environment", []).append(
                {"name": "PIP_INDEX_URL", "value": pip_index_url}
            )
        return super().execute(context)

    def _start_task(self):
        # Superclass populates self.arn after the RunTask call. Emit a
        # clickable console URL as soon as the task ID is known so
        # engineers can jump straight from Airflow logs to the running
        # job. Label stays platform-neutral ("Job console") so future
        # backends can supply their own URL template without a rename.
        result = super()._start_task()
        if self.arn:
            # arn:aws:ecs:<region>:<account>:task/<cluster>/<task-id>
            region = self.arn.split(":")[3]
            task_id = self.arn.rsplit("/", 1)[-1]
            self.log.info(
                "Job console: https://%s.console.aws.amazon.com/ecs/v2/clusters/%s/tasks/%s?region=%s",
                region,
                self.cluster,
                task_id,
                region,
            )
        return result


def serverless_python_task_group(
    group_id: str,
    *,
    module_name: str,
    class_name: str,
    python_packages: str,
    task_role_arn: str,
    network_config: dict,
    image_uri: str,
    ecs_task_builder_factory: "type[EcsTaskBuilderLike] | Any",
    parameters: str = "{}",
    size: Literal["xs", "s", "m", "l"] | None = None,
    cpu: str | None = None,
    memory: str | None = None,
    ephemeral_storage_gib: int = 21,
    retries: int = 0,
    job_name: str = "",
    region: str = "us-west-2",
    log_region: str | None = None,
    codeartifact_domain: str = "overture-pypi",
    codeartifact_owner: str = "505071440022",
    codeartifact_repo: str = "overture",
    output_path: str | None = None,
):
    """Run a Python job on the AWS ECS Fargate serverless backend.

    Callers implement a class exposing ``run(params: str) -> None`` --
    typically a ``ServerlessPythonJob`` subclass, but any object with that
    shape works. The class is cloud-agnostic; only this factory ties the
    invocation to Fargate.

    Args:
        group_id: Airflow task group ID shown in the UI.
        module_name: Dotted Python module containing the job class,
            e.g. ``"overture_addresses.collect"``.
        class_name: Name of the class to instantiate, e.g. ``"CollectionJob"``.
        python_packages: Space-separated pip specs installed at run time.
        task_role_arn: Full ARN of the IAM role used as both ``taskRoleArn``
            and ``executionRoleArn``. Required -- resolve it (e.g. from an
            IAM role name via STS) before calling in; this module has no
            opinion on how role names map to ARNs in your account.
        network_config: ECS ``networkConfiguration`` dict (subnets, security
            groups, ``assignPublicIp``) as accepted by the ECS ``RunTask``
            API. Required -- resolve it from wherever your deployment stores
            VPC/subnet/security-group configuration before calling in.
        image_uri: Fully resolved runner container image URI (registry,
            repository, and tag). Required -- build it however your
            deployment maps environments/versions to image tags (e.g. via an
            ECR-URI builder keyed by account and region) before calling in.
        ecs_task_builder_factory: Callable with the same call signature as
            ``EcsTaskBuilder.__init__`` (``family``, ``container_definitions``,
            ``role_arn``, ``network_configuration``, ``cpu``, ``memory``,
            ``ephemeral_storage_gib``, ``cluster``, ``awslogs_group``,
            ``awslogs_region``, ``launch_type``, ``network_mode``,
            ``output_path``) that returns an object satisfying
            :class:`EcsTaskBuilderLike` (``register()``/``run()``/``teardown()``).
            Required -- pass your deployment's ECS task-definition builder
            (e.g. Overture's internal ``EcsTaskBuilder``); this module never
            constructs ECS task definitions on its own.
        parameters: JSON string forwarded to ``cls().run(params)``.
        size: T-shirt preset. ``xs`` (default) covers streaming I/O, API
            calls, and lightweight transforms. Presets:

            +------+-----------+------------+-----------+---------+
            | size | CPU units | Memory MiB | real vCPU | RAM     |
            +======+===========+============+===========+=========+
            | xs   | 256       | 512        | 0.25      | 0.5 GB  |
            | s    | 512       | 1024       | 0.5       | 1 GB    |
            | m    | 1024      | 2048       | 1         | 2 GB    |
            | l    | 2048      | 4096       | 2         | 4 GB    |
            +------+-----------+------------+-----------+---------+

            If ``l`` is not enough, pass ``cpu``/``memory`` explicitly.
        cpu, memory: Escape hatch for exact Fargate CPU units / memory MiB.
            Must form a valid Fargate combo; the wrapper validates at parse
            time. Mutually exclusive with ``size``.
        ephemeral_storage_gib: Fargate ephemeral storage in GiB (minimum 21
            when set explicitly, max 200).
        retries: Airflow-level retries on the ECS run task.
        job_name: Optional display suffix appended to the task definition
            family name in the ECS console.
        region: AWS region for Fargate, ECS, and CodeArtifact.
        log_region: AWS region for CloudWatch Logs. Defaults to ``region`` --
            override when logs live in a different region than compute.
        codeartifact_domain, codeartifact_owner, codeartifact_repo:
            CodeArtifact coordinates. This backend fetches an auth token
            and assembles a ``PIP_INDEX_URL`` before invoking the runner;
            the container itself is not CodeArtifact-aware.
        output_path: Forwarded to ``ecs_task_builder_factory`` so an
            injected builder can record container image provenance; this
            module does not interpret it itself.
    """
    resolved_cpu, resolved_memory = _resolve_cpu_memory(size, cpu, memory)

    if not 21 <= ephemeral_storage_gib <= 200:
        raise ValueError(
            f"ephemeral_storage_gib={ephemeral_storage_gib} is invalid; Fargate requires 21..200 GiB"
        )
    resolved_log_region = log_region or region

    family_suffix = "-".join(
        part.replace(".", "-") for part in (module_name, class_name, job_name) if part
    )
    family = f"serverless-python-{family_suffix}"

    with TaskGroup(group_id=group_id) as tg:
        ecs = ecs_task_builder_factory(
            family=family,
            cluster=_ECS_CLUSTER,
            awslogs_group=_LOG_GROUP,
            container_definitions=[
                {
                    "name": _CONTAINER_NAME,
                    "image": image_uri,
                    "essential": True,
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": _LOG_GROUP,
                            "awslogs-region": resolved_log_region,
                            "awslogs-stream-prefix": f"serverless-python/{group_id}",
                        },
                    },
                }
            ],
            role_arn=task_role_arn,
            network_configuration=network_config,
            cpu=resolved_cpu,
            memory=resolved_memory,
            ephemeral_storage_gib=ephemeral_storage_gib,
            output_path=output_path,
        )

        # Task IDs use platform-agnostic names (setup / execute_job / cleanup)
        # rather than the ECS-specific "register_task_definition" /
        # "deregister_task_definition" defaults, so future backends
        # (k8s Jobs, Cloud Run, Azure Container Apps) can slot into the
        # same UI shape without leaking cloud vocabulary.
        register = ecs.register(task_id="setup")
        # Build the run operator directly rather than via ``ecs.run()`` so we
        # can use the custom subclass that mints and injects PIP_INDEX_URL at
        # execute time. Mirrors ``EcsTaskBuilder.run()`` internals; register()
        # must have been called to expose the task-definition XCom.
        run = _CodeArtifactPipIndexUrlEcsOperator(
            task_id="execute_job",
            cluster=_ECS_CLUSTER,
            task_definition=str(register.output),
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": _CONTAINER_NAME,
                        "environment": [
                            {"name": "MODULE_NAME", "value": module_name},
                            {"name": "CLASS_NAME", "value": class_name},
                            {"name": "PYTHON_PACKAGES", "value": python_packages},
                            {"name": "PARAMS", "value": parameters},
                        ],
                    }
                ]
            },
            network_configuration=network_config,
            awslogs_group=_LOG_GROUP,
            awslogs_region=resolved_log_region,
            awslogs_stream_prefix=f"serverless-python/{group_id}",
            do_xcom_push=True,
            retries=retries,
            codeartifact_domain=codeartifact_domain,
            codeartifact_owner=codeartifact_owner,
            codeartifact_repo=codeartifact_repo,
            codeartifact_region=region,
            target_container_name=_CONTAINER_NAME,
        )

        chain(
            register,
            run,
            ecs.teardown(_CONTAINER_NAME, run_op=run, task_id="cleanup"),
        )

    return tg


__all__ = ["serverless_python_task_group", "EcsTaskBuilderLike"]
