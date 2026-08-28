from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

from overture_serverless.backends.fargate import (
    _CodeArtifactPipIndexUrlEcsOperator,
    _resolve_cpu_memory,
    fargate_task_group,
    serverless_python_task_group,
)


class _FakeEcsTaskBuilder:
    """Minimal ``EcsTaskBuilderLike`` stand-in that records how it was built
    and returns real (empty) operators so the DAG can be structurally
    inspected without touching AWS."""

    instances: list["_FakeEcsTaskBuilder"] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.register_task_id = None
        self.teardown_calls: list[dict] = []
        _FakeEcsTaskBuilder.instances.append(self)

    def register(self, task_id: str = "register_task_definition"):
        self.register_task_id = task_id
        return EmptyOperator(task_id=task_id)

    def run(
        self,
        overrides,
        task_id="run",
        awslogs_stream_prefix="",
        retries=0,
        reattach=True,
    ):
        raise AssertionError(
            "fargate.py builds the run operator directly (for the CodeArtifact "
            "PIP_INDEX_URL injection), it must not call ecs.run()"
        )

    def teardown(
        self, container_name, run_op=None, task_id="deregister_task_definition"
    ):
        self.teardown_calls.append(
            {"container_name": container_name, "run_op": run_op, "task_id": task_id}
        )
        return [EmptyOperator(task_id=task_id)]


@pytest.fixture(autouse=True)
def _reset_fake_builder_instances():
    _FakeEcsTaskBuilder.instances.clear()
    yield
    _FakeEcsTaskBuilder.instances.clear()


def _build_task_group(**overrides):
    kwargs = dict(
        group_id="collection_job",
        module_name="overture_addresses.collect",
        class_name="CollectionJob",
        python_packages="overture-addresses==1.0.0",
        task_role_arn="arn:aws:iam::123456789012:role/overture-python-runner-role",
        network_config={
            "awsvpcConfiguration": {
                "subnets": ["subnet-abc"],
                "securityGroups": ["sg-abc"],
                "assignPublicIp": "ENABLED",
            }
        },
        image_uri="123456789012.dkr.ecr.us-west-2.amazonaws.com/overture-python-runner:py311-stable-prod",
        ecs_task_builder_factory=_FakeEcsTaskBuilder,
    )
    kwargs.update(overrides)
    with DAG(dag_id="test_dag", start_date=datetime(2024, 1, 1), schedule=None):
        return serverless_python_task_group(**kwargs)


def _build_generic_task_group(**overrides):
    kwargs = dict(
        group_id="diff_job",
        family="bundle-diff",
        image_uri="123456789012.dkr.ecr.us-west-2.amazonaws.com/overture-agent-runner:stable",
        task_role_arn="arn:aws:iam::123456789012:role/overture-agent-runner-role",
        network_config={
            "awsvpcConfiguration": {
                "subnets": ["subnet-abc"],
                "securityGroups": ["sg-abc"],
                "assignPublicIp": "ENABLED",
            }
        },
        ecs_task_builder_factory=_FakeEcsTaskBuilder,
    )
    kwargs.update(overrides)
    with DAG(dag_id="test_dag_generic", start_date=datetime(2024, 1, 1), schedule=None):
        return fargate_task_group(**kwargs)


def _run_operator_of(tg):
    return next(t for t in tg if t.task_id.endswith("execute_job"))


def test_builds_expected_task_ids():
    tg = _build_task_group()
    task_ids = {t.split(".")[-1] for t in tg.children}
    assert task_ids == {"setup", "execute_job", "cleanup"}


def test_injects_network_config_and_role_arn_into_ecs_task_builder():
    network_config = {
        "awsvpcConfiguration": {
            "subnets": ["subnet-xyz"],
            "securityGroups": ["sg-xyz"],
            "assignPublicIp": "ENABLED",
        }
    }
    _build_task_group(
        task_role_arn="arn:aws:iam::999:role/custom-role",
        network_config=network_config,
    )
    (builder,) = _FakeEcsTaskBuilder.instances
    assert builder.kwargs["role_arn"] == "arn:aws:iam::999:role/custom-role"
    assert builder.kwargs["network_configuration"] == network_config


def test_no_import_of_private_helpers():
    """The whole point of this module: it must never reach into
    account-specific private helpers to resolve network config, IAM roles,
    or ECS composition -- callers inject all of it."""
    import overture_serverless.backends.fargate as fargate_module

    source = fargate_module.__file__
    with open(source, encoding="utf-8") as f:
        import_lines = [
            line.strip() for line in f if line.startswith(("import ", "from "))
        ]
    assert not any("aws_helper" in line for line in import_lines)
    assert not any("ecs_helper" in line for line in import_lines)
    assert not any(line.startswith("from src.") for line in import_lines)


def test_ecs_task_builder_uses_injected_image_uri_in_container_definition():
    _build_task_group(image_uri="example.com/runner:custom-tag")
    (builder,) = _FakeEcsTaskBuilder.instances
    [container] = builder.kwargs["container_definitions"]
    assert container["image"] == "example.com/runner:custom-tag"


def test_log_region_defaults_to_region():
    _build_task_group(region="eu-west-1")
    (builder,) = _FakeEcsTaskBuilder.instances
    [container] = builder.kwargs["container_definitions"]
    assert container["logConfiguration"]["options"]["awslogs-region"] == "eu-west-1"


def test_log_region_overridable_independently_of_region():
    _build_task_group(region="us-west-2", log_region="us-east-1")
    (builder,) = _FakeEcsTaskBuilder.instances
    [container] = builder.kwargs["container_definitions"]
    assert container["logConfiguration"]["options"]["awslogs-region"] == "us-east-1"


def test_output_path_forwarded_to_ecs_task_builder_factory():
    _build_task_group(output_path="s3://bucket/pipeline/metadata")
    (builder,) = _FakeEcsTaskBuilder.instances
    assert builder.kwargs["output_path"] == "s3://bucket/pipeline/metadata"


def test_ecs_cluster_and_log_group_default_to_overture_runner():
    _build_task_group()
    (builder,) = _FakeEcsTaskBuilder.instances
    [container] = builder.kwargs["container_definitions"]
    assert builder.kwargs["cluster"] == "overture-python-runner"
    assert builder.kwargs["awslogs_group"] == "/ecs/overture-python-runner"
    assert container["logConfiguration"]["options"]["awslogs-group"] == (
        "/ecs/overture-python-runner"
    )


def test_ecs_cluster_and_log_group_are_overridable():
    _build_task_group(ecs_cluster="other-cluster", log_group="/ecs/other-cluster")
    (builder,) = _FakeEcsTaskBuilder.instances
    [container] = builder.kwargs["container_definitions"]
    assert builder.kwargs["cluster"] == "other-cluster"
    assert builder.kwargs["awslogs_group"] == "/ecs/other-cluster"
    assert container["logConfiguration"]["options"]["awslogs-group"] == (
        "/ecs/other-cluster"
    )


def test_teardown_called_with_run_operator():
    _build_task_group()
    (builder,) = _FakeEcsTaskBuilder.instances
    assert len(builder.teardown_calls) == 1
    assert builder.teardown_calls[0]["container_name"] == "runner"
    assert builder.teardown_calls[0]["task_id"] == "cleanup"


def test_default_size_is_xs():
    _build_task_group()
    (builder,) = _FakeEcsTaskBuilder.instances
    assert builder.kwargs["cpu"] == "256"
    assert builder.kwargs["memory"] == "512"


def test_size_preset_selects_cpu_memory():
    _build_task_group(size="l")
    (builder,) = _FakeEcsTaskBuilder.instances
    assert builder.kwargs["cpu"] == "2048"
    assert builder.kwargs["memory"] == "4096"


def test_explicit_cpu_memory_override():
    _build_task_group(cpu="4096", memory="30720")
    (builder,) = _FakeEcsTaskBuilder.instances
    assert builder.kwargs["cpu"] == "4096"
    assert builder.kwargs["memory"] == "30720"


def test_invalid_ephemeral_storage_raises():
    with pytest.raises(ValueError, match="ephemeral_storage_gib"):
        _build_task_group(ephemeral_storage_gib=1)


def test_resolve_cpu_memory_rejects_size_and_cpu_together():
    with pytest.raises(ValueError, match="Set either"):
        _resolve_cpu_memory(size="s", cpu="512", memory=None)


def test_resolve_cpu_memory_rejects_unknown_size():
    with pytest.raises(ValueError, match="Unknown size preset"):
        _resolve_cpu_memory(size="xl", cpu=None, memory=None)


def test_resolve_cpu_memory_requires_both_cpu_and_memory():
    with pytest.raises(ValueError, match="Both `cpu` and `memory`"):
        _resolve_cpu_memory(size=None, cpu="512", memory=None)


def test_resolve_cpu_memory_rejects_invalid_combo():
    with pytest.raises(ValueError, match="not a valid Fargate combo"):
        _resolve_cpu_memory(size=None, cpu="256", memory="999999")


def test_resolve_cpu_memory_default_is_xs_preset():
    assert _resolve_cpu_memory(size=None, cpu=None, memory=None) == ("256", "512")


def test_family_name_includes_job_name_suffix():
    _build_task_group(job_name="nightly")
    (builder,) = _FakeEcsTaskBuilder.instances
    assert builder.kwargs["family"] == (
        "serverless-python-overture_addresses-collect-CollectionJob-nightly"
    )


def test_python_flavor_uses_codeartifact_operator_with_runner_env_vars():
    """The published wrapper must keep injecting the runner-contract env vars
    and the token-minting operator, now via the generic layer underneath."""
    tg = _build_task_group(parameters='{"a": 1}')
    run = _run_operator_of(tg)
    assert isinstance(run, _CodeArtifactPipIndexUrlEcsOperator)
    [container] = run.overrides["containerOverrides"]
    assert container["name"] == "runner"
    assert "command" not in container
    env_by_name = {e["name"]: e["value"] for e in container["environment"]}
    assert env_by_name == {
        "MODULE_NAME": "overture_addresses.collect",
        "CLASS_NAME": "CollectionJob",
        "PYTHON_PACKAGES": "overture-addresses==1.0.0",
        "PARAMS": '{"a": 1}',
    }
    assert run.awslogs_stream_prefix == "serverless-python/collection_job"


# ---------------------------------------------------------------------------
# Generic lifecycle layer (fargate_task_group)
# ---------------------------------------------------------------------------


def test_generic_builds_expected_task_ids():
    tg = _build_generic_task_group()
    task_ids = {t.split(".")[-1] for t in tg.children}
    assert task_ids == {"setup", "execute_job", "cleanup"}


def test_generic_default_run_operator_is_plain_ecs_run_task():
    tg = _build_generic_task_group()
    run = _run_operator_of(tg)
    assert type(run) is EcsRunTaskOperator


def test_generic_passes_command_and_environment_into_overrides():
    tg = _build_generic_task_group(
        command=["diff", "--bundle", "s3://bucket/x"],
        environment={"LOG_LEVEL": "info"},
    )
    run = _run_operator_of(tg)
    [container] = run.overrides["containerOverrides"]
    assert container["command"] == ["diff", "--bundle", "s3://bucket/x"]
    assert container["environment"] == [{"name": "LOG_LEVEL", "value": "info"}]


def test_generic_omits_command_and_environment_when_not_given():
    tg = _build_generic_task_group()
    run = _run_operator_of(tg)
    [container] = run.overrides["containerOverrides"]
    assert container == {"name": "runner"}


def test_generic_no_pip_or_module_dispatch_assumptions():
    tg = _build_generic_task_group(environment={"MY_VAR": "x"})
    run = _run_operator_of(tg)
    [container] = run.overrides["containerOverrides"]
    env_names = {e["name"] for e in container["environment"]}
    assert not env_names & {
        "MODULE_NAME",
        "CLASS_NAME",
        "PYTHON_PACKAGES",
        "PIP_INDEX_URL",
        "PARAMS",
    }


def test_generic_uses_injected_family_verbatim():
    _build_generic_task_group(family="my-custom-family")
    (builder,) = _FakeEcsTaskBuilder.instances
    assert builder.kwargs["family"] == "my-custom-family"


def test_generic_stream_prefix_defaults_to_group_id():
    tg = _build_generic_task_group()
    run = _run_operator_of(tg)
    (builder,) = _FakeEcsTaskBuilder.instances
    [container] = builder.kwargs["container_definitions"]
    assert run.awslogs_stream_prefix == "diff_job"
    assert (
        container["logConfiguration"]["options"]["awslogs-stream-prefix"] == "diff_job"
    )


def test_generic_stream_prefix_overridable():
    tg = _build_generic_task_group(awslogs_stream_prefix="custom/prefix")
    run = _run_operator_of(tg)
    assert run.awslogs_stream_prefix == "custom/prefix"


def test_generic_container_name_flows_everywhere():
    tg = _build_generic_task_group(container_name="agent")
    run = _run_operator_of(tg)
    (builder,) = _FakeEcsTaskBuilder.instances
    [definition] = builder.kwargs["container_definitions"]
    [override] = run.overrides["containerOverrides"]
    assert definition["name"] == "agent"
    assert override["name"] == "agent"
    assert builder.teardown_calls[0]["container_name"] == "agent"


def test_generic_sizing_validation_applies():
    with pytest.raises(ValueError, match="not a valid Fargate combo"):
        _build_generic_task_group(cpu="256", memory="999999")
    with pytest.raises(ValueError, match="ephemeral_storage_gib"):
        _build_generic_task_group(ephemeral_storage_gib=1)


def test_generic_run_operator_factory_receives_assembled_kwargs():
    captured: dict = {}

    def factory(**kwargs):
        captured.update(kwargs)
        return EcsRunTaskOperator(**kwargs)

    tg = _build_generic_task_group(run_operator_factory=factory, retries=3)
    run = _run_operator_of(tg)
    assert captured["task_id"] == "execute_job"
    assert captured["launch_type"] == "FARGATE"
    assert captured["retries"] == 3
    assert run.retries == 3


def _make_run_operator(**overrides) -> _CodeArtifactPipIndexUrlEcsOperator:
    kwargs = dict(
        task_id="execute_job",
        cluster="overture-python-runner",
        task_definition="fake-task-def",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": "runner",
                    "environment": [{"name": "MODULE_NAME", "value": "some.module"}],
                }
            ]
        },
        network_configuration={"awsvpcConfiguration": {}},
        codeartifact_domain="overture-pypi",
        codeartifact_owner="123456789012",
        codeartifact_repo="overture",
        codeartifact_region="us-west-2",
        target_container_name="runner",
    )
    kwargs.update(overrides)
    with DAG(dag_id="test_dag_op", start_date=datetime(2024, 1, 1), schedule=None):
        return _CodeArtifactPipIndexUrlEcsOperator(**kwargs)


@patch("boto3.client")
def test_execute_injects_masked_pip_index_url(mock_boto_client):
    mock_ca = MagicMock()
    mock_ca.get_authorization_token.return_value = {
        "authorizationToken": "super-secret-token"
    }
    mock_boto_client.return_value = mock_ca

    op = _make_run_operator()
    with patch(
        "airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator.execute",
        return_value="done",
    ) as mock_super_execute:
        result = op.execute(context={})

    assert result == "done"
    mock_super_execute.assert_called_once()
    [container] = op.overrides["containerOverrides"]
    env_by_name = {e["name"]: e["value"] for e in container["environment"]}
    assert env_by_name["PIP_INDEX_URL"].startswith("https://aws:super-secret-token@")
    assert "overture-pypi-123456789012" in env_by_name["PIP_INDEX_URL"]


@patch("boto3.client")
def test_execute_serializes_non_string_env_values(mock_boto_client):
    mock_ca = MagicMock()
    mock_ca.get_authorization_token.return_value = {"authorizationToken": "tok"}
    mock_boto_client.return_value = mock_ca

    op = _make_run_operator(
        overrides={
            "containerOverrides": [
                {
                    "name": "runner",
                    "environment": [{"name": "SOME_DICT", "value": {"a": 1}}],
                }
            ]
        }
    )
    with patch(
        "airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator.execute",
        return_value=None,
    ):
        op.execute(context={})

    [container] = op.overrides["containerOverrides"]
    some_dict_value = next(
        e["value"] for e in container["environment"] if e["name"] == "SOME_DICT"
    )
    assert json.loads(some_dict_value) == {"a": 1}


@patch("boto3.client")
def test_execute_ignores_containers_with_other_names(mock_boto_client):
    mock_ca = MagicMock()
    mock_ca.get_authorization_token.return_value = {"authorizationToken": "tok"}
    mock_boto_client.return_value = mock_ca

    op = _make_run_operator(
        overrides={
            "containerOverrides": [
                {"name": "sidecar", "environment": []},
                {"name": "runner", "environment": []},
            ]
        }
    )
    with patch(
        "airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator.execute",
        return_value=None,
    ):
        op.execute(context={})

    sidecar, runner = op.overrides["containerOverrides"]
    assert sidecar["environment"] == []
    assert any(e["name"] == "PIP_INDEX_URL" for e in runner["environment"])


@patch("boto3.client")
def test_execute_raises_when_target_container_missing(mock_boto_client):
    mock_ca = MagicMock()
    mock_ca.get_authorization_token.return_value = {"authorizationToken": "tok"}
    mock_boto_client.return_value = mock_ca

    op = _make_run_operator(
        overrides={"containerOverrides": [{"name": "sidecar", "environment": []}]}
    )
    with pytest.raises(ValueError, match="not found in containerOverrides"):
        op.execute(context={})


@patch("boto3.client")
def test_execute_replaces_stale_pip_index_url_on_retry(mock_boto_client):
    mock_ca = MagicMock()
    mock_ca.get_authorization_token.side_effect = [
        {"authorizationToken": "first-token"},
        {"authorizationToken": "second-token"},
    ]
    mock_boto_client.return_value = mock_ca

    op = _make_run_operator()
    with patch(
        "airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator.execute",
        return_value=None,
    ):
        op.execute(context={})  # first attempt
        op.execute(context={})  # simulated retry, same operator instance

    [container] = op.overrides["containerOverrides"]
    pip_index_urls = [
        e["value"] for e in container["environment"] if e["name"] == "PIP_INDEX_URL"
    ]
    assert len(pip_index_urls) == 1
    assert "second-token" in pip_index_urls[0]


def test_start_task_logs_job_console_url(caplog):
    op = _make_run_operator()
    op.cluster = "overture-python-runner"
    with patch(
        "airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator._start_task",
        return_value="started",
    ):
        op.arn = "arn:aws:ecs:us-west-2:123456789012:task/overture-python-runner/abc123"
        with caplog.at_level("INFO"):
            result = op._start_task()

    assert result == "started"
    assert any("Job console" in message for message in caplog.messages)


def test_start_task_skips_log_when_no_arn():
    op = _make_run_operator()
    with patch(
        "airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator._start_task",
        return_value="started",
    ):
        op.arn = None
        result = op._start_task()

    assert result == "started"
