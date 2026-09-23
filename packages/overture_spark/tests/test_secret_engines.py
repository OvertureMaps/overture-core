from __future__ import annotations

import base64
import sys
import types
from unittest.mock import MagicMock

import pytest

from overture_spark.secret_engines import (
    AwsSecretsManager,
    Databricks,
    DatabricksNoDbutils,
    SecretsInterface,
)


def test_secrets_interface_is_abstract():
    with pytest.raises(TypeError):
        SecretsInterface()


def test_secrets_interface_get_secret_raises_not_implemented():
    class _Bare(SecretsInterface):
        def get_secret(self, scope: str):
            return SecretsInterface.get_secret(self, scope)

    with pytest.raises(NotImplementedError):
        _Bare().get_secret("scope")


def test_databricks_get_secret_delegates_to_dbutils(monkeypatch):
    fake_dbutils = MagicMock()
    fake_dbutils.secrets.get.return_value = "shh"
    fake_runtime = types.ModuleType("databricks.sdk.runtime")
    fake_runtime.dbutils = fake_dbutils
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", fake_runtime)

    engine = Databricks(scope="my-scope")
    assert engine.get_secret("token") == "shh"
    fake_dbutils.secrets.get.assert_called_once_with(scope="my-scope", key="token")


def test_databricks_uses_default_scope():
    assert Databricks().scope == "keyvaultsecret"


def test_databricks_no_dbutils_get_secret_decodes_base64(monkeypatch):
    encoded = base64.b64encode(b"shh").decode("utf-8")
    fake_secret = MagicMock(value=encoded)
    fake_client = MagicMock()
    fake_client.secrets.get_secret.return_value = fake_secret
    fake_workspace_client_cls = MagicMock(return_value=fake_client)
    fake_module = types.ModuleType("databricks.sdk")
    fake_module.WorkspaceClient = fake_workspace_client_cls
    monkeypatch.setitem(sys.modules, "databricks.sdk", fake_module)

    engine = DatabricksNoDbutils(scope="my-scope")
    assert engine.get_secret("token") == "shh"
    fake_client.secrets.get_secret.assert_called_once_with(
        scope="my-scope", key="token"
    )


def test_aws_secrets_manager_get_secret_parses_json(monkeypatch):
    fake_client = MagicMock()
    fake_client.get_secret_value.return_value = {"SecretString": '{"token": "shh"}'}
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = MagicMock(return_value=fake_client)
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    engine = AwsSecretsManager(scope="my-secret-arn")
    assert engine.get_secret("token") == "shh"
    fake_boto3.client.assert_called_once_with(
        service_name="secretsmanager", region_name="us-west-2"
    )
    fake_client.get_secret_value.assert_called_once_with(SecretId="my-secret-arn")
