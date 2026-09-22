"""Secret-fetching engines used by Spark jobs to read runtime credentials.

``SecretsInterface`` implementations import their backing SDK (``databricks.sdk``
or ``boto3``) lazily, inside ``__init__``/``get_secret`` rather than at module
load. This is intentional, not an oversight: it lets a caller depend on
``overture_spark`` without pulling in every secrets-backend SDK, since only the
engine actually instantiated ever imports its SDK. Keep new engines lazy the
same way.
"""

import base64
import json
from abc import ABC, abstractmethod

DATABRICKS_DEFAULT_SCOPE = "keyvaultsecret"


class SecretsInterface(ABC):
    @abstractmethod
    def get_secret(self, scope: str):
        raise NotImplementedError()


class Databricks(SecretsInterface):
    def __init__(self, scope: str = DATABRICKS_DEFAULT_SCOPE):
        SecretsInterface.__init__(self)
        self.scope = scope

    def get_secret(self, secret_name: str) -> str:
        from databricks.sdk.runtime import dbutils

        return dbutils.secrets.get(scope=self.scope, key=secret_name)


class DatabricksNoDbutils(SecretsInterface):
    def __init__(self, scope: str = DATABRICKS_DEFAULT_SCOPE):
        SecretsInterface.__init__(self)
        from databricks.sdk import WorkspaceClient

        self.client = WorkspaceClient()
        self.scope = scope

    def get_secret(self, secret_name: str) -> str:
        return base64.b64decode(
            self.client.secrets.get_secret(scope=self.scope, key=secret_name).value
        ).decode("utf-8")


class AwsSecretsManager(SecretsInterface):
    def __init__(self, scope: str = DATABRICKS_DEFAULT_SCOPE):
        SecretsInterface.__init__(self)
        import boto3

        self.client = boto3.client(
            service_name="secretsmanager", region_name="us-west-2"
        )
        self.scope = scope

    def get_secret(self, secret_name: str) -> str:
        res = self.client.get_secret_value(SecretId=self.scope)
        return json.loads(res["SecretString"])[secret_name]
