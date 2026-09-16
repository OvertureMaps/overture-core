"""AWS Secrets Manager helpers built on boto3."""

import boto3


def get_secret_string(secret_name: str, region: str | None = None) -> str:
    """Return the ``SecretString`` for *secret_name*, whitespace-stripped.

    Stripping matters for tokens pasted into the console with a trailing
    newline, which otherwise silently breaks header/URL auth downstream.

    Args:
        secret_name: Secret name or full ARN.
        region: Region of the secret. Defaults to boto3's own resolution.
    """
    sm = boto3.client("secretsmanager", region_name=region)
    return sm.get_secret_value(SecretId=secret_name)["SecretString"].strip()
