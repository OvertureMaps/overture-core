"""AWS account, region, and role helpers built on boto3."""

import os
from functools import lru_cache

import boto3


@lru_cache(maxsize=1)
def get_account_id() -> str:
    """Return the AWS account ID for the caller's current credentials.

    Resolved via STS ``GetCallerIdentity`` and cached for the process
    lifetime — the account ID for a given credential set never changes
    mid-run, and repeated STS calls are wasted latency.
    """
    return boto3.client("sts").get_caller_identity()["Account"]


def get_region(default: str = "us-west-2") -> str:
    """Return the AWS region from ``AWS_REGION`` or ``AWS_DEFAULT_REGION``.

    Checks ``AWS_REGION`` first, then falls back to ``AWS_DEFAULT_REGION``
    (the variable boto3 itself checks when ``AWS_REGION`` isn't set), then
    *default*.

    Args:
        default: Region to fall back to when neither env var is set.
    """
    return (
        os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or default
    )


def build_role_arn(account_id: str, role_name: str) -> str:
    """Build an IAM role ARN from an account ID and role name."""
    return f"arn:aws:iam::{account_id}:role/{role_name}"


def assume_role(
    role_arn: str,
    session_name: str,
    *,
    region: str | None = None,
    duration_seconds: int = 3600,
) -> dict[str, str]:
    """Assume an IAM role and return temporary credentials as boto3 client kwargs.

    Args:
        role_arn: Full ARN of the role to assume.
        session_name: Identifier for the assumed-role session (appears in CloudTrail).
        region: Region for the returned client kwargs. Defaults to ``get_region()``.
        duration_seconds: Session duration in seconds. 3600 is the ceiling when
            the caller's own credentials were themselves obtained via
            AssumeRole ("role chaining"); a role assumed directly from a
            non-temporary identity may allow up to its configured
            ``MaxSessionDuration`` (up to 43200).

    Returns:
        A dict with ``aws_access_key_id``, ``aws_secret_access_key``,
        ``aws_session_token``, and ``region_name``, suitable for passing
        directly to ``boto3.client(..., **credentials)``.
    """
    resolved_region = region or get_region()
    sts = boto3.client("sts", region_name=resolved_region)
    response = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name,
        DurationSeconds=duration_seconds,
    )
    creds = response["Credentials"]
    return {
        "aws_access_key_id": creds["AccessKeyId"],
        "aws_secret_access_key": creds["SecretAccessKey"],
        "aws_session_token": creds["SessionToken"],
        "region_name": resolved_region,
    }
