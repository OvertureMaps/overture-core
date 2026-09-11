"""Unit tests for Secrets Manager helpers."""

from unittest.mock import MagicMock, patch

from overture_core.cloud.aws.secrets import get_secret_string


def test_returns_stripped_secret_string():
    sm = MagicMock()
    sm.get_secret_value.return_value = {"SecretString": "  sv=token\n"}
    with patch(
        "overture_core.cloud.aws.secrets.boto3.client", return_value=sm
    ) as client:
        assert get_secret_string("my/secret", region="us-east-1") == "sv=token"
    client.assert_called_once_with("secretsmanager", region_name="us-east-1")
    sm.get_secret_value.assert_called_once_with(SecretId="my/secret")
