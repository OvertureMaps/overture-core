"""Unit tests for AWS CodeArtifact helpers."""

from unittest.mock import MagicMock, patch

from overture_core.cloud.aws.codeartifact import get_codeartifact_token


class TestGetCodeartifactToken:
    def test_returns_authorization_token(self):
        codeartifact = MagicMock()
        codeartifact.get_authorization_token.return_value = {
            "authorizationToken": "token-value"
        }
        with patch(
            "overture_core.cloud.aws.codeartifact.boto3.client",
            return_value=codeartifact,
        ) as client:
            token = get_codeartifact_token("overture-pypi", "123456789012", "us-west-2")

        assert token == "token-value"
        client.assert_called_once_with("codeartifact", region_name="us-west-2")
        codeartifact.get_authorization_token.assert_called_once_with(
            domain="overture-pypi", domainOwner="123456789012"
        )
