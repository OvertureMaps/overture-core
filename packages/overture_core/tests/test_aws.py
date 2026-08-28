"""Unit tests for AWS account/region/role helpers."""

from unittest.mock import MagicMock, patch

from overture_core.aws import assume_role, build_role_arn, get_account_id, get_region


class TestGetAccountId:
    def test_returns_account_id_from_sts(self):
        get_account_id.cache_clear()
        sts = MagicMock()
        sts.get_caller_identity.return_value = {"Account": "123456789012"}
        with patch("overture_core.aws.boto3.client", return_value=sts):
            assert get_account_id() == "123456789012"

    def test_caches_across_calls(self):
        get_account_id.cache_clear()
        sts = MagicMock()
        sts.get_caller_identity.return_value = {"Account": "123456789012"}
        with patch("overture_core.aws.boto3.client", return_value=sts) as client:
            get_account_id()
            get_account_id()
        assert client.call_count == 1


class TestGetRegion:
    def test_reads_aws_region_env_var(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        assert get_region() == "eu-west-1"

    def test_falls_back_to_default(self, monkeypatch):
        monkeypatch.delenv("AWS_REGION", raising=False)
        assert get_region() == "us-west-2"

    def test_custom_default(self, monkeypatch):
        monkeypatch.delenv("AWS_REGION", raising=False)
        assert get_region(default="ap-south-1") == "ap-south-1"


class TestBuildRoleArn:
    def test_builds_arn(self):
        assert (
            build_role_arn("123456789012", "my-role")
            == "arn:aws:iam::123456789012:role/my-role"
        )


class TestAssumeRole:
    def test_returns_client_kwargs(self, monkeypatch):
        monkeypatch.delenv("AWS_REGION", raising=False)
        sts = MagicMock()
        sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIDEXAMPLE",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            }
        }
        with patch("overture_core.aws.boto3.client", return_value=sts):
            creds = assume_role("arn:aws:iam::123456789012:role/my-role", "session")

        assert creds == {
            "aws_access_key_id": "AKIDEXAMPLE",
            "aws_secret_access_key": "secret",
            "aws_session_token": "token",
            "region_name": "us-west-2",
        }
        sts.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/my-role",
            RoleSessionName="session",
            DurationSeconds=3600,
        )

    def test_uses_explicit_region_over_default(self):
        sts = MagicMock()
        sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIDEXAMPLE",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            }
        }
        with patch("overture_core.aws.boto3.client", return_value=sts):
            creds = assume_role(
                "arn:aws:iam::123456789012:role/my-role",
                "session",
                region="eu-central-1",
            )
        assert creds["region_name"] == "eu-central-1"
