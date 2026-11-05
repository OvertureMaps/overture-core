import boto3
import pytest
from moto import mock_aws


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("overture-core-sql-test")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def moto_s3(monkeypatch):
    """A boto3 S3 client on moto's in-process S3, with no buckets."""
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(name, "testing")
    with mock_aws():
        # A session client is unaffected by tests that patch ``boto3.client``.
        yield boto3.session.Session().client("s3", region_name="us-east-1")
