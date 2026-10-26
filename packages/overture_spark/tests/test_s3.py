import os
import pickle
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs

import pytest
from botocore.awsrequest import AWSResponse
from botocore.credentials import RefreshableCredentials

from overture_spark import s3
from overture_spark.s3 import (
    S3Access,
    boto3_session,
    pyarrow_s3_filesystem,
    resolve_s3_access,
    s3_client,
)

try:
    import pyspark  # noqa: F401
except ImportError:
    pyspark = None

needs_spark = pytest.mark.skipif(
    pyspark is None, reason="pyspark not installed; requires the sql-spark extra"
)

ROLE_ARN = "arn:aws:iam::123456789012:role/storage-integration"
EXTERNAL_ID_ENV = "WBC_STINT_ASSUME_ROLE_EXTERNAL_ID"


@pytest.fixture(autouse=True)
def isolated_aws(monkeypatch, tmp_path):
    """No ambient AWS config or credentials, and empty client caches."""
    for name in list(os.environ):
        if name.startswith("AWS_") or name == EXTERNAL_ID_ENV:
            monkeypatch.delenv(name)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    boto3_session.cache_clear()
    pyarrow_s3_filesystem.cache_clear()
    yield
    boto3_session.cache_clear()
    pyarrow_s3_filesystem.cache_clear()


@pytest.fixture
def source_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "source-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "source-secret")


def _spark(hadoop_conf: dict[str, str]):
    conf = SimpleNamespace(get=hadoop_conf.get)
    return SimpleNamespace(_jsc=SimpleNamespace(hadoopConfiguration=lambda: conf))


# S3Access


def test_s3_access_hides_secrets_from_repr():
    access = S3Access(region="us-west-2", access_key="key", secret_key="secret")
    assert repr(access) == "S3Access(region='us-west-2', role_arn=None)"


def test_s3_access_is_picklable_and_hashable():
    access = S3Access("us-west-2", ROLE_ARN)
    assert pickle.loads(pickle.dumps(access)) == access
    assert hash(S3Access("us-west-2", ROLE_ARN)) == hash(access)


@pytest.mark.parametrize("keys", [{"access_key": "key"}, {"secret_key": "secret"}])
def test_s3_access_requires_both_keys(keys):
    with pytest.raises(ValueError, match="together"):
        S3Access(**keys)


# resolve_s3_access


@pytest.mark.parametrize(
    "uri", ["/tmp/out", "file:///tmp/out", "gs://bucket/out", "s3:///out"]
)
def test_resolve_non_s3_uri(uri):
    """Non-S3 URIs never touch Spark."""
    assert resolve_s3_access(None, uri) == S3Access()


def test_resolve_without_s3a_config():
    """Glue: nothing configured, so clients keep the default chain."""
    assert resolve_s3_access(_spark({}), "s3://bucket/out") == S3Access()


@pytest.mark.parametrize("scheme", ["s3", "s3a"])
def test_resolve_bucket_role(scheme):
    spark = _spark(
        {
            "fs.s3a.bucket.bucket.assumed.role.arn": ROLE_ARN,
            "fs.s3a.bucket.bucket.endpoint.region": "eu-west-1",
        }
    )
    assert resolve_s3_access(spark, f"{scheme}://bucket/out") == S3Access(
        region="eu-west-1", role_arn=ROLE_ARN
    )


def test_resolve_bucket_overrides_global():
    spark = _spark(
        {
            "fs.s3a.assumed.role.arn": "arn:aws:iam::123456789012:role/global",
            "fs.s3a.endpoint.region": "us-east-1",
            "fs.s3a.bucket.bucket.assumed.role.arn": ROLE_ARN,
            "fs.s3a.bucket.bucket.endpoint.region": "eu-west-1",
        }
    )
    assert resolve_s3_access(spark, "s3a://bucket/out") == S3Access(
        region="eu-west-1", role_arn=ROLE_ARN
    )


def test_resolve_falls_back_to_global():
    spark = _spark(
        {
            "fs.s3a.assumed.role.arn": ROLE_ARN,
            "fs.s3a.endpoint.region": "us-east-1",
            "fs.s3a.bucket.other.assumed.role.arn": "arn:aws:iam::1:role/other",
            "fs.s3a.bucket.bucket.endpoint.region": "  ",
        }
    )
    assert resolve_s3_access(spark, "s3a://bucket/out") == S3Access(
        region="us-east-1", role_arn=ROLE_ARN
    )


@pytest.mark.parametrize(
    "conf",
    [
        {
            "fs.s3a.bucket.bucket.assumed.role.arn": ROLE_ARN,
            "fs.s3a.access.key": "key",
            "fs.s3a.secret.key": "secret",
        },
        {
            "fs.s3a.bucket.bucket.assumed.role.arn": ROLE_ARN,
            "fs.s3a.bucket.bucket.access.key": "key",
            "fs.s3a.bucket.bucket.secret.key": "secret",
        },
        {
            "fs.s3a.assumed.role.arn": ROLE_ARN,
            "fs.s3a.access.key": "key",
            "fs.s3a.secret.key": "secret",
        },
        {
            "fs.s3a.assumed.role.arn": ROLE_ARN,
            "fs.s3a.bucket.bucket.access.key": "lone-key",
        },
    ],
)
def test_resolve_role_wins_over_keys_at_the_same_or_wider_scope(conf):
    assert resolve_s3_access(_spark(conf), "s3a://bucket/out") == S3Access(
        role_arn=ROLE_ARN
    )


@pytest.mark.parametrize(
    "conf",
    [
        {
            "fs.s3a.assumed.role.arn": ROLE_ARN,
            "fs.s3a.bucket.bucket.access.key": "key",
            "fs.s3a.bucket.bucket.secret.key": "secret",
        },
        {
            "fs.s3a.assumed.role.arn": ROLE_ARN,
            "fs.s3a.bucket.bucket.access.key": "key",
            "fs.s3a.secret.key": "secret",
        },
    ],
)
def test_resolve_bucket_keys_win_over_global_role(conf):
    assert resolve_s3_access(_spark(conf), "s3a://bucket/out") == S3Access(
        access_key="key", secret_key="secret"
    )


@pytest.mark.parametrize(
    "conf",
    [
        {"fs.s3a.bucket.bucket.access.key": "key", "fs.s3a.secret.key": "secret"},
        {
            "fs.s3a.access.key": "global-key",
            "fs.s3a.secret.key": "global-secret",
            "fs.s3a.bucket.bucket.access.key": "key",
            "fs.s3a.bucket.bucket.secret.key": "secret",
        },
    ],
)
def test_resolve_static_keys(conf):
    """Databricks: each key resolves on its own, as in S3A."""
    access = resolve_s3_access(_spark(conf), "s3://bucket/out")
    assert access == S3Access(access_key="key", secret_key="secret")


def test_resolve_ignores_a_lone_key():
    spark = _spark(
        {
            "fs.s3a.access.key": "key",
            "fs.s3a.bucket.bucket.endpoint.region": "eu-west-1",
        }
    )
    assert resolve_s3_access(spark, "s3://bucket/out") == S3Access(region="eu-west-1")


# boto3_session


def _fake_sts(session, responses):
    """Answers the session's STS AssumeRole calls with ``responses`` in order,
    each an (access key, lifetime) pair. Returns the captured requests."""
    requests = []

    def send(request, **kwargs):
        access_key, lifetime = responses[len(requests)]
        requests.append(request)
        expiration = datetime.now(timezone.utc) + lifetime
        body = f"""
            <AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
              <AssumeRoleResult><Credentials>
                <AccessKeyId>{access_key}</AccessKeyId>
                <SecretAccessKey>{access_key}-secret</SecretAccessKey>
                <SessionToken>{access_key}-token</SessionToken>
                <Expiration>{expiration:%Y-%m-%dT%H:%M:%SZ}</Expiration>
              </Credentials></AssumeRoleResult>
            </AssumeRoleResponse>"""
        raw = SimpleNamespace(stream=lambda: [body.encode()])
        return AWSResponse(request.url, 200, {}, raw)

    session.events.register("before-send.sts.AssumeRole", send)
    return requests


def _params(request) -> dict[str, str]:
    body = request.body.decode() if isinstance(request.body, bytes) else request.body
    return {k: v for k, [v] in parse_qs(body).items()}


def test_boto3_session_default_chain(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
    session = boto3_session(S3Access(region="eu-west-1"))
    assert session.region_name == "eu-west-1"
    assert session.get_credentials().access_key == "env-key"


def test_boto3_session_static_keys(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
    session = boto3_session(S3Access(access_key="key", secret_key="secret"))
    credentials = session.get_credentials()
    assert (credentials.access_key, credentials.secret_key) == ("key", "secret")


def test_boto3_session_is_cached():
    session = boto3_session(S3Access(region="eu-west-1", role_arn=ROLE_ARN))
    assert boto3_session(S3Access(region="eu-west-1", role_arn=ROLE_ARN)) is session
    assert boto3_session(S3Access(region="eu-west-1")) is not session


@pytest.mark.usefixtures("source_credentials")
def test_boto3_session_assumes_role(monkeypatch):
    monkeypatch.setenv(EXTERNAL_ID_ENV, "external-id")
    # Older botocore defaults some regions to the global STS endpoint.
    monkeypatch.setenv("AWS_STS_REGIONAL_ENDPOINTS", "regional")
    session = boto3_session(S3Access(region="eu-west-1", role_arn=ROLE_ARN))
    requests = _fake_sts(session, [("assumed", timedelta(hours=1))])

    credentials = session.get_credentials().get_frozen_credentials()

    assert credentials.access_key == "assumed"
    assert credentials.secret_key == "assumed-secret"
    assert credentials.token == "assumed-token"
    [request] = requests
    assert request.url.startswith("https://sts.eu-west-1.amazonaws.com")
    authorization = request.headers["Authorization"]
    if isinstance(authorization, bytes):
        authorization = authorization.decode()
    assert "Credential=source-key/" in authorization
    params = _params(request)
    assert params["RoleArn"] == ROLE_ARN
    assert params["RoleSessionName"] == "overture-spark"
    assert params["ExternalId"] == "external-id"


@pytest.mark.usefixtures("source_credentials")
def test_boto3_session_assumes_role_without_external_id():
    session = boto3_session(S3Access(region="eu-west-1", role_arn=ROLE_ARN))
    requests = _fake_sts(session, [("assumed", timedelta(hours=1))])

    assert session.get_credentials().get_frozen_credentials().access_key == "assumed"
    assert "ExternalId" not in _params(requests[0])


@pytest.mark.usefixtures("source_credentials")
def test_boto3_session_refreshes_role():
    session = boto3_session(S3Access(region="eu-west-1", role_arn=ROLE_ARN))
    requests = _fake_sts(
        session, [("first", timedelta(minutes=5)), ("second", timedelta(hours=1))]
    )
    credentials = session.get_credentials()

    assert credentials.get_frozen_credentials().access_key == "first"
    assert credentials.get_frozen_credentials().access_key == "second"
    assert credentials.get_frozen_credentials().access_key == "second"
    assert len(requests) == 2


def test_boto3_session_role_needs_source_credentials():
    session = boto3_session(S3Access(region="eu-west-1", role_arn=ROLE_ARN))
    assert session.get_credentials() is None


# s3_client


def test_s3_client_uses_the_cached_session():
    access = S3Access(region="eu-west-1", access_key="key", secret_key="secret")
    client = s3_client(access, endpoint_url="http://localhost:9000")

    assert client.meta.service_model.service_name == "s3"
    assert client.meta.region_name == "eu-west-1"
    assert client.meta.endpoint_url == "http://localhost:9000"
    credentials = client._request_signer._credentials
    assert credentials is boto3_session(access).get_credentials()
    assert credentials.access_key == "key"


@pytest.mark.usefixtures("source_credentials")
def test_s3_client_gets_refreshable_assumed_role_without_calling_sts():
    access = S3Access(region="eu-west-1", role_arn=ROLE_ARN)
    requests = _fake_sts(boto3_session(access), [("assumed", timedelta(hours=1))])

    credentials = s3_client(access)._request_signer._credentials

    assert isinstance(credentials, RefreshableCredentials)
    assert credentials.method == "assume-role"
    assert credentials is boto3_session(access).get_credentials()
    assert requests == []


@pytest.mark.usefixtures("source_credentials")
def test_s3_client_and_role_refresh_create_clients_under_the_lock(monkeypatch):
    access = S3Access(region="eu-west-1", role_arn=ROLE_ARN)
    session = boto3_session(access)
    _fake_sts(session, [("assumed", timedelta(hours=1))])
    created = []
    create_client = session._session.create_client

    def spy(service_name, *args, **kwargs):
        created.append((service_name, s3._CLIENT_LOCK.locked()))
        return create_client(service_name, *args, **kwargs)

    monkeypatch.setattr(session._session, "create_client", spy)

    credentials = s3_client(access)._request_signer._credentials
    assert credentials.get_frozen_credentials().access_key == "assumed"
    assert created == [("s3", True), ("sts", True)]
    assert not s3._CLIENT_LOCK.locked()


@pytest.mark.usefixtures("source_credentials")
def test_s3_client_from_threads():
    access = S3Access(region="eu-west-1", role_arn=ROLE_ARN)
    with ThreadPoolExecutor(8) as pool:
        clients = list(pool.map(lambda _: s3_client(access), range(16)))
    assert len({id(client) for client in clients}) == 16
    assert {client.meta.region_name for client in clients} == {"eu-west-1"}


# pyarrow_s3_filesystem


@pytest.fixture
def pyarrow_kwargs(monkeypatch):
    """Keyword arguments of each S3FileSystem built; still built for real."""
    from pyarrow import fs

    calls = []
    real = fs.S3FileSystem

    def spy(**kwargs):
        calls.append(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(fs, "S3FileSystem", spy)
    return calls


def test_pyarrow_s3_filesystem_default_chain(pyarrow_kwargs):
    filesystem = pyarrow_s3_filesystem(S3Access(region="eu-west-1"))
    assert filesystem.region == "eu-west-1"
    assert pyarrow_kwargs == [
        {"access_key": None, "secret_key": None, "region": "eu-west-1"}
    ]


def test_pyarrow_s3_filesystem_static_keys(pyarrow_kwargs):
    access = S3Access(region="eu-west-1", access_key="key", secret_key="secret")
    pyarrow_s3_filesystem(access)
    assert pyarrow_kwargs == [
        {"access_key": "key", "secret_key": "secret", "region": "eu-west-1"}
    ]


def test_pyarrow_s3_filesystem_assumes_role(monkeypatch, pyarrow_kwargs):
    monkeypatch.setenv(EXTERNAL_ID_ENV, "external-id")
    filesystem = pyarrow_s3_filesystem(S3Access(region="eu-west-1", role_arn=ROLE_ARN))
    assert filesystem.region == "eu-west-1"
    assert pyarrow_kwargs == [
        {
            "role_arn": ROLE_ARN,
            "session_name": "overture-spark",
            "external_id": "external-id",
            "region": "eu-west-1",
        }
    ]


def test_pyarrow_s3_filesystem_assumes_role_without_external_id(pyarrow_kwargs):
    pyarrow_s3_filesystem(S3Access(region="eu-west-1", role_arn=ROLE_ARN))
    assert pyarrow_kwargs[0]["external_id"] is None


def test_pyarrow_s3_filesystem_is_cached():
    filesystem = pyarrow_s3_filesystem(S3Access(region="eu-west-1"))
    assert pyarrow_s3_filesystem(S3Access(region="eu-west-1")) is filesystem
    assert pyarrow_s3_filesystem(S3Access(region="us-east-1")) is not filesystem


def test_imports_sdks_lazily():
    assert not {"boto3", "botocore", "pyarrow"} & set(vars(s3))


# Spark


@pytest.mark.spark
@needs_spark
def test_resolve_in_spark_and_use_on_workers(monkeypatch):
    """``spark.hadoop.*`` conf reaches resolve_s3_access, and Python workers
    rebuild clients from the driver-resolved access."""
    from overture_spark import SparkPlatform, getSparkSedonaSession

    monkeypatch.setenv("PYSPARK_PYTHON", sys.executable)
    spark = getSparkSedonaSession(
        spark_platform=SparkPlatform.LOCAL,
        app_name="S3Access",
        extra_spark_conf={
            "spark.hadoop.fs.s3a.bucket.role.assumed.role.arn": ROLE_ARN,
            "spark.hadoop.fs.s3a.bucket.role.endpoint.region": "eu-west-1",
            "spark.hadoop.fs.s3a.bucket.keys.access.key": "key",
            "spark.hadoop.fs.s3a.bucket.keys.secret.key": "secret",
        },
    )
    try:
        access = resolve_s3_access(spark, "s3a://role/out")
        assert access == S3Access(region="eu-west-1", role_arn=ROLE_ARN)
        assert resolve_s3_access(spark, "s3://keys/out") == S3Access(
            access_key="key", secret_key="secret"
        )
        assert resolve_s3_access(spark, "s3://other/out") == S3Access()

        regions = (
            spark.sparkContext.parallelize([access], 1)
            .map(
                lambda a: (
                    boto3_session(a).region_name,
                    s3_client(a).meta.region_name,
                    pyarrow_s3_filesystem(a).region,
                )
            )
            .collect()
        )
        assert regions == [("eu-west-1", "eu-west-1", "eu-west-1")]
    finally:
        spark.stop()
