"""Unit tests for the Iceberg catalog identity types and ``render_catalog``.

The ``get_*_table()`` helpers stay in tf-data-platform (they depend on the
dev-environment ``get_namespace()`` lookup), so only the catalog identity
types and the platform-agnostic ``render_catalog`` renderer moved here are
exercised: the enums, ``CatalogSpec``, ``CatalogBinding`` validation in its
``__post_init__``, and the full platform x kind rendering matrix.
"""

from unittest import mock

import pytest

import overture_core.iceberg as iceberg
from overture_core.iceberg import (
    ICEBERG_CATALOG,
    ICEBERG_CATALOG_SPEC,
    ICEBERG_SPARK_EXTENSIONS,
    S3TABLES_CATALOG_ALIAS,
    S3TABLES_CATALOG_SPEC,
    CatalogBinding,
    CatalogKind,
    CatalogSpec,
    Platform,
    render_catalog,
)


class TestEnums:
    def test_platform_members(self):
        assert {p.name for p in Platform} == {"GLUE", "WHEROBOTS"}

    def test_catalog_kind_members(self):
        assert {k.name for k in CatalogKind} == {"GLUE_DATA_CATALOG", "S3_TABLES"}


class TestCatalogSpec:
    def test_iceberg_catalog_spec(self):
        assert ICEBERG_CATALOG_SPEC.alias == ICEBERG_CATALOG
        assert ICEBERG_CATALOG_SPEC.kind is CatalogKind.GLUE_DATA_CATALOG
        assert ICEBERG_CATALOG_SPEC.is_default is True

    def test_s3tables_catalog_spec(self):
        assert S3TABLES_CATALOG_SPEC.alias == S3TABLES_CATALOG_ALIAS
        assert S3TABLES_CATALOG_SPEC.kind is CatalogKind.S3_TABLES
        assert S3TABLES_CATALOG_SPEC.is_default is False

    def test_is_frozen(self):
        with pytest.raises(AttributeError):
            ICEBERG_CATALOG_SPEC.alias = "other"


class TestCatalogBinding:
    def test_glue_catalog_binding_needs_no_bucket(self):
        binding = CatalogBinding(spec=ICEBERG_CATALOG_SPEC)
        assert binding.bucket is None
        assert binding.bucket_var is None

    def test_s3tables_binding_with_bucket(self):
        binding = CatalogBinding(spec=S3TABLES_CATALOG_SPEC, bucket="my-bucket")
        assert binding.bucket == "my-bucket"

    def test_s3tables_binding_with_bucket_var(self):
        binding = CatalogBinding(
            spec=S3TABLES_CATALOG_SPEC, bucket_var="managed_bucket_iceberg"
        )
        assert binding.bucket_var == "managed_bucket_iceberg"

    def test_s3tables_binding_requires_bucket_or_bucket_var(self):
        with pytest.raises(ValueError, match="requires a"):
            CatalogBinding(spec=S3TABLES_CATALOG_SPEC)

    def test_bucket_and_bucket_var_mutually_exclusive(self):
        with pytest.raises(ValueError, match="not both"):
            CatalogBinding(
                spec=S3TABLES_CATALOG_SPEC, bucket="a-bucket", bucket_var="a-var"
            )

    def test_is_frozen(self):
        binding = CatalogBinding(spec=ICEBERG_CATALOG_SPEC)
        with pytest.raises(AttributeError):
            binding.bucket = "other"


class TestCatalogSpecEquality:
    def test_specs_with_same_fields_are_equal(self):
        assert CatalogSpec("iceberg_catalog", CatalogKind.GLUE_DATA_CATALOG, True) == (
            ICEBERG_CATALOG_SPEC
        )


_REGION = "us-west-2"
_ACCOUNT = "123456789012"

_GLUE_SPEC = CatalogSpec(
    "iceberg_catalog", CatalogKind.GLUE_DATA_CATALOG, is_default=True
)
_S3TABLES_SPEC = CatalogSpec("s3tables_catalog", CatalogKind.S3_TABLES)


@pytest.fixture
def _aws_ctx():
    with (
        mock.patch.object(iceberg, "get_aws_region", return_value=_REGION),
        mock.patch.object(iceberg, "get_current_aws_account_id", return_value=_ACCOUNT),
    ):
        yield


@pytest.fixture
def s3tables_config(_aws_ctx):
    return render_catalog(
        S3TABLES_CATALOG_SPEC,
        Platform.GLUE,
        bucket="{{ var.value.managed_bucket_iceberg }}",
    )


def _key(suffix: str) -> str:
    return f"spark.sql.catalog.{S3TABLES_CATALOG_ALIAS}.{suffix}"


class TestRenderCatalogS3TablesRest:
    def test_rest_catalog_wiring(self, s3tables_config):
        assert (
            s3tables_config[f"spark.sql.catalog.{S3TABLES_CATALOG_ALIAS}"]
            == "org.apache.iceberg.spark.SparkCatalog"
        )
        assert (
            s3tables_config[_key("catalog-impl")]
            == "org.apache.iceberg.rest.RESTCatalog"
        )
        assert (
            s3tables_config[_key("uri")]
            == f"https://s3tables.{_REGION}.amazonaws.com/iceberg"
        )
        assert (
            s3tables_config[_key("warehouse")]
            == f"arn:aws:s3tables:{_REGION}:{_ACCOUNT}:bucket/"
            "{{ var.value.managed_bucket_iceberg }}"
        )

    def test_sigv4_signing_for_s3tables(self, s3tables_config):
        assert s3tables_config[_key("rest.sigv4-enabled")] == "true"
        assert s3tables_config[_key("rest.signing-name")] == "s3tables"
        assert s3tables_config[_key("rest.signing-region")] == _REGION

    def test_includes_iceberg_sql_extensions(self, s3tables_config):
        # Required so the bucket(...) partition transform DDL parses; also
        # keeps Sedona's spatial SQL extensions registered.
        assert s3tables_config["spark.sql.extensions"] == ICEBERG_SPARK_EXTENSIONS
        assert (
            "IcebergSparkSessionExtensions" in s3tables_config["spark.sql.extensions"]
        )

    def test_uses_s3_file_io(self, s3tables_config):
        assert s3tables_config[_key("io-impl")] == "org.apache.iceberg.aws.s3.S3FileIO"

    def test_rest_metrics_reporting_disabled(self, s3tables_config):
        # The S3 Tables REST endpoint has no metrics endpoint; must be opt-out.
        assert s3tables_config[_key("rest-metrics-reporting-enabled")] == "false"

    def test_http_connection_pool_sized(self, s3tables_config):
        assert s3tables_config[_key("http-client.apache.max-connections")] == 3000


class TestRenderCatalogMatrix:
    """Full platform x kind matrix for the single declarative renderer."""

    def test_render_glue_data_catalog_on_glue(self, _aws_ctx):
        cfg = render_catalog(_GLUE_SPEC, Platform.GLUE)
        assert (
            cfg["spark.sql.catalog.iceberg_catalog"]
            == "org.apache.iceberg.spark.SparkCatalog"
        )
        assert (
            cfg["spark.sql.catalog.iceberg_catalog.catalog-impl"]
            == "org.apache.iceberg.rest.RESTCatalog"
        )
        assert cfg["spark.sql.catalog.iceberg_catalog.rest.signing-name"] == "glue"
        assert cfg["spark.sql.catalog.iceberg_catalog.warehouse"] == _ACCOUNT
        assert cfg["spark.sql.defaultCatalog"] == "iceberg_catalog"

    def test_render_s3tables_on_glue(self, _aws_ctx):
        cfg = render_catalog(
            _S3TABLES_SPEC, Platform.GLUE, bucket="overture-managed-iceberg-dev"
        )
        assert (
            cfg["spark.sql.catalog.s3tables_catalog.catalog-impl"]
            == "org.apache.iceberg.rest.RESTCatalog"
        )
        assert cfg["spark.sql.catalog.s3tables_catalog.rest.signing-name"] == "s3tables"
        assert cfg["spark.sql.catalog.s3tables_catalog.warehouse"].startswith(
            "arn:aws:s3tables:"
        )
        assert cfg["spark.sql.catalog.s3tables_catalog.warehouse"].endswith(
            "bucket/overture-managed-iceberg-dev"
        )
        # Secondary catalog must NOT set defaultCatalog or it clobbers the
        # primary on merge.
        assert "spark.sql.defaultCatalog" not in cfg

    def test_render_glue_data_catalog_on_wherobots(self, _aws_ctx):
        cfg = render_catalog(
            _GLUE_SPEC,
            Platform.WHEROBOTS,
            warehouse_path="s3://overture-managed-violations-dev/",
        )
        assert (
            cfg["spark.sql.catalog.iceberg_catalog"]
            == "org.apache.iceberg.spark.SparkCatalog"
        )
        assert (
            cfg["spark.sql.catalog.iceberg_catalog.catalog-impl"]
            == "org.apache.iceberg.aws.glue.GlueCatalog"
        )
        assert (
            cfg["spark.sql.catalog.iceberg_catalog.warehouse"]
            == "s3://overture-managed-violations-dev/"
        )
        assert cfg["spark.sql.defaultCatalog"] == "iceberg_catalog"

    def test_render_s3tables_on_wherobots_federates_glue_id(self, _aws_ctx):
        cfg = render_catalog(
            _S3TABLES_SPEC, Platform.WHEROBOTS, bucket="overture-managed-iceberg-dev"
        )
        assert (
            cfg["spark.sql.catalog.s3tables_catalog.catalog-impl"]
            == "org.apache.iceberg.aws.glue.GlueCatalog"
        )
        # The critical federation routing: glue.id points at the S3 Tables
        # bucket.
        assert (
            cfg["spark.sql.catalog.s3tables_catalog.glue.id"]
            == f"{_ACCOUNT}:s3tablescatalog/overture-managed-iceberg-dev"
        )
        assert "spark.sql.defaultCatalog" not in cfg

    def test_render_s3tables_requires_bucket(self, _aws_ctx):
        with pytest.raises(ValueError, match="bucket is required"):
            render_catalog(_S3TABLES_SPEC, Platform.GLUE)

    def test_render_wherobots_glue_requires_warehouse_path(self, _aws_ctx):
        with pytest.raises(ValueError, match="warehouse_path is required"):
            render_catalog(_GLUE_SPEC, Platform.WHEROBOTS)

    def test_render_uses_explicit_account_and_region_overrides(self):
        # No _aws_ctx: proves the overrides bypass the AWS lookups entirely.
        cfg = render_catalog(
            _GLUE_SPEC,
            Platform.GLUE,
            aws_account_id="999999999999",
            aws_region="eu-west-1",
        )
        assert cfg["spark.sql.catalog.iceberg_catalog.warehouse"] == "999999999999"
        assert (
            cfg["spark.sql.catalog.iceberg_catalog.uri"]
            == "https://glue.eu-west-1.amazonaws.com/iceberg"
        )
