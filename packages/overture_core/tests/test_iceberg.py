"""Unit tests for the pure Iceberg catalog identity types.

``render_catalog`` and the ``get_*_table()`` helpers stay in tf-data-platform
(they depend on AWS account/region/namespace lookups), so only the catalog
identity types moved here are exercised: the enums, ``CatalogSpec``, and the
``CatalogBinding`` validation in its ``__post_init__``.
"""

import pytest

from overture_core.iceberg import (
    ICEBERG_CATALOG,
    ICEBERG_CATALOG_SPEC,
    S3TABLES_CATALOG_ALIAS,
    S3TABLES_CATALOG_SPEC,
    CatalogBinding,
    CatalogKind,
    CatalogSpec,
    Platform,
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
