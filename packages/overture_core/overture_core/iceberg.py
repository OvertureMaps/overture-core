"""Overture Iceberg catalog configuration."""

from dataclasses import dataclass
from enum import Enum, auto

ICEBERG_CATALOG = "iceberg_catalog"
S3TABLES_CATALOG_ALIAS = "s3tables_catalog"

# Iceberg + Sedona Spark SQL extensions. Iceberg's extensions are required for
# DDL/DML such as the bucket() partition transform, MERGE/UPDATE/DELETE, and
# stored procedures; the Sedona extensions register its spatial SQL. Kept as a
# single constant so every catalog config sets the identical value (a divergent
# spark.sql.extensions on any platform would clobber one set of extensions).
ICEBERG_SPARK_EXTENSIONS = (
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
    "org.apache.sedona.viz.sql.SedonaVizExtensions,"
    "org.apache.sedona.sql.SedonaSqlExtensions"
)

# There is exactly one S3 Tables bucket per environment; every s3tables
# namespace (violations, geocoder, embeddings, ...) lives in it. Emitted as a
# Jinja template into REST (Glue/Databricks) configs, which the operator
# resolves at runtime; Wherobots resolves it to a concrete bucket at parse time
# (see spark_factory).
MANAGED_ICEBERG_BUCKET_TEMPLATE = "{{ var.value.managed_bucket_iceberg }}"


class Platform(Enum):
    """Execution platform, which determines HOW a catalog is reached.

    GLUE reaches the AWS Iceberg REST endpoints with native credentials (also
    covers Databricks). WHEROBOTS cannot reach those endpoints, so it uses the
    client-side GlueCatalog with cross-account credential delegation.
    """

    GLUE = auto()
    WHEROBOTS = auto()


class CatalogKind(Enum):
    """The logical catalog being addressed, independent of platform.

    GLUE_DATA_CATALOG is the account's Glue Data Catalog (``iceberg_catalog``).
    S3_TABLES is Amazon S3 Tables (``s3tables_catalog``), which live in a single
    table bucket per environment.
    """

    GLUE_DATA_CATALOG = auto()
    S3_TABLES = auto()


@dataclass(frozen=True)
class CatalogSpec:
    """Declarative identity of one Iceberg catalog, rendered per platform by
    ``render_catalog``.

    alias is the Spark catalog name (``spark.sql.catalog.<alias>``). is_default
    controls ``spark.sql.defaultCatalog`` -- only the primary catalog sets it, or
    merging a secondary catalog's config would clobber the default.
    """

    alias: str
    kind: CatalogKind
    is_default: bool = False


# The two catalogs every platform registers. Catalog identity is declared here
# once; platform-specific Spark keys are derived in render_catalog. Build configs
# from these specs via render_catalog (Glue/Databricks) or a CatalogBinding fed to
# the spark_factory (platform-agnostic).
ICEBERG_CATALOG_SPEC = CatalogSpec(
    ICEBERG_CATALOG, CatalogKind.GLUE_DATA_CATALOG, is_default=True
)
S3TABLES_CATALOG_SPEC = CatalogSpec(S3TABLES_CATALOG_ALIAS, CatalogKind.S3_TABLES)


@dataclass(frozen=True)
class CatalogBinding:
    """A catalog a job needs, declared independent of platform.

    Lets a DAG say WHICH catalog it wants without naming a platform: the
    spark_factory renders the binding for every platform the task group may run on
    (Glue/Databricks and Wherobots) and packs each into the matching IcebergConfig
    slot, so the provider picks the right one at runtime. This avoids hardcoding
    ``Platform.GLUE`` in a DAG that can be dispatched to Wherobots.

    Supply exactly one bucket source for an S3 Tables catalog:

    bucket     - a concrete bucket name, used verbatim on every platform (e.g. a
                 fixed cross-account prod bucket).
    bucket_var - an Airflow Variable name resolved per platform: a Jinja
                 ``{{ var.value.<name> }}`` template on Glue/Databricks (the
                 operator resolves it at run time) and a concrete parse-time
                 ``Variable.get`` on Wherobots (which does not template configs).
    """

    spec: CatalogSpec
    bucket: str | None = None
    bucket_var: str | None = None
    aws_account_id: str | None = None
    aws_region: str | None = None

    def __post_init__(self):
        if self.bucket and self.bucket_var:
            raise ValueError(
                f"CatalogBinding for {self.spec.alias!r}: set bucket or bucket_var, "
                "not both."
            )
        if self.spec.kind is CatalogKind.S3_TABLES and not (
            self.bucket or self.bucket_var
        ):
            raise ValueError(
                f"CatalogBinding for S3 Tables catalog {self.spec.alias!r} requires a "
                "bucket or bucket_var."
            )
