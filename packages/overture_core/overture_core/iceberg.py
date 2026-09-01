"""Overture Iceberg catalog configuration."""

from dataclasses import dataclass
from enum import Enum, auto

from overture_core.cloud.aws.core import get_account_id as get_current_aws_account_id
from overture_core.cloud.aws.core import get_region as get_aws_region

# Iceberg catalog implementation classes.
_SPARK_CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"
_REST_CATALOG_IMPL = "org.apache.iceberg.rest.RESTCatalog"
_GLUE_CATALOG_IMPL = "org.apache.iceberg.aws.glue.GlueCatalog"
_S3_FILE_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO"

# Bigger AWS SDK HTTP connection pool to avoid pool-timeout errors under
# concurrent S3FileIO load.
# https://stackoverflow.com/questions/77960050/when-using-iceberg-with-emr-7-0-0-with-s3-i-got-awssdk-sdkclientexception-timeo
_MAX_HTTP_CONNECTIONS = 3000

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


def render_catalog(
    spec: CatalogSpec,
    platform: Platform,
    *,
    aws_account_id: str | None = None,
    aws_region: str | None = None,
    bucket: str | None = None,
    warehouse_path: str | None = None,
) -> dict:
    """Render the Spark config for one catalog on one platform.

    Single source of platform-specific Iceberg wiring: REST vs GlueCatalog, the
    S3 Tables federation ``glue.id``, the ARN-vs-plain-S3 warehouse form, and
    SigV4 signing all live here. Callers describe WHAT catalog they want (spec)
    and WHERE it runs (platform); this decides HOW.

    Args:
        spec: The catalog identity to render.
        platform: Execution platform selecting the access mechanism.
        aws_account_id: Overrides the caller account (e.g. cross-account prod
            reads). Defaults to the current account.
        aws_region: Overrides the region. Defaults to the current region.
        bucket: S3 Tables bucket. Required for S3_TABLES catalogs; ignored for
            GLUE_DATA_CATALOG.
        warehouse_path: GlueCatalog warehouse for the primary catalog on
            Wherobots (e.g. ``s3://<bucket>/``).

    Returns:
        A Spark configuration dict for the single catalog.
    """
    alias = spec.alias
    account_id = aws_account_id or get_current_aws_account_id()
    region = aws_region or get_aws_region()

    if spec.kind is CatalogKind.S3_TABLES and not bucket:
        raise ValueError(f"bucket is required to render S3 Tables catalog {alias!r}")
    if (
        platform is Platform.WHEROBOTS
        and spec.kind is CatalogKind.GLUE_DATA_CATALOG
        and not warehouse_path
    ):
        raise ValueError(
            f"warehouse_path is required to render GlueCatalog {alias!r} on Wherobots"
        )

    def key(suffix: str) -> str:
        return f"spark.sql.catalog.{alias}.{suffix}"

    config = {
        "spark.sql.extensions": ICEBERG_SPARK_EXTENSIONS,
        f"spark.sql.catalog.{alias}": _SPARK_CATALOG_IMPL,
        key("http-client.apache.max-connections"): _MAX_HTTP_CONNECTIONS,
    }
    # Only the primary catalog sets the session default; a secondary catalog must
    # not, or merging its config would clobber the default.
    if spec.is_default:
        config["spark.sql.defaultCatalog"] = alias

    if platform is Platform.GLUE:
        # RESTCatalog against the AWS Iceberg REST endpoints. catalog-impl (not
        # type=rest): Glue 4.0 bundles Iceberg 1.0.0, which does not recognise
        # the "rest" shorthand (added in 1.1.0); loading the class directly works
        # on 1.0.0 and all future versions.
        config[key("catalog-impl")] = _REST_CATALOG_IMPL
        config[key("rest.sigv4-enabled")] = "true"
        if spec.kind is CatalogKind.GLUE_DATA_CATALOG:
            config[key("uri")] = f"https://glue.{region}.amazonaws.com/iceberg"
            config[key("warehouse")] = account_id
            config[key("rest.signing-name")] = "glue"
        else:  # S3_TABLES REST endpoint
            config[key("uri")] = f"https://s3tables.{region}.amazonaws.com/iceberg"
            config[key("warehouse")] = (
                f"arn:aws:s3tables:{region}:{account_id}:bucket/{bucket}"
            )
            config[key("rest.signing-name")] = "s3tables"
            config[key("rest.signing-region")] = region
            # S3FileIO for table data/metadata IO (AWS S3 Tables guidance).
            config[key("io-impl")] = _S3_FILE_IO_IMPL
            # The S3 Tables REST endpoint has no metrics endpoint; leaving this
            # on causes failed async metric POSTs.
            config[key("rest-metrics-reporting-enabled")] = "false"
    else:  # Platform.WHEROBOTS -- client-side GlueCatalog
        config[key("catalog-impl")] = _GLUE_CATALOG_IMPL
        config[key("glue.account-id")] = account_id
        if spec.kind is CatalogKind.GLUE_DATA_CATALOG:
            config[key("warehouse")] = warehouse_path
        else:  # S3_TABLES via Glue's S3 Tables federation
            # S3 Tables namespaces are only visible via Glue's federation,
            # addressed by glue.id -- NOT glue.account-id (ARN-construction only).
            # Without glue.id, GlueCatalog hits the plain account catalog and
            # throws EntityNotFound, which callers misreport as "namespace does
            # not exist". Warehouse is a plain S3 path; GlueCatalog ignores the
            # arn:aws:s3tables:... ARN the REST catalog uses.
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-glue.html
            # https://github.com/apache/iceberg/blob/main/aws/src/main/java/org/apache/iceberg/aws/AwsProperties.java
            config[key("warehouse")] = f"s3://{bucket}/warehouse/"
            config[key("glue.id")] = f"{account_id}:s3tablescatalog/{bucket}"

    return config
