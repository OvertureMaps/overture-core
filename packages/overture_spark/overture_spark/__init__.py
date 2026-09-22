"""Cluster-side Spark/Sedona runtime for Overture jobs.

Installed and imported **on** the Spark cluster (Glue / Databricks / Wherobots /
local) and runs **inside** the executor. This is deliberately separate from the
Airflow orchestration layer:

- Orchestration (launch, cluster sizing, asset staging) lives in the external,
  Overture-agnostic ``overture-airflow-provider``.
- This package is the cluster-execution-time side: it builds the configured
  Sedona ``SparkSession`` (``getSparkSedonaSession``), resolves Sedona/GeoTools
  JAR coordinates (``SparkSedona``), and detects the platform
  (``SparkPlatform.autodetect``). ``SparkSedonaJob`` (in ``job.py``) is the base
  class user jobs subclass; the provider's runners import those job classes and
  call ``run()``.

Rule of thumb: launches a job via Airflow/AWS APIs -> provider/factory; runs inside
the Spark job -> here.

``pyspark``/``apache-sedona`` are only imported lazily, inside
``getSparkSedonaSession()``, the one function that actually builds a Spark
session. Everything else in this module (platform detection, version/JAR
helpers) is plain Python, so it — and the tests that only exercise it — never
pays the JVM cost of installing/starting Spark. Callers that do need a real
session install the ``sql-spark`` extra.
"""

import os
import site
import sys
from enum import IntEnum, auto
from importlib.util import find_spec
from typing import TYPE_CHECKING, Dict, List

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class PackageNotFoundError(Exception):
    def __init__(self, package_name):
        super().__init__(
            f"Package '{package_name}' is not installed or could not be found."
        )
        self.package_name = package_name


def get_package_version(package_name):
    if sys.version_info >= (3, 8):
        # Use importlib.metadata for Python 3.8 and above
        try:
            from importlib.metadata import version
        except ImportError:
            from importlib_metadata import (
                version,
            )  # For compatibility with older pip-installed versions
        try:
            return version(package_name)
        except Exception:
            raise PackageNotFoundError(package_name)
    else:
        # Use pkg_resources for older Python versions
        import pkg_resources

        try:
            return pkg_resources.get_distribution(package_name).version
        except pkg_resources.DistributionNotFound:
            raise PackageNotFoundError(package_name)


class SparkPlatform(IntEnum):
    GLUE = auto()
    DATABRICKS = auto()
    WHEROBOTS = auto()
    LOCAL = auto()

    @classmethod
    def autodetect(cls):
        if cls.isRunningInGlue():
            return SparkPlatform.GLUE
        elif cls.isRunningInDatabricks():
            return SparkPlatform.DATABRICKS
        elif cls.isRunningInWherobots():
            return SparkPlatform.WHEROBOTS
        else:
            return SparkPlatform.LOCAL

    @classmethod
    def from_str(cls, name):
        # Normalize the input name to uppercase
        normalized_name = name.upper()
        if normalized_name in cls.__members__:
            return cls[normalized_name]
        raise ValueError(f"{name} is not a valid platform")

    @classmethod
    def isRunningInDatabricks(cls):
        if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
            return False
        return find_spec("pyspark.dbutils") is not None

    @classmethod
    def isRunningInDatabricksNotebook(cls):
        try:
            dbutils
            return True
        except NameError:
            return False

    @classmethod
    def isRunningInGlue(cls):
        try:
            return find_spec("awsglue.utils") is not None
        except ModuleNotFoundError:
            return False

    @classmethod
    def isRunningInWherobots(cls):
        marker_env_vars = (
            "WHEROBOTS_RUNTIME_VERSION",
            "WHEROBOTS_RUNTIME",
            "WHEROBOTS_ENV",
            "WHEROBOTS_CLUSTER_ID",
            "WHEROBOTS_JOB_ID",
        )
        if any(os.getenv(var) for var in marker_env_vars):
            return True

        marker_paths = (
            "/home/wherobots/run-scripts/run_submit.py",
            "/opt/conda/envs/wherobots",
        )
        if any(os.path.exists(path) for path in marker_paths):
            return True

        if "wherobots" in (sys.prefix or "").lower():
            return True

        try:
            return find_spec("wherobots.db") is not None
        except ModuleNotFoundError:
            return False


class SparkSedona:
    @classmethod
    def getPySparkVersion(cls) -> str:
        return get_package_version("pyspark")

    @classmethod
    def getSedonaVersion(cls) -> str:
        return get_package_version("apache-sedona")

    @classmethod
    def getSparkVersionForSedona(
        cls, py_spark_version: str, sedona_version: str
    ) -> str:
        # https://sedona.apache.org/latest-snapshot/setup/install-python/#prepare-sedona-spark-jar
        spark_v = py_spark_version or cls.getPySparkVersion()
        sparkMajorVersion, sparkMinorVersion = spark_v.split(".")[:2]
        sedona_v = sedona_version or cls.getSedonaVersion()
        sedonaMajorVersion, sedonaMinorVersion = sedona_v.split(".")[:2]
        if sparkMajorVersion != "3":
            raise RuntimeError("I'm only supporting spark 3")
        if (
            int(sparkMinorVersion) <= 3
            and int(sedonaMajorVersion) == 1
            and int(sedonaMinorVersion) <= 6
        ):
            sparkMajorMinorVersion = f"{sparkMajorVersion}.0"
        else:
            sparkMajorMinorVersion = f"{sparkMajorVersion}.{sparkMinorVersion}"
        return sparkMajorMinorVersion

    @classmethod
    def getGeotoolsWrapperVersion(cls, sedona_version: str) -> str:
        # see https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/
        geotoolsVersionMap = {
            "1.5.3": "28.2",
            "1.6.1": "28.2",
            "1.7.0": "28.5",
            "1.7.1": "28.5",
            "1.7.2": "28.5",
            "1.8.0": "33.1",
            "1.8.1": "33.1",
        }
        return geotoolsVersionMap[sedona_version]

    @classmethod
    def getSedonaJarPackages(
        cls,
        spark_platform: SparkPlatform,
        scala_version: str,
        py_spark_version: str,
        sedona_version: str,
    ) -> List[str]:
        if spark_platform == SparkPlatform.DATABRICKS:
            # on databricks, sedona packages need to be configured in cluster setup
            return []

        suffix = "-shaded" if spark_platform != SparkPlatform.LOCAL else ""
        packages = [
            f"org.apache.sedona:sedona-spark{suffix}-{cls.getSparkVersionForSedona(py_spark_version=py_spark_version, sedona_version=sedona_version)}_{scala_version}:{sedona_version}",
            f"org.datasyslab:geotools-wrapper:{sedona_version}-{cls.getGeotoolsWrapperVersion(sedona_version=sedona_version)}",
        ]
        return packages


def getSparkSedonaSession(
    spark_platform: SparkPlatform = None,
    app_name: str = "SparkSedonaApp",
    extra_spark_conf: Dict[str, str] = None,
    extra_packages: List[str] = None,
) -> "SparkSession":
    # Lazy: this is the one call site that actually needs pyspark/sedona
    # installed and starts a JVM, see the module docstring.
    from pyspark.sql import SparkSession
    from sedona.spark import KryoSerializer, SedonaContext, SedonaKryoRegistrator

    spark_platform = spark_platform or SparkPlatform.autodetect()
    extra_spark_conf = extra_spark_conf or {}
    extra_packages = extra_packages or []
    if (
        spark_platform == SparkPlatform.DATABRICKS
        and SparkPlatform.isRunningInDatabricksNotebook()
    ):
        global spark
        if len(extra_spark_conf) > 0:
            raise RuntimeError(
                "You need to set extra spark configuration when configuring the databricks cluster"
            )
    else:
        # init a new spark session
        builder = SparkSession.builder.appName(app_name)
        if spark_platform == SparkPlatform.LOCAL:
            hadoopAwsHelper = HadoopAwsInstaller()
            jars = (
                SparkSedona.getSedonaJarPackages(
                    spark_platform=spark_platform,
                    py_spark_version=SparkSedona.getPySparkVersion(),
                    sedona_version=SparkSedona.getSedonaVersion(),
                    scala_version="2.12",
                )
                + hadoopAwsHelper.install()
                + extra_packages
            )
            (
                builder.master("local[*]")  # * = use all available cores
                .config("spark.serializer", KryoSerializer.getName)
                .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
                .config(
                    "spark.jars.repositories",
                    "https://artifacts.unidata.ucar.edu/repository/unidata-all",
                )
                .config("spark.jars.packages", ",".join(jars))
                .config(
                    "spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
                )
            )
        else:
            # sedona jars for other (non-local) spark platforms are configured in SparkSedonaOperator*
            pass
        builder.config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        for k, v in extra_spark_conf.items():
            builder.config(k, v)
        spark = builder.getOrCreate()

    spark = SedonaContext.create(spark)
    if spark_platform == SparkPlatform.GLUE:
        from awsglue.context import GlueContext

        spark = GlueContext(spark).spark_session
    return spark


def isLocalSpark(spark):
    return spark.conf.get("spark.master").startswith("local")


class HadoopAwsInstaller:
    def __init__(self):
        if hasattr(sys, "real_prefix") or (
            hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix
        ):
            # The site.getsitepackages() function returns a list of all site-packages directories
            self.site_packages_path = site.getsitepackages()[0]
            self.jarPath = os.path.join(self.site_packages_path, "pyspark", "jars")
        else:
            self.site_packages_path = None
            self.jarPath = None

    def install(self):
        hadoopVersion = self._getInstalledVersion()
        return [f"org.apache.hadoop:hadoop-aws:{hadoopVersion}"]

    def _getInstalledVersion(self):
        if self.site_packages_path:
            for root, dirs, files in os.walk(self.jarPath):
                for file in files:
                    if file.startswith("hadoop-client-api-"):
                        return os.path.splitext(file)[0].replace(
                            "hadoop-client-api-", ""
                        )
