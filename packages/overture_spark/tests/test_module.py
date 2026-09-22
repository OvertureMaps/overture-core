import os
import sys
import unittest
from importlib.machinery import ModuleSpec
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from overture_spark import *  # noqa: F403

try:
    import pyspark  # noqa: F401
    from moto.core import set_initial_no_auth_action_count
    from moto.server import ThreadedMotoServer
    from sedona.spark import KryoSerializer, SedonaKryoRegistrator
except ImportError:
    pyspark = None
    ThreadedMotoServer = None
    KryoSerializer = None
    SedonaKryoRegistrator = None

    def set_initial_no_auth_action_count(fn):
        """No-op stand-in so the decorated method still defines cleanly when
        moto[server] isn't installed; the class-level skipif below skips the
        test itself before this ever runs."""
        return fn


needs_spark = pytest.mark.skipif(
    pyspark is None, reason="pyspark not installed; requires the sql-spark extra"
)


class TestSparkPlatform(unittest.TestCase):
    def testConstruction(self):
        self.assertEqual(SparkPlatform.LOCAL, SparkPlatform.from_str("local"))
        self.assertEqual(SparkPlatform.DATABRICKS, SparkPlatform.from_str("databricks"))
        self.assertEqual(SparkPlatform.GLUE, SparkPlatform.from_str("glue"))
        self.assertEqual(SparkPlatform.WHEROBOTS, SparkPlatform.from_str("wherOBOTS"))

    @patch("overture_spark.SparkPlatform.isRunningInGlue", return_value=False)
    @patch("overture_spark.SparkPlatform.isRunningInDatabricks", return_value=False)
    @patch("overture_spark.SparkPlatform.isRunningInWherobots", return_value=False)
    def testAutoDetect(self, _mock_wherobots, _mock_databricks, _mock_glue):
        self.assertEqual(SparkPlatform.LOCAL, SparkPlatform.autodetect())

    def testIs(self):
        self.assertFalse(SparkPlatform.isRunningInGlue())
        self.assertFalse(SparkPlatform.isRunningInDatabricks())
        self.assertFalse(SparkPlatform.isRunningInDatabricksNotebook())

    @patch.dict(os.environ, {"WHEROBOTS_RUNTIME": "1"}, clear=False)
    @patch("overture_spark.os.path.exists", return_value=False)
    @patch("overture_spark.find_spec", return_value=None)
    def testIsRunningInWherobotsWhenEnvMarkerPresent(
        self, _mock_find_spec, _mock_exists
    ):
        self.assertTrue(SparkPlatform.isRunningInWherobots())

    @patch("overture_spark.os.path.exists")
    @patch("overture_spark.find_spec", return_value=None)
    def testIsRunningInWherobotsWhenPathPresent(self, _mock_find_spec, mock_exists):
        mock_exists.side_effect = lambda p: (
            p == "/home/wherobots/run-scripts/run_submit.py"
        )
        self.assertTrue(SparkPlatform.isRunningInWherobots())

    @patch("overture_spark.find_spec")
    def testIsRunningInWherobotsWhenModuleIsPresent(self, mock_find_spec):
        mock_find_spec.return_value = ModuleSpec("wherobots.db", loader=None)

        self.assertTrue(SparkPlatform.isRunningInWherobots())

    @patch("overture_spark.sys.prefix", "/tmp/venv")
    @patch("overture_spark.os.path.exists", return_value=False)
    @patch("overture_spark.find_spec")
    def testIsNotRunningInWherobotsWhenModuleIsMissing(
        self, mock_find_spec, _mock_exists
    ):
        mock_find_spec.return_value = None

        self.assertFalse(SparkPlatform.isRunningInWherobots())


@pytest.mark.spark
@needs_spark
class TestSparkSedona(unittest.TestCase):
    DEFAULT_SCALA_VERSION = "2.12"

    def testPySparkVersion(self):
        self.assertIn(
            ".".join(SparkSedona.getPySparkVersion().split(".")[:2]),
            ["3.3", "3.4", "3.5"],
            "Unsupported pyspark version",
        )

    def testPythonVersion(self):
        self.assertEqual(3, sys.version_info.major)
        self.assertIn(sys.version_info.minor, [8, 9, 10, 11, 12, 13])

    def testSedonaVersion(self):
        self.assertIn(
            SparkSedona.getSedonaVersion(),
            ["1.6.1", "1.7.0", "1.7.2"],
            "Unsupported apache-sedona version",
        )

    def testSparkVersionsForSedona(self):
        self.assertEqual("3.0", SparkSedona.getSparkVersionForSedona("3.0", "1.6.1"))
        self.assertEqual("3.0", SparkSedona.getSparkVersionForSedona("3.1.2", "1.6.1"))
        self.assertEqual("3.0", SparkSedona.getSparkVersionForSedona("3.2", "1.6.1"))
        self.assertEqual("3.0", SparkSedona.getSparkVersionForSedona("3.3.4", "1.6.1"))
        self.assertEqual("3.3", SparkSedona.getSparkVersionForSedona("3.3.4", "1.7.2"))
        self.assertEqual("3.4", SparkSedona.getSparkVersionForSedona("3.4", "1.6.1"))
        self.assertEqual("3.5", SparkSedona.getSparkVersionForSedona("3.5.1", "1.6.1"))

    def testSedonaJarPackages(self):
        self.assertEqual(
            0,
            len(
                SparkSedona.getSedonaJarPackages(
                    spark_platform=SparkPlatform.DATABRICKS,
                    scala_version=self.DEFAULT_SCALA_VERSION,
                    py_spark_version="3.3.0",
                    sedona_version="1.6.1",
                )
            ),
        )
        self.assertEqual(
            2,
            len(
                SparkSedona.getSedonaJarPackages(
                    spark_platform=SparkPlatform.LOCAL,
                    scala_version=self.DEFAULT_SCALA_VERSION,
                    py_spark_version="3.3.0",
                    sedona_version="1.6.1",
                )
            ),
        )
        self.assertTrue(
            "shaded"
            in SparkSedona.getSedonaJarPackages(
                scala_version=self.DEFAULT_SCALA_VERSION,
                spark_platform=SparkPlatform.GLUE,
                py_spark_version="3.3.0",
                sedona_version="1.6.1",
            )[0]
        )
        self.assertFalse(
            "shaded"
            in SparkSedona.getSedonaJarPackages(
                scala_version=self.DEFAULT_SCALA_VERSION,
                spark_platform=SparkPlatform.LOCAL,
                py_spark_version="3.3.0",
                sedona_version="1.6.1",
            )[0]
        )


@pytest.mark.spark
@needs_spark
class TestLocalSparkSedonaSession(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.spark = getSparkSedonaSession(
            spark_platform=SparkPlatform.LOCAL,
            app_name="LocalTest",
            extra_spark_conf={
                "spark.driver.memory": "2g",
                "spark.executor.memory": "1g",
            },
        )

        # Create a temporary view with coordinates
        data = [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]
        columns = ["longitude", "latitude"]
        df = self.spark.createDataFrame(data, columns)
        df.createOrReplaceTempView("coordinates")

    @classmethod
    def tearDown(self):
        self.spark.stop()

    def testDefaultSparkProperties(self):
        self.assertEqual(
            KryoSerializer.getName, self.spark.conf.get("spark.serializer")
        )
        self.assertEqual(
            SedonaKryoRegistrator.getName,
            self.spark.conf.get("spark.kryo.registrator"),
        )
        self.assertEqual(
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
            self.spark.conf.get("spark.hadoop.fs.s3.impl"),
        )
        self.assertTrue(":sedona-spark" in self.spark.conf.get("spark.jars.packages"))
        self.assertTrue(
            ":geotools-wrapper:" in self.spark.conf.get("spark.jars.packages")
        )
        self.assertTrue(":hadoop-aws:" in self.spark.conf.get("spark.jars.packages"))

    def testCustomSparkProperties(self):
        self.assertEqual("2g", self.spark.conf.get("spark.driver.memory"))
        self.assertEqual("1g", self.spark.conf.get("spark.executor.memory"))

    def testSessionSparkProperty(self):
        self.spark.conf.set("spark.sql.shuffle.partitions", "123")
        self.assertEqual("123", self.spark.conf.get("spark.sql.shuffle.partitions"))

    def testIsLocalSpark(self):
        self.assertTrue(isLocalSpark(self.spark))

    def testSedonaSql(self):
        self.spark.sql(
            "SELECT ST_Point(longitude, latitude) AS point FROM coordinates"
        ).createOrReplaceTempView("points")

        points_df = self.spark.sql("SELECT ST_AsText(point) AS wkt FROM points")
        points = points_df.collect()
        actual_points = [row["wkt"] for row in points]
        expected_points = ["POINT (1 2)", "POINT (3 4)", "POINT (5 6)"]
        self.assertEqual(expected_points, actual_points)


@pytest.mark.spark
@needs_spark
class TestHadoopAwsInstaller(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.hadoopAwsInstaller = HadoopAwsInstaller()
        self.jarPackages = self.hadoopAwsInstaller.install()
        self.hadoopVersion = self.hadoopAwsInstaller._getInstalledVersion()

    def testVersion(self):
        self.assertIsNotNone(self.hadoopVersion)

    def testJarPackages(self):
        self.assertEqual(1, len(self.jarPackages))
        self.assertEqual(
            f"org.apache.hadoop:hadoop-aws:{self.hadoopVersion}", self.jarPackages[0]
        )


@pytest.mark.spark
@needs_spark
class TestMockS3WithDummyAuth(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.motoServer = ThreadedMotoServer(port=0)
        self.motoServer.start()
        host, port = self.motoServer.get_host_and_port()
        self.motoServerEndpoint = f"http://{host}:{port}"
        self.spark = getSparkSedonaSession(
            spark_platform=SparkPlatform.LOCAL,
            app_name="MockS3DummyAuth",
            extra_spark_conf={
                "spark.hadoop.fs.s3a.endpoint": f"{self.motoServerEndpoint}",
                "spark.hadoop.fs.s3a.access.key": "dummy_for_moto",
                "spark.hadoop.fs.s3a.secret.key": "dummy_for_moto",
            },
        )

    @classmethod
    def tearDown(self):
        self.spark.stop()
        self.motoServer.stop()

    @mock_aws
    def testS3AccessFromSpark(self):
        s3_client = boto3.client(
            "s3", region_name="us-east-1", endpoint_url=self.motoServerEndpoint
        )
        bucket = "mock-bucket"
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(
            Bucket=bucket, Key="data/test.csv", Body="id,name\n1,Alice\n2,Bob"
        )

        s3_path = "s3://mock-bucket/data/test.csv"
        df = self.spark.read.csv(s3_path, header=True, inferSchema=True)
        df.show()

        self.assertEqual(df.count(), 2)
        self.assertEqual(df.columns, ["id", "name"])

        data = df.collect()
        self.assertEqual(data[0]["id"], 1)
        self.assertEqual(data[0]["name"], "Alice")
        self.assertEqual(data[1]["id"], 2)
        self.assertEqual(data[1]["name"], "Bob")


@pytest.mark.spark
@needs_spark
class TestMockS3NoAuth(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.motoServer = ThreadedMotoServer(port=0)
        self.motoServer.start()
        host, port = self.motoServer.get_host_and_port()
        self.motoServerEndpoint = f"http://{host}:{port}"
        self.spark = getSparkSedonaSession(
            spark_platform=SparkPlatform.LOCAL,
            app_name="MockS3NoAuth",
            extra_spark_conf={
                "spark.hadoop.fs.s3a.endpoint": f"{self.motoServerEndpoint}"
            },
        )

    @classmethod
    def tearDown(self):
        self.spark.stop()
        self.motoServer.stop()

    @set_initial_no_auth_action_count  # disables authentication
    @mock_aws
    def testS3AccessFromSpark(self):
        s3_client = boto3.client(
            "s3", region_name="us-east-1", endpoint_url=self.motoServerEndpoint
        )
        bucket = "mock-bucket"
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(
            Bucket=bucket, Key="data/test.csv", Body="id,name\n1,Alice\n2,Bob"
        )

        s3_path = "s3://mock-bucket/data/test.csv"
        df = self.spark.read.csv(s3_path, header=True, inferSchema=True)
        df.show()

        self.assertEqual(df.count(), 2)
        self.assertEqual(df.columns, ["id", "name"])

        data = df.collect()
        self.assertEqual(data[0]["id"], 1)
        self.assertEqual(data[0]["name"], "Alice")
        self.assertEqual(data[1]["id"], 2)
        self.assertEqual(data[1]["name"], "Bob")


if __name__ == "__main__":
    unittest.main()
