import json
import unittest
from unittest.mock import Mock

import boto3
import pytest
from moto import mock_aws

from overture_spark.job import MissingParameterError, SparkSedonaJob
from overture_spark.secret_engines import AwsSecretsManager

try:
    import pyspark  # noqa: F401
except ImportError:
    pyspark = None


@pytest.mark.spark
@pytest.mark.skipif(
    pyspark is None, reason="pyspark not installed; requires the sql-spark extra"
)
class TestInvalidJob(unittest.TestCase):
    def testInvalidJob(self):
        class JobTest(SparkSedonaJob):
            def execute_job(self):
                self.spark.sql("SELECT ST_NotExistingSedonaFunction(1, 1)").show()

        result = JobTest().run()
        self.assertFalse(result.isSuccess)
        self.assertTrue(result.elapsed > 0.0)
        self.assertIsNotNone(result.exception)
        self.assertIsNotNone(result.exception_traceback)


@pytest.mark.spark
@pytest.mark.skipif(
    pyspark is None, reason="pyspark not installed; requires the sql-spark extra"
)
class TestRealSedonaJob(unittest.TestCase):
    def testJobWithoutParameters(self):
        class JobTest(SparkSedonaJob):
            def execute_job(self):
                self.spark.sql("SELECT ST_Point(1, 2)").show()

        result = JobTest().run()
        self.assertTrue(result.isSuccess)
        self.assertTrue(result.elapsed > 0.0)
        self.assertIsNone(result.exception)
        self.assertTrue(result.exception_traceback == [])

    def testJobWithParameters(self):
        class JobTest(SparkSedonaJob):
            def execute_job(self):
                long = self.get_param("long")
                lat = self.get_param("lat")
                point = f"{long}, {lat}"

                data = [
                    ("1", "POINT (30 10)"),
                    ("2", "POINT (10 30)"),
                    ("3", "POINT (20 20)"),
                    ("4", "LINESTRING (30 10, 10 30, 40 40)"),
                ]

                columns = ["id", "geometry"]
                df = self.spark.createDataFrame(data, columns)
                df.show()

                self.spark.sql(f"SELECT ST_Point({point})").show()
                self.log_data("point", point)

        result = JobTest().run(params={"long": 1, "lat": 2})
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertEqual("1, 2", result.data["point"])
        self.assertTrue(result.elapsed > 0.0)
        self.assertIsNone(result.exception)
        self.assertTrue(result.exception_traceback == [])


class JobBaseMockSpark(SparkSedonaJob):
    """Base for tests that exercise SparkSedonaJob's plain-Python logic
    (params, logging, secrets) without a real Spark session — a plain
    ``Mock()`` stands in for ``self.spark`` since these tests never call
    into it, so they don't need pyspark installed at all."""

    def __init__(self):
        SparkSedonaJob.__init__(self)
        self.spark = Mock()


class TestJobParameters(unittest.TestCase):
    def testMissingParameterWithoutDefault(self):
        class JobTest(JobBaseMockSpark):
            def execute_job(self):
                self.get_param("missing")

        result = JobTest().run()
        self.assertFalse(result.isSuccess)
        self.assertIsInstance(result.exception, MissingParameterError)
        self.assertEqual("missing", result.exception.param_name)

    def testMissingParameterWithDefault(self):
        class JobTest(JobBaseMockSpark):
            def execute_job(self):
                p = self.get_param("missing", default_value="123")

        result = JobTest().run()
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertIsNone(result.exception)

    def testMissingParameterWithEmptyDefault(self):
        class JobTest(JobBaseMockSpark):
            def execute_job(self):
                self.get_param("missing", default_value=" ")

        result = JobTest().run()
        self.assertFalse(result.isSuccess)
        self.assertIsInstance(result.exception, MissingParameterError)
        self.assertEqual("missing", result.exception.param_name)

    def testOverrideDefaultValue(self):
        class JobTest(JobBaseMockSpark):
            def execute_job(self):
                self.get_param("provided", default_value=123)

        result = JobTest().run({"provided": 456})
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertIsNone(result.exception)
        self.assertEqual(456, result.params["provided"])


class TestLogData(unittest.TestCase):
    def testLogString(self):
        class JobTest(JobBaseMockSpark):
            def execute_job(self):
                self.log_data("k", "v")

        result = JobTest().run()
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertEqual("v", result.data["k"])

    def testLogInt(self):
        class JobTest(JobBaseMockSpark):
            def execute_job(self):
                self.log_data("k", 123)

        result = JobTest().run()
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertEqual(123, result.data["k"])

    def testLogDict(self):
        class JobTest(JobBaseMockSpark):
            def execute_job(self):
                self.log_data("k", {"abc": 123})

        result = JobTest().run()
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertEqual({"abc": 123}, result.data["k"])


class TestSecrets(unittest.TestCase):
    @mock_aws
    def testOutsideDatabricks(self):
        boto3.client("secretsmanager", region_name="us-west-2").create_secret(
            Name="keyvaultsecret",
            SecretString=json.dumps({"lakefskey": "AKIAEXAMPLE1234567890"}),
        )

        class JobTest(JobBaseMockSpark):
            def execute_job(self):
                self.log_data("lakefs_secret", self.get_secret("lakefskey"))

        result = JobTest().with_secrets_engine(AwsSecretsManager()).run()
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertEqual("AKIAEXAMPLE1234567890", result.data["lakefs_secret"])


if __name__ == "__main__":
    unittest.main()
