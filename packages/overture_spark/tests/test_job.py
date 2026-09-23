import json
import unittest
from unittest.mock import Mock

import boto3
import pytest
from moto import mock_aws

from overture_spark.job import (
    JobResult,
    MissingParameterError,
    SparkSedonaJob,
    pretty_print_elapsed_time,
)
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


class TestJobResult(unittest.TestCase):
    def testLogAppendsMessage(self):
        result = JobResult()
        result.log("hello")
        self.assertEqual(["hello"], result.messages)

    def testToJsonIncludesExceptionAsString(self):
        result = JobResult()
        result.exception = ValueError("boom")
        result.exception_traceback = ["Traceback...", "ValueError: boom"]
        result.params = {"a": 1}
        result.data = {"b": 2}
        as_json = result.to_json()
        self.assertEqual("boom", as_json["exception"])
        self.assertEqual(
            ["Traceback...", "ValueError: boom"], as_json["exception_traceback"]
        )
        self.assertEqual({"a": 1}, as_json["params"])
        self.assertEqual({"b": 2}, as_json["data"])

    def testToJsonExceptionIsNoneWhenNoException(self):
        self.assertIsNone(JobResult().to_json()["exception"])


class TestPrettyPrintElapsedTime(unittest.TestCase):
    def testFormatsHoursMinutesSecondsMilliseconds(self):
        self.assertEqual("01:02:03.004", pretty_print_elapsed_time(3723004))


class TestLog(unittest.TestCase):
    """SparkSedonaJob.log() itself, distinct from TestLogData's log_data()."""

    def testLogStringMessage(self):
        class JobTest(JobBaseMockSpark):
            def execute_job(self):
                self.log("plain message")

        result = JobTest().run()
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertTrue(any("plain message" in m for m in result.messages))

    def testLogDictMessage(self):
        class JobTest(JobBaseMockSpark):
            def execute_job(self):
                self.log({"k": "v"})

        result = JobTest().run()
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertIn({"k": "v"}, result.messages)


class TestRunInvalidParams(unittest.TestCase):
    def testInvalidJsonParamsMarksJobFailed(self):
        class JobTest(JobBaseMockSpark):
            def execute_job(self):
                pass

        result = JobTest().run("not-json")
        self.assertFalse(result.isSuccess)
        self.assertIn("Cannot parse params", str(result.exception))


class TestCheckOutputWritable(unittest.TestCase):
    class _ConcreteJob(JobBaseMockSpark):
        def execute_job(self):
            pass

    def _fs_mock(self, job):
        jvm = job.spark._jvm
        return jvm.org.apache.hadoop.fs.FileSystem.get.return_value

    def testCreatesMissingDirectoryAndWritesProbeFile(self):
        job = self._ConcreteJob()
        fs = self._fs_mock(job)
        fs.exists.return_value = False
        fs.mkdirs.return_value = True
        stream = fs.create.return_value

        job.check_output_writable("s3://bucket/prefix")

        fs.mkdirs.assert_called_once()
        stream.write.assert_called_once_with(0)
        stream.close.assert_called_once()
        fs.delete.assert_called_once()

    def testRaisesPermissionErrorWhenMkdirsFails(self):
        job = self._ConcreteJob()
        fs = self._fs_mock(job)
        fs.exists.return_value = False
        fs.mkdirs.return_value = False

        with self.assertRaises(PermissionError):
            job.check_output_writable("s3://bucket/prefix")

    def testRaisesPermissionErrorWhenWriteFails(self):
        job = self._ConcreteJob()
        fs = self._fs_mock(job)
        fs.exists.return_value = True
        stream = fs.create.return_value
        stream.write.side_effect = OSError("disk full")

        with self.assertRaises(PermissionError):
            job.check_output_writable("s3://bucket/prefix")
        fs.delete.assert_called_once()


@pytest.mark.spark
@pytest.mark.skipif(
    pyspark is None, reason="pyspark not installed; requires the sql-spark extra"
)
class TestInitSparkForPlatformInvalidConf(unittest.TestCase):
    """init_spark_for_platform imports pyspark.sql.SQLContext at the top of
    the method (see its lazy-import note in job.py), so this needs the
    sql-spark extra even though the invalid-JSON error it raises here never
    reaches a real Spark session."""

    def testInvalidExtraSparkConfRaisesBeforeBuildingSession(self):
        class JobTest(SparkSedonaJob):
            def execute_job(self):
                pass

        job = JobTest()
        with self.assertRaises(Exception):
            job.init_spark_for_platform(extra_spark_conf="not-json")


if __name__ == "__main__":
    unittest.main()
