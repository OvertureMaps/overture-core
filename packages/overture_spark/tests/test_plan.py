import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

import pytest

from overture_spark.plan import (
    DEFAULT_MAX_PLAN_CHARS,
    PlanTooLargeError,
    assert_plan_size,
    plan_size,
)

try:
    import pyspark  # noqa: F401
except ImportError:
    pyspark = None


def df_with_plan(text: str) -> Mock:
    df = Mock()
    df._jdf.queryExecution.return_value.analyzed.return_value.toString.return_value = (
        text
    )
    return df


class TestPlanSizeMocked(unittest.TestCase):
    def testPlanSizeIsAnalyzedPlanStringLength(self):
        self.assertEqual(5, plan_size(df_with_plan("abcde")))

    def testAssertPassesAtLimitAndReturnsSize(self):
        self.assertEqual(5, assert_plan_size(df_with_plan("abcde"), 5))

    def testAssertFailsOverLimitWithSizeAndLimitInMessage(self):
        with self.assertRaises(PlanTooLargeError) as ctx:
            assert_plan_size(df_with_plan("a" * 1234), 1000)
        self.assertEqual(1234, ctx.exception.size)
        self.assertEqual(1000, ctx.exception.max_chars)
        self.assertIn("1,234", str(ctx.exception))
        self.assertIn("1,000", str(ctx.exception))

    def testErrorIsAnAssertionError(self):
        self.assertTrue(issubclass(PlanTooLargeError, AssertionError))

    def testDefaultLimitIsPositive(self):
        self.assertGreater(DEFAULT_MAX_PLAN_CHARS, 0)


@pytest.mark.spark
@pytest.mark.skipif(
    pyspark is None, reason="pyspark not installed; requires the sql-spark extra"
)
class TestPlanSizeRealSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from overture_spark import SparkPlatform, getSparkSedonaSession

        cls.spark = getSparkSedonaSession(
            spark_platform=SparkPlatform.LOCAL, app_name="PlanSizeTest"
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _union_chain(self, n):
        base = self.spark.range(1)
        df = base
        for _ in range(n):
            df = df.union(base)
        return df

    def testGrowingUnionPlanFailsAndFlatPlanPasses(self):
        flat = self.spark.range(1).select("id")
        grown = self._union_chain(50)
        limit = plan_size(flat) * 10
        self.assertEqual(plan_size(flat), assert_plan_size(flat, limit))
        with self.assertRaises(PlanTooLargeError):
            assert_plan_size(grown, limit)

    def testDefaultMaxPlanStringLengthIsSet(self):
        self.assertEqual(
            "100000000", self.spark.conf.get("spark.sql.maxPlanStringLength")
        )

    def testPlanStringIsTruncatedPastConfiguredLimit(self):
        grown = self._union_chain(50)
        full = len(grown._jdf.queryExecution().toString())
        full_plan = plan_size(grown)
        self.spark.conf.set("spark.sql.maxPlanStringLength", "500")
        try:
            truncated = grown._jdf.queryExecution().toString()
            capped_plan = plan_size(grown)
        finally:
            self.spark.conf.set("spark.sql.maxPlanStringLength", "100000000")
        self.assertLess(len(truncated), full)
        self.assertLessEqual(len(truncated), 500 + 100)
        self.assertLess(capped_plan, full_plan)


@pytest.mark.spark
@pytest.mark.skipif(
    pyspark is None, reason="pyspark not installed; requires the sql-spark extra"
)
class TestAqeEventLogTruncation(unittest.TestCase):
    """The prod crash was the AQE event-log update
    (SparkListenerSQLAdaptiveExecutionUpdate) rendering a huge plan string."""

    LIMIT = 500

    def testAqeEventLogPlanDescriptionIsTruncatedAndJobCompletes(self):
        from overture_spark import SparkPlatform, getSparkSedonaSession

        with tempfile.TemporaryDirectory() as log_dir:
            spark = getSparkSedonaSession(
                spark_platform=SparkPlatform.LOCAL,
                app_name="AqeEventLogTest",
                extra_spark_conf={
                    "spark.eventLog.enabled": "true",
                    "spark.eventLog.dir": Path(log_dir).as_uri(),
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.maxPlanStringLength": str(self.LIMIT),
                },
            )
            try:
                base = spark.range(1)
                df = base
                for _ in range(50):
                    df = df.union(base)
                # groupBy adds a shuffle, so AQE re-plans and emits updates
                result = df.groupBy("id").count().collect()
            finally:
                spark.stop()

            self.assertEqual(1, len(result))
            self.assertEqual(51, result[0]["count"])

            events = [
                json.loads(line)
                for log in Path(log_dir).iterdir()
                for line in log.read_text().splitlines()
            ]

        updates = [
            e
            for e in events
            if e["Event"].endswith("SparkListenerSQLAdaptiveExecutionUpdate")
        ]
        self.assertTrue(updates, "no AQE update event was logged")
        for event in updates:
            description = event["physicalPlanDescription"]
            self.assertLessEqual(len(description), self.LIMIT + 200)
            self.assertIn("more characters", description)
