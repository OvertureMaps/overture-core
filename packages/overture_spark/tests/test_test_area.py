import unittest

import pytest

from overture_spark.test_area import (
    area_intersects_sql,
    area_point_predicate_sql,
    area_polygon_wkt,
    bbox_polygon_wkt,
    filter_df_to_area,
    is_wkt_area,
    parse_area_envelope,
    parse_bbox,
    validate_area,
)

try:
    import pyspark  # noqa: F401

    from overture_spark.job import SparkSedonaJob
except ImportError:
    pyspark = None
    SparkSedonaJob = None

needs_spark = pytest.mark.skipif(
    pyspark is None, reason="pyspark not installed; requires the sql-spark extra"
)

ICELAND = "-25,63,-13,67"
# Triangle covering the west of the Iceland bbox; its envelope is the bbox
# but (-14, 66.5) is inside the envelope and outside the triangle.
ICELAND_TRIANGLE = "POLYGON ((-25 63, -13 63, -25 67, -25 63))"
ICELAND_MULTI = "MULTIPOLYGON (((-25 63, -13 63, -13 67, -25 67, -25 63)))"


class TestParseBbox(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(parse_bbox(ICELAND), (-25.0, 63.0, -13.0, 67.0))

    def test_wrong_arity(self):
        with self.assertRaises(ValueError):
            parse_bbox("-25,63,-13")

    def test_non_numeric(self):
        with self.assertRaises(ValueError):
            parse_bbox("a,b,c,d")

    def test_out_of_range_lon(self):
        with self.assertRaises(ValueError):
            parse_bbox("-181,63,-13,67")

    def test_out_of_range_lat(self):
        with self.assertRaises(ValueError):
            parse_bbox("-25,63,-13,91")

    def test_min_not_below_max(self):
        with self.assertRaises(ValueError):
            parse_bbox("-13,63,-25,67")


class TestWktArea(unittest.TestCase):
    def test_is_wkt_area(self):
        self.assertTrue(is_wkt_area(ICELAND_TRIANGLE))
        self.assertTrue(is_wkt_area("  multipolygon (((0 0, 1 0, 1 1, 0 0)))"))
        self.assertFalse(is_wkt_area(ICELAND))
        self.assertFalse(is_wkt_area(""))

    def test_envelope_of_polygon(self):
        self.assertEqual(
            parse_area_envelope(ICELAND_TRIANGLE), (-25.0, 63.0, -13.0, 67.0)
        )

    def test_envelope_of_multipolygon(self):
        self.assertEqual(parse_area_envelope(ICELAND_MULTI), (-25.0, 63.0, -13.0, 67.0))

    def test_envelope_of_bbox(self):
        self.assertEqual(parse_area_envelope(ICELAND), (-25.0, 63.0, -13.0, 67.0))

    def test_rejects_unbalanced_parens(self):
        with self.assertRaises(ValueError):
            parse_area_envelope("POLYGON ((-25 63, -13 63, -25 67, -25 63)")

    def test_rejects_too_few_vertices(self):
        with self.assertRaises(ValueError):
            parse_area_envelope("POLYGON ((-25 63, -13 63, -25 67))")

    def test_rejects_out_of_range_coordinates(self):
        with self.assertRaises(ValueError):
            parse_area_envelope("POLYGON ((0 0, 200 0, 200 1, 0 0))")

    def test_rejects_sql_breakout_characters(self):
        with self.assertRaises(ValueError):
            parse_area_envelope("POLYGON ((0 0, 1 0, 1 1, 0 0))'); DROP TABLE x;--")

    def test_rejects_non_polygon_wkt_as_bbox(self):
        with self.assertRaises(ValueError):
            parse_area_envelope("POINT (1 1)")

    def test_validate_area_accepts_empty(self):
        validate_area("")

    def test_validate_area_rejects_malformed(self):
        with self.assertRaises(ValueError):
            validate_area("not-a-bbox")


class TestAreaSql(unittest.TestCase):
    def test_bbox_polygon_wkt(self):
        self.assertEqual(
            bbox_polygon_wkt(ICELAND),
            "POLYGON((-25.0 63.0, -13.0 63.0, -13.0 67.0, -25.0 67.0, -25.0 63.0))",
        )

    def test_area_polygon_wkt_passes_wkt_through(self):
        self.assertEqual(area_polygon_wkt(f"  {ICELAND_TRIANGLE} "), ICELAND_TRIANGLE)

    def test_area_polygon_wkt_converts_bbox(self):
        self.assertEqual(area_polygon_wkt(ICELAND), bbox_polygon_wkt(ICELAND))

    def test_intersects_sql_keeps_null(self):
        sql = area_intersects_sql(ICELAND, "ST_GeomFromWKB(geometry)")
        self.assertIn("ST_GeomFromWKB(geometry) IS NULL", sql)
        self.assertIn("ST_Intersects", sql)

    def test_point_predicate_bbox_is_range_only(self):
        sql = area_point_predicate_sql(ICELAND)
        self.assertEqual(
            sql, "lon BETWEEN -25.0 AND -13.0 AND lat BETWEEN 63.0 AND 67.0"
        )

    def test_point_predicate_wkt_adds_exact_test(self):
        sql = area_point_predicate_sql(
            ICELAND_TRIANGLE, lon_expr="n.lon", lat_expr="n.lat"
        )
        self.assertTrue(
            sql.startswith(
                "n.lon BETWEEN -25.0 AND -13.0 AND n.lat BETWEEN 63.0 AND 67.0"
            )
        )
        self.assertIn("ST_Point(CAST(n.lon AS DOUBLE), CAST(n.lat AS DOUBLE))", sql)
        self.assertIn(f"ST_GeomFromText('{ICELAND_TRIANGLE}')", sql)

    def test_empty_area_noop_without_spark(self):
        sentinel = object()
        self.assertIs(filter_df_to_area(sentinel, ""), sentinel)


@pytest.mark.spark
@needs_spark
class TestFilterDfToArea(unittest.TestCase):
    def _run_filter_job(self, test_area):
        """Filter WKT point rows through a real Sedona job."""

        class JobTest(SparkSedonaJob):
            def execute_job(self):
                data = [
                    (1, "POINT (-20 65)"),  # inside Iceland bbox
                    (2, "POINT (30 10)"),  # outside
                    (3, None),  # NULL geometry is kept
                ]
                df = self.spark.createDataFrame(data, ["id", "wkt"]).selectExpr(
                    "id",
                    "CASE WHEN wkt IS NOT NULL "
                    "THEN ST_AsBinary(ST_GeomFromText(wkt)) END AS geometry",
                )
                filtered = self.apply_test_area_filter(df)
                self.log_data("ids", sorted(r.id for r in filtered.collect()))

        return JobTest().run(params={"test_area": test_area})

    def test_filters_to_bbox_keeping_null_geometry(self):
        result = self._run_filter_job(ICELAND)
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertEqual(result.data["ids"], [1, 3])

    def test_filters_to_wkt_keeping_null_geometry(self):
        result = self._run_filter_job(ICELAND_MULTI)
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertEqual(result.data["ids"], [1, 3])

    def test_empty_area_is_noop(self):
        result = self._run_filter_job("")
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertEqual(result.data["ids"], [1, 2, 3])


@pytest.mark.spark
@needs_spark
class TestPointPredicateInSpark(unittest.TestCase):
    def test_wkt_predicate_is_exact_not_envelope(self):
        class JobTest(SparkSedonaJob):
            def execute_job(self):
                data = [
                    (1, -20.0, 64.0),  # inside the triangle
                    (2, -14.0, 66.5),  # inside the envelope, outside the triangle
                    (3, 30.0, 10.0),  # outside both
                ]
                self.spark.createDataFrame(
                    data, ["id", "lon", "lat"]
                ).createOrReplaceTempView("pts")
                rows = self.spark.sql(
                    f"SELECT id FROM pts WHERE {area_point_predicate_sql(ICELAND_TRIANGLE)}"
                ).collect()
                self.log_data("ids", sorted(r.id for r in rows))

        result = JobTest().run(params={})
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertEqual(result.data["ids"], [1])


@pytest.mark.spark
@needs_spark
class TestGetTestAreaParam(unittest.TestCase):
    def test_invalid_area_fails_job(self):
        class JobTest(SparkSedonaJob):
            def execute_job(self):
                self.get_test_area_param()

        result = JobTest().run(params={"test_area": "not-a-bbox"})
        self.assertFalse(result.isSuccess)
        self.assertIsInstance(result.exception, ValueError)

    def test_wkt_area_is_accepted(self):
        class JobTest(SparkSedonaJob):
            def execute_job(self):
                self.log_data("area", self.get_test_area_param())

        result = JobTest().run(params={"test_area": ICELAND_TRIANGLE})
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertEqual(result.data["area"], ICELAND_TRIANGLE)


if __name__ == "__main__":
    unittest.main()
