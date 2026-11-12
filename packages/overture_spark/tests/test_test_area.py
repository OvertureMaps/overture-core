import unittest
from unittest.mock import Mock

import pytest

from overture_spark.test_area import (
    area_intersects_sql,
    area_point_predicate_sql,
    filter_df_to_area,
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

# ST_Intersects raises a TopologyException on this polygon with THIN_TRIANGLE.
INVALID_POLYGON = "POLYGON ((5 1, 5 3, 5 1, 2 1, 3 3, 5 1))"
THIN_TRIANGLE = "POLYGON ((6 2, 2 -2, 5 2, 6 2))"
# THIN_TRIANGLE plus a part that is far from every test row.
THIN_TRIANGLE_MULTI = (
    "MULTIPOLYGON (((6 2, 2 -2, 5 2, 6 2)), ((60 60, 61 60, 61 61, 60 60)))"
)
THIN_TRIANGLE_BBOX = "0,-2,6,3"
# Self-intersecting ring: not a valid area.
BOWTIE = "POLYGON ((0 0, 4 4, 4 0, 0 4, 0 0))"


class TestAreaSql(unittest.TestCase):
    def test_intersects_sql_keeps_null(self):
        sql = area_intersects_sql(ICELAND, "ST_GeomFromWKB(geometry)")
        self.assertIn("ST_GeomFromWKB(geometry) IS NULL", sql)
        self.assertIn("ST_Intersects", sql)

    def test_intersects_sql_checks_validity_only_for_wkt_area(self):
        geometry = "ST_GeomFromWKB(geometry)"
        self.assertNotIn("ST_IsValid", area_intersects_sql(ICELAND, geometry))
        self.assertIn("ST_IsValid", area_intersects_sql(ICELAND_TRIANGLE, geometry))

    def test_invalid_area_is_rejected_before_sql_is_built(self):
        with self.assertRaises(ValueError):
            area_intersects_sql(BOWTIE, "geometry")
        with self.assertRaises(ValueError):
            area_point_predicate_sql(BOWTIE)
        df = Mock()
        with self.assertRaises(ValueError):
            filter_df_to_area(df, BOWTIE)
        df.where.assert_not_called()

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
class TestInvalidGeometry(unittest.TestCase):
    ENTRY_POINTS = ("job_wkb", "df_geometry", "sql_wkb", "sql_geometry")

    def _filtered_ids(self, test_area):
        """Ids kept by each filter entry point, for WKB and geometry columns."""

        class JobTest(SparkSedonaJob):
            def execute_job(self):
                data = [
                    (1, INVALID_POLYGON),
                    # INVALID_POLYGON moved 50 degrees east
                    (2, "POLYGON ((55 1, 55 3, 55 1, 52 1, 53 3, 55 1))"),
                    (3, "POLYGON ((4 0, 5 0, 5 2, 4 0))"),  # valid, in the triangle
                    (4, "LINESTRING (4 1, 6 1)"),  # valid, crosses the triangle
                    # valid, misses the triangle but its envelope does not
                    (5, "LINESTRING (2 0, 5 3)"),
                    (6, None),
                    (7, "POLYGON EMPTY"),
                    # invalid; its envelope is a line that crosses the triangle
                    (8, "POLYGON ((3 1, 4 1, 5 1, 3 1))"),
                ]
                geom_df = self.spark.createDataFrame(data, ["id", "wkt"]).selectExpr(
                    "id", "ST_GeomFromText(wkt) AS geometry"
                )
                wkb_df = geom_df.selectExpr("id", "ST_AsBinary(geometry) AS geometry")
                filtered = {
                    "job_wkb": self.apply_test_area_filter(wkb_df),
                    "df_geometry": filter_df_to_area(geom_df, test_area, is_wkb=False),
                    "sql_wkb": wkb_df.where(
                        area_intersects_sql(test_area, "ST_GeomFromWKB(geometry)")
                    ),
                    "sql_geometry": geom_df.where(
                        area_intersects_sql(test_area, "geometry")
                    ),
                }
                for name, df in filtered.items():
                    self.log_data(name, sorted(r.id for r in df.collect()))

        result = JobTest().run(params={"test_area": test_area})
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        return result.data

    def test_wkt_area_keeps_invalid_geometry_by_envelope(self):
        for area in (THIN_TRIANGLE, THIN_TRIANGLE_MULTI):
            with self.subTest(area=area):
                self.assertEqual(
                    self._filtered_ids(area),
                    dict.fromkeys(self.ENTRY_POINTS, [1, 3, 4, 6, 8]),
                )

    def test_bbox_area_does_not_raise_on_invalid_geometry(self):
        self.assertEqual(
            self._filtered_ids(THIN_TRIANGLE_BBOX),
            dict.fromkeys(self.ENTRY_POINTS, [1, 3, 4, 5, 6, 8]),
        )


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
