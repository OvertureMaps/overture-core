import re
import struct
import unittest

import pytest

from overture_spark.bbox import BBOX_FIELDS, bbox_from_geometry_sql

try:
    import pyspark  # noqa: F401

    from overture_spark.job import SparkSedonaJob
except ImportError:
    pyspark = None
    SparkSedonaJob = None

needs_spark = pytest.mark.skipif(
    pyspark is None, reason="pyspark not installed; requires the sql-spark extra"
)

GEOMETRIES = {
    # A plain cast to float32 moves every bound of these two lines inward:
    # the minimums up and the maximums down. One line per sign, because the
    # rounding takes a different branch for each.
    "all_positive": (
        "LINESTRING (9.5597723861 47.5078412731, 120.7929768725 60.8096445344)"
    ),
    "all_negative": (
        "LINESTRING (-120.0352422189 -60.3460774665, -9.3844372954 -47.8425059049)"
    ),
    "near_zero": (
        "LINESTRING (-0.0000000001234 0.0000000001234, 0.0000000004321 0.0000000009876)"
    ),
    "antimeridian": (
        "LINESTRING (-179.99999999 -89.99999999, 179.99999999 89.99999999)"
    ),
    # Just inside +-128, where rounding outward lands in the next power of
    # two and float32 values are twice as far apart.
    "power_of_two_edge": (
        "LINESTRING (-127.99999420924175 -127.99999420924175, "
        "127.99999420924175 127.99999420924175)"
    ),
    # Below the smallest normal float32, where a plain cast moves every bound
    # inward and the two smallest ones become zero.
    "subnormal": "LINESTRING (-1e-40 -1e-46, 1e-46 1e-40)",
    "exact_in_float32": "LINESTRING (-180 -1.5, 0 0.25)",
    "point": "POINT (-122.4194155 37.7749295)",
    "null": None,
}


def _as_float32(value: float) -> float:
    """``value`` after a plain cast to float32."""
    return struct.unpack("<f", struct.pack("<f", value))[0]


def _next_float32_up(value: float) -> float:
    """The float32 after ``value``, which must itself be a float32."""
    if value == 0:
        return 2.0**-149
    bits = struct.unpack("<I", struct.pack("<f", value))[0]
    return struct.unpack("<f", struct.pack("<I", bits + (1 if value > 0 else -1)))[0]


def _next_float32_down(value: float) -> float:
    return -_next_float32_up(-value)


def _nearest_float32_at_or_below(value: float) -> float:
    cast = _as_float32(value)
    return cast if cast <= value else _next_float32_down(cast)


def _nearest_float32_at_or_above(value: float) -> float:
    cast = _as_float32(value)
    return cast if cast >= value else _next_float32_up(cast)


class TestBboxSql(unittest.TestCase):
    def test_fields_follow_geoparquet_order(self):
        self.assertEqual(BBOX_FIELDS, ("xmin", "ymin", "xmax", "ymax"))
        names = re.findall(r"'(\w+)', \(CASE", bbox_from_geometry_sql())
        self.assertEqual(tuple(names), BBOX_FIELDS)

    def test_each_field_reads_its_own_bound(self):
        sql = bbox_from_geometry_sql("geom")
        for name, function in zip(BBOX_FIELDS, ("XMin", "YMin", "XMax", "YMax")):
            field_sql = sql.split(f"'{name}', ")[1].split(" END)")[0]
            self.assertIn(f"ST_{function}(geom)", field_sql)
            for other in {"XMin", "YMin", "XMax", "YMax"} - {function}:
                self.assertNotIn(f"ST_{other}(", field_sql)

    def test_defaults_to_the_wkb_geometry_column(self):
        self.assertIn("ST_XMin(ST_GeomFromWKB(geometry))", bbox_from_geometry_sql())


@pytest.mark.spark
@needs_spark
class TestBboxInSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class JobTest(SparkSedonaJob):
            def execute_job(self):
                df = self.spark.createDataFrame(
                    list(GEOMETRIES.items()), "name string, wkt string"
                ).selectExpr("name", "ST_GeomFromText(wkt) AS geom")
                bbox = df.selectExpr(
                    "name",
                    f"{bbox_from_geometry_sql('geom')} AS bbox",
                    "ST_XMin(geom) AS xmin",
                    "ST_YMin(geom) AS ymin",
                    "ST_XMax(geom) AS xmax",
                    "ST_YMax(geom) AS ymax",
                )
                self.log_data("bbox_type", bbox.schema["bbox"].dataType.simpleString())
                self.log_data(
                    "rows",
                    {
                        r.name: {
                            "bbox": None if r.bbox is None else r.bbox.asDict(),
                            "exact": {f: r[f] for f in BBOX_FIELDS},
                        }
                        for r in bbox.collect()
                    },
                )

                # The default argument reads a WKB column named `geometry`.
                same = (
                    df.where("geom IS NOT NULL")
                    .selectExpr("geom", "ST_AsBinary(geom) AS geometry")
                    .selectExpr(
                        f"{bbox_from_geometry_sql()} = "
                        f"{bbox_from_geometry_sql('geom')} AS same"
                    )
                )
                self.log_data("default_matches", [r.same for r in same.collect()])

        cls.result = JobTest().run(params={})

    def setUp(self):
        self.assertTrue(
            self.result.isSuccess, msg="\n".join(self.result.exception_traceback)
        )
        self.rows = self.result.data["rows"]

    def test_struct_is_float_in_geoparquet_order(self):
        self.assertEqual(
            self.result.data["bbox_type"],
            "struct<xmin:float,ymin:float,xmax:float,ymax:float>",
        )

    def test_bbox_contains_the_geometry_and_stays_tight(self):
        """Each bound is the nearest float32 on the outer side, or the next one."""
        for name in GEOMETRIES.keys() - {"null"}:
            bbox, exact = self.rows[name]["bbox"], self.rows[name]["exact"]
            for field in ("xmin", "ymin"):
                with self.subTest(name=name, field=field):
                    nearest = _nearest_float32_at_or_below(exact[field])
                    self.assertLessEqual(bbox[field], exact[field])
                    self.assertIn(bbox[field], (nearest, _next_float32_down(nearest)))
            for field in ("xmax", "ymax"):
                with self.subTest(name=name, field=field):
                    nearest = _nearest_float32_at_or_above(exact[field])
                    self.assertGreaterEqual(bbox[field], exact[field])
                    self.assertIn(bbox[field], (nearest, _next_float32_up(nearest)))

    def test_fixtures_cover_every_rounding_branch(self):
        """Guards the fixtures: a plain cast moves each of these bounds inward."""
        for name, sign in (("all_positive", 1), ("all_negative", -1)):
            for value in self.rows[name]["exact"].values():
                self.assertGreater(value * sign, 0)
        for name in ("all_positive", "all_negative", "subnormal"):
            exact = self.rows[name]["exact"]
            for field in ("xmin", "ymin"):
                with self.subTest(name=name, field=field):
                    self.assertGreater(_as_float32(exact[field]), exact[field])
            for field in ("xmax", "ymax"):
                with self.subTest(name=name, field=field):
                    self.assertLess(_as_float32(exact[field]), exact[field])
        self.assertEqual(_as_float32(self.rows["subnormal"]["exact"]["ymin"]), 0)
        self.assertEqual(_as_float32(self.rows["subnormal"]["exact"]["xmax"]), 0)

    def test_values_float32_holds_exactly_are_unchanged(self):
        self.assertEqual(
            self.rows["exact_in_float32"]["bbox"],
            {"xmin": -180.0, "ymin": -1.5, "xmax": 0.0, "ymax": 0.25},
        )

    def test_null_geometry_gives_a_struct_of_nulls(self):
        self.assertEqual(self.rows["null"]["bbox"], dict.fromkeys(BBOX_FIELDS))

    def test_default_reads_the_wkb_geometry_column(self):
        self.assertEqual(
            self.result.data["default_matches"], [True] * (len(GEOMETRIES) - 1)
        )
