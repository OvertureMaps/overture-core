import datetime
import unittest

import pytest

from overture_spark.job import SparkSedonaJob
from overture_spark.osm_history import (
    HISTORY_TAG_PREFIX,
    OSM_SNAPSHOT_COLUMNS,
    history_tag_name,
    osm_snapshot_sql,
    read_job_osm_snapshot,
    read_osm_snapshot,
)

TABLE = "s3tables_catalog.osm.planet_history"


class _FakeReader:
    def __init__(self):
        self.paths = []

    def parquet(self, path):
        self.paths.append(path)
        return f"parquet:{path}"


class _FakeSpark:
    def __init__(self):
        self.read = _FakeReader()
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        return "sql-df"


class TestTagContract(unittest.TestCase):
    def test_history_tag_name(self):
        self.assertEqual(HISTORY_TAG_PREFIX, "ds-")
        self.assertEqual(history_tag_name("2025-06-15"), "ds-2025-06-15")

    def test_history_tag_name_rejects_bad_ds(self):
        for bad in ("20250615", "2025-6-15", "2025-06-15T00", "not-a-date"):
            with self.assertRaises(ValueError):
                history_tag_name(bad)


class TestOsmSnapshotSql(unittest.TestCase):
    def test_reads_tag_and_latest_visible_rows(self):
        sql = osm_snapshot_sql(TABLE, "2025-06-15")
        self.assertIn(f"FROM {TABLE} VERSION AS OF 'ds-2025-06-15'", sql)
        self.assertIn("WHERE is_latest AND visible", sql)

    def test_node_geometry_from_lat_lon(self):
        sql = osm_snapshot_sql(TABLE, "2025-06-15")
        self.assertIn("WHEN type = 'node'", sql)
        self.assertIn("ST_ReducePrecision(ST_Point(lon, lat), 7)", sql)
        self.assertIn("ELSE geometry", sql)

    def test_projects_geometry_daily_columns(self):
        sql = osm_snapshot_sql(TABLE, "2025-06-15")
        for column in OSM_SNAPSHOT_COLUMNS:
            self.assertRegex(sql, rf"\b{column}\b")
        self.assertEqual(OSM_SNAPSHOT_COLUMNS[-1], "type")


class TestReadOsmSnapshot(unittest.TestCase):
    def test_history_table_runs_osm_snapshot_sql(self):
        spark = _FakeSpark()
        df = read_osm_snapshot(spark, history_table=TABLE, ds="2025-06-15")
        self.assertEqual(df, "sql-df")
        self.assertEqual(spark.queries, [osm_snapshot_sql(TABLE, "2025-06-15")])
        self.assertEqual(spark.read.paths, [])

    def test_input_path_reads_parquet_without_trailing_slash(self):
        spark = _FakeSpark()
        df = read_osm_snapshot(spark, input_path="s3a://bucket/geometry_daily/")
        self.assertEqual(df, "parquet:s3a://bucket/geometry_daily")
        self.assertEqual(spark.queries, [])

    def test_history_table_requires_ds(self):
        with self.assertRaises(ValueError):
            read_osm_snapshot(_FakeSpark(), history_table=TABLE)

    def test_rejects_both_sources(self):
        with self.assertRaises(ValueError):
            read_osm_snapshot(
                _FakeSpark(),
                input_path="s3a://bucket/geometry_daily",
                history_table=TABLE,
                ds="2025-06-15",
            )

    def test_rejects_no_source(self):
        with self.assertRaises(ValueError):
            read_osm_snapshot(_FakeSpark())


class _FakeJob:
    def __init__(self, params):
        self.spark = _FakeSpark()
        self.params = params

    def get_param(self, name, default_value=None, is_required=True):
        value = self.params.get(name, default_value)
        if is_required and not value:
            raise KeyError(name)
        return default_value if not value else value


class TestReadJobOsmSnapshot(unittest.TestCase):
    def test_history_params_read_tagged_snapshot(self):
        job = _FakeJob({"history_table": TABLE, "snapshot_ds": "2025-06-15"})
        self.assertEqual(read_job_osm_snapshot(job), "sql-df")
        self.assertEqual(job.spark.queries, [osm_snapshot_sql(TABLE, "2025-06-15")])

    def test_path_param_name_is_configurable(self):
        job = _FakeJob({"osm_path": "s3a://bucket/geometry_daily/"})
        df = read_job_osm_snapshot(job, path_param="osm_path")
        self.assertEqual(df, "parquet:s3a://bucket/geometry_daily")
        self.assertEqual(job.spark.queries, [])

    def test_default_path_param_is_input_path(self):
        job = _FakeJob({"input_path": "s3a://bucket/geometry_daily"})
        self.assertEqual(
            read_job_osm_snapshot(job), "parquet:s3a://bucket/geometry_daily"
        )

    def test_history_table_without_snapshot_ds_fails(self):
        with self.assertRaises(ValueError):
            read_job_osm_snapshot(_FakeJob({"history_table": TABLE}))

    def test_no_source_fails(self):
        with self.assertRaises(ValueError):
            read_job_osm_snapshot(_FakeJob({}))

    def test_ds_param_name_is_configurable(self):
        job = _FakeJob(
            {
                "history_table": TABLE,
                "reset_snapshot_ds": "2025-06-08",
                "today_snapshot_ds": "2025-06-15",
            }
        )
        read_job_osm_snapshot(
            job, path_param="reset_path", ds_param="reset_snapshot_ds"
        )
        read_job_osm_snapshot(
            job, path_param="today_path", ds_param="today_snapshot_ds"
        )
        self.assertEqual(
            job.spark.queries,
            [
                osm_snapshot_sql(TABLE, "2025-06-08"),
                osm_snapshot_sql(TABLE, "2025-06-15"),
            ],
        )

    def test_ds_param_falls_back_to_path_per_snapshot(self):
        job = _FakeJob(
            {
                "reset_path": "s3a://bucket/geometry_daily/version=2025-06-09",
                "today_path": "s3a://bucket/geometry_daily/version=2025-06-16",
            }
        )
        self.assertEqual(
            read_job_osm_snapshot(
                job, path_param="reset_path", ds_param="reset_snapshot_ds"
            ),
            "parquet:s3a://bucket/geometry_daily/version=2025-06-09",
        )
        self.assertEqual(
            read_job_osm_snapshot(
                job, path_param="today_path", ds_param="today_snapshot_ds"
            ),
            "parquet:s3a://bucket/geometry_daily/version=2025-06-16",
        )
        self.assertEqual(job.spark.queries, [])


HISTORY_VIEW_SCHEMA = (
    "id BIGINT, type STRING, version INT, visible BOOLEAN, `timestamp` TIMESTAMP, "
    "changeset BIGINT, uid BIGINT, `user` STRING, tags MAP<STRING, STRING>, "
    "lat DOUBLE, lon DOUBLE, refs ARRAY<BIGINT>, "
    "members ARRAY<STRUCT<type: STRING, ref: BIGINT, role: STRING>>, "
    "is_latest BOOLEAN, is_last_visible BOOLEAN, latest_ts TIMESTAMP, "
    "geometry BINARY"
)
TS = datetime.datetime(2025, 6, 15, 12, 0, 0)


def _row(id, type, version, visible, is_latest, is_last_visible, **kw):
    return {
        "id": id,
        "type": type,
        "version": version,
        "visible": visible,
        "timestamp": TS,
        "changeset": 1,
        "uid": 7,
        "user": "mapper",
        "tags": kw.get("tags", {}),
        "lat": kw.get("lat"),
        "lon": kw.get("lon"),
        "refs": kw.get("refs"),
        "members": None,
        "is_latest": is_latest,
        "is_last_visible": is_last_visible,
        "latest_ts": TS,
        "geometry": kw.get("geometry"),
    }


@pytest.mark.spark
class TestOsmSnapshotSqlOnSpark(unittest.TestCase):
    """Run the snapshot SELECT against a history-shaped view in real Spark.

    Temp views cannot be tagged, so the VERSION AS OF clause is stripped; the
    tag itself is covered by the string tests above and the writer's tests.
    """

    def _run(self, ansi: bool, test_area: str = ""):
        ansi_confs = (
            "spark.sql.ansi.enabled",
            "spark.sql.ansi.enforceReservedKeywords",
        )

        class JobTest(SparkSedonaJob):
            def execute_job(self):
                # The SparkSession is shared across tests; restore confs on exit.
                saved = {k: self.spark.conf.get(k, None) for k in ansi_confs}
                try:
                    for k in ansi_confs:
                        self.spark.conf.set(k, str(ansi).lower())
                    self._run_snapshot()
                finally:
                    for k, v in saved.items():
                        if v is None:
                            self.spark.conf.unset(k)
                        else:
                            self.spark.conf.set(k, v)

            def _run_snapshot(self):
                import pyspark.sql.functions as F

                rows = [
                    _row(1, "node", 2, True, True, True, lat=65.0, lon=-20.0),
                    _row(1, "node", 1, True, False, False, lat=64.0, lon=-21.0),
                    _row(2, "node", 2, False, True, False, lat=65.0, lon=-20.0),
                    _row(2, "node", 1, True, False, True, lat=65.0, lon=-20.0),
                    _row(3, "node", 1, True, True, True, lat=10.0, lon=30.0),
                    _row(10, "way", 1, True, True, True, refs=[1, 3]),
                    _row(11, "way", 2, False, True, False, refs=[1, 3]),
                    _row(11, "way", 1, True, False, True, refs=[1, 3]),
                ]
                history = self.spark.createDataFrame(rows, HISTORY_VIEW_SCHEMA)
                # Way geometry lives on last-visible rows only (as the writer
                # materializes it); nodes carry lat/lon.
                history = history.withColumn(
                    "geometry",
                    F.expr(
                        "CASE WHEN type = 'way' AND is_last_visible "
                        "THEN ST_AsBinary(ST_GeomFromText('LINESTRING (-20 65, 30 10)')) END"
                    ),
                )
                history.createOrReplaceTempView("history_with_geom")
                sql = osm_snapshot_sql("history_with_geom", "2025-06-15").replace(
                    "VERSION AS OF 'ds-2025-06-15'", ""
                )
                snapshot = self.spark.sql(sql)
                self.log_data("columns", snapshot.columns)
                if test_area:
                    snapshot = self.apply_test_area_filter(snapshot)
                out = snapshot.selectExpr(
                    "type",
                    "id",
                    "version",
                    "`user`",
                    "ST_AsText(ST_GeomFromWKB(geometry)) AS wkt",
                ).collect()
                self.log_data(
                    "rows",
                    sorted((r.type, r.id, r.version, r.user, r.wkt) for r in out),
                )

        return JobTest().run(params={"test_area": test_area})

    def test_snapshot_with_node_geometry_from_lat_lon(self):
        result = self._run(ansi=False)
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertEqual(result.data["columns"], OSM_SNAPSHOT_COLUMNS)
        self.assertEqual(
            result.data["rows"],
            [
                ("node", 1, 2, "mapper", "POINT (-20 65)"),
                ("node", 3, 1, "mapper", "POINT (30 10)"),
                ("way", 10, 1, "mapper", "LINESTRING (-20 65, 30 10)"),
            ],
        )

    def test_user_column_survives_ansi_mode(self):
        # With reserved keywords enforced, an unquoted `user` parses as current_user().
        result = self._run(ansi=True)
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertEqual({r[3] for r in result.data["rows"]}, {"mapper"})

    def test_test_area_filter_applies_to_materialized_node_geometry(self):
        result = self._run(ansi=False, test_area="-25,63,-13,67")
        self.assertTrue(result.isSuccess, msg="\n".join(result.exception_traceback))
        self.assertEqual(
            [(r[0], r[1]) for r in result.data["rows"]],
            [("node", 1), ("way", 10)],
        )


if __name__ == "__main__":
    unittest.main()
