"""Read-side helpers for the OSM full-history Iceberg table.

The history table, written by the OSM history pipeline in
OvertureMaps/tf-data-platform (``omf.utilities.osm_history``), keeps every
version of every OSM entity. After each daily merge the table snapshot is
tagged ``ds-YYYY-MM-DD`` for the last applied OSC day, so consumers read a
frozen, named daily state with ``VERSION AS OF`` and never race a concurrent
merge. A missing tag fails fast (Iceberg: "Cannot find snapshot with
reference name"), which is the freshness contract.

An "OSM snapshot as of ``ds-<date>``" is the latest visible version of every
node/way/relation after applying the daily diffs through that day, i.e. the
rows with ``is_latest AND visible`` at that tag.

Date contract: the history DAG run for day D merges OSC day D-1 and tags the
result ``ds-{D-1}``, so a consumer running for day D reads
``VERSION AS OF 'ds-{D-1}'``.
"""

from datetime import datetime

HISTORY_TAG_PREFIX = "ds-"
HISTORY_TAG_RETAIN_DAYS = 30

# Columns of the OSM snapshot, in geometry_daily order minus bbox: the
# history table carries none, so consumers that emit one derive it from
# geometry. ``type`` is last, matching the hive partition column position of
# geometry_daily-shaped parquet.
OSM_SNAPSHOT_COLUMNS = [
    "id",
    "version",
    "timestamp",
    "uid",
    "user",
    "changeset",
    "tags",
    "lat",
    "lon",
    "refs",
    "members",
    "latest_ts",
    "geometry",
    "type",
]


def _validate_ds(ds: str) -> str:
    parsed = datetime.strptime(ds, "%Y-%m-%d")
    if parsed.strftime("%Y-%m-%d") != ds:
        raise ValueError("ds must use YYYY-MM-DD format")
    return ds


def history_tag_name(ds: str) -> str:
    """Tag name for the table state through OSC day ds (YYYY-MM-DD)."""
    return f"{HISTORY_TAG_PREFIX}{_validate_ds(ds)}"


def osm_snapshot_sql(history_table: str, ds: str) -> str:
    """SELECT of the OSM snapshot as of tag ds, in geometry_daily shape.

    The snapshot is the latest visible version of every entity
    (``is_latest AND visible``). Way/relation geometry is already
    materialized on those rows; node geometry is rebuilt from lat/lon at
    7-decimal precision. Filters on ``type`` applied by the caller push
    through to the Iceberg scan and prune the ``type`` partition.
    """
    tag = history_tag_name(ds)
    # `user` and `timestamp` are backticked: with ANSI reserved keywords
    # enforced (spark.sql.ansi.enabled + spark.sql.ansi.enforceReservedKeywords)
    # a bare `user` parses as the current_user() keyword function.
    return f"""
        SELECT
            id, version, `timestamp`, uid, `user`, changeset, tags, lat, lon,
            refs, members, latest_ts,
            CASE
                WHEN type = 'node'
                THEN ST_AsBinary(ST_ReducePrecision(ST_Point(lon, lat), 7))
                ELSE geometry
            END AS geometry,
            type
        FROM {history_table} VERSION AS OF '{tag}'
        WHERE is_latest AND visible
    """


def read_osm_snapshot(
    spark,
    input_path: str | None = None,
    history_table: str | None = None,
    ds: str | None = None,
):
    """Load the OSM snapshot as of a day as a DataFrame in geometry_daily shape.

    Exactly one source must be given:

    - ``history_table`` + ``ds``: the history Iceberg table frozen at tag
      ``ds-{ds}`` (production path).
    - ``input_path``: a geometry_daily-shaped parquet hive-partitioned by
      ``type`` with WKB ``geometry`` (unit-test fixtures and ad-hoc reads),
      returned as-is.

    The history path returns the columns in ``OSM_SNAPSHOT_COLUMNS`` order.
    """
    if history_table and input_path:
        raise ValueError("Pass either history_table or input_path, not both.")
    if history_table:
        if not ds:
            raise ValueError("ds is required when reading from history_table.")
        return spark.sql(osm_snapshot_sql(history_table, ds))
    if input_path:
        return spark.read.parquet(input_path.rstrip("/"))
    raise ValueError("One of history_table (with ds) or input_path is required.")


def read_job_osm_snapshot(
    job, path_param: str = "input_path", ds_param: str = "snapshot_ds"
):
    """Load a job's OSM input from its params via ``read_osm_snapshot``.

    Production passes ``history_table`` + the tag day under ``ds_param`` (the
    history table frozen at tag ``ds-{day}``); tests and ad-hoc runs pass the
    geometry_daily-shaped parquet path under ``path_param`` instead. Jobs that
    read more than one snapshot (e.g. sprint reset + today) call this once per
    snapshot with distinct ``path_param``/``ds_param`` pairs. The job must
    expose ``spark`` and ``get_param`` (``SparkSedonaJob``).
    """
    return read_osm_snapshot(
        job.spark,
        input_path=job.get_param(path_param, is_required=False),
        history_table=job.get_param("history_table", is_required=False),
        ds=job.get_param(ds_param, is_required=False),
    )
