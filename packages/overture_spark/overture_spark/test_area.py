"""Optional geographic scoping ("test area") for dev/testing job runs.

Jobs accept an optional ``test_area`` param — a single geometry string that
is either a bbox ``"min_lon,min_lat,max_lon,max_lat"`` (e.g. Iceland:
``-25,63,-13,67``) or a ``POLYGON``/``MULTIPOLYGON`` WKT; empty = full
planet. ``overture_core.test_area`` owns that contract (parsing and
validation), shared with the other entry points that accept a test area; its
helpers are re-exported here. This module adds the Spark SQL that filters a
DataFrame to geometries intersecting the area. Rows with NULL geometry are
kept: several checks specifically target entities whose geometry could not
be built, and dropping them would hide those violations.

An invalid geometry (``ST_IsValid`` is false) does not fail the filter. With
a WKT area it is kept when its envelope intersects the area, because
``ST_Intersects`` on the geometry itself can raise a JTS
``TopologyException``. Checks that look for broken geometries still get the
ones near the area, and a few just outside it may be kept too. With a bbox
it is tested like a valid geometry: JTS tests a rectangle without building
the topology that raises. An invalid area never reaches the SQL:
``overture_core.test_area`` rejects it with a ``ValueError``.
"""

from overture_core.test_area import (
    Envelope,
    area_polygon_wkt,
    bbox_polygon_wkt,
    is_wkt_area,
    parse_area_envelope,
    parse_bbox,
    validate_area,
)

__all__ = [
    "Envelope",
    "area_intersects_sql",
    "area_point_predicate_sql",
    "area_polygon_wkt",
    "bbox_polygon_wkt",
    "filter_df_to_area",
    "is_wkt_area",
    "parse_area_envelope",
    "parse_bbox",
    "validate_area",
]


def _intersects_area_sql(area: str, geometry_expr: str) -> str:
    """SQL test of a non-NULL geometry against the area.

    A WKT area needs CASE WHEN, whose branches run in order (AND/OR operands
    may not): ST_Intersects only sees a valid geometry, and ST_IsValid only
    runs on rows whose envelope intersects the area.
    """
    area_expr = f"ST_GeomFromText('{area_polygon_wkt(area)}')"
    intersects = f"ST_Intersects({geometry_expr}, {area_expr})"
    if not is_wkt_area(area):
        return intersects
    return (
        f"CASE WHEN NOT ST_Intersects(ST_Envelope({geometry_expr}), {area_expr}) "
        f"THEN FALSE "
        f"WHEN NOT ST_IsValid({geometry_expr}) THEN TRUE "
        f"ELSE {intersects} END"
    )


def area_intersects_sql(area: str, geometry_expr: str) -> str:
    """SQL predicate: geometry_expr intersects the area (NULL geometry passes).

    geometry_expr must evaluate to a Sedona geometry (e.g.
    ``ST_GeomFromWKB(geometry)``). Invalid geometries do not raise (see
    module docstring).
    """
    return f"({geometry_expr} IS NULL OR {_intersects_area_sql(area, geometry_expr)})"


def area_point_predicate_sql(
    area: str, lon_expr: str = "lon", lat_expr: str = "lat"
) -> str:
    """SQL predicate: the point (lon_expr, lat_expr) lies within the area.

    Always emits a lon/lat range test on the area's envelope (cheap,
    pushdown-friendly); for WKT areas it additionally tests the exact
    polygon with ST_Intersects, so the result is precise for both forms.
    """
    min_lon, min_lat, max_lon, max_lat = parse_area_envelope(area)
    predicate = (
        f"{lon_expr} BETWEEN {min_lon} AND {max_lon} "
        f"AND {lat_expr} BETWEEN {min_lat} AND {max_lat}"
    )
    if is_wkt_area(area):
        predicate += (
            f" AND ST_Intersects("
            f"ST_Point(CAST({lon_expr} AS DOUBLE), CAST({lat_expr} AS DOUBLE)), "
            f"ST_GeomFromText('{area_polygon_wkt(area)}'))"
        )
    return predicate


def filter_df_to_area(
    df, area: str, geometry_col: str = "geometry", is_wkb: bool = True
):
    """Keep rows whose geometry intersects the area; no-op when area is empty.

    Rows with NULL geometry are kept, and invalid geometries do not raise
    (see module docstring).
    """
    if not area:
        return df
    geometry_expr = f"ST_GeomFromWKB({geometry_col})" if is_wkb else geometry_col
    return df.where(
        f"{geometry_col} IS NULL OR {_intersects_area_sql(area, geometry_expr)}"
    )
