"""Optional geographic scoping ("test area") for dev/testing job runs.

Jobs accept an optional ``test_area`` param — a single geometry string that
is either a bbox ``"min_lon,min_lat,max_lon,max_lat"`` (e.g. Iceland:
``-25,63,-13,67``) or a ``POLYGON``/``MULTIPOLYGON`` WKT; empty = full
planet. These helpers validate the param and filter a DataFrame to
geometries intersecting the area. Rows with NULL geometry are kept: several
checks specifically target entities whose geometry could not be built, and
dropping them would hide those violations.
"""

import re
from typing import Tuple

Envelope = Tuple[float, float, float, float]

_WKT_PREFIXES = ("POLYGON", "MULTIPOLYGON")
_NUMBER = r"[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?"
_COORD_PAIR = re.compile(rf"({_NUMBER})\s+({_NUMBER})")
# Restrict WKT to its grammar's alphabet so it can be safely inlined in SQL.
_WKT_CHARS = re.compile(r"[A-Za-z0-9\s(),.+\-]+")


def is_wkt_area(area: str) -> bool:
    """True when the area string is a (MULTI)POLYGON WKT rather than a bbox."""
    return bool(area) and area.strip().upper().startswith(_WKT_PREFIXES)


def _check_lon_lat_ranges(min_lon, min_lat, max_lon, max_lat, area: str) -> None:
    if not (-180.0 <= min_lon <= 180.0 and -180.0 <= max_lon <= 180.0):
        raise ValueError(f"test_area longitudes must be in [-180, 180], got: {area!r}")
    if not (-90.0 <= min_lat <= 90.0 and -90.0 <= max_lat <= 90.0):
        raise ValueError(f"test_area latitudes must be in [-90, 90], got: {area!r}")


def parse_bbox(bbox: str) -> Envelope:
    """Parse and validate a 'min_lon,min_lat,max_lon,max_lat' string."""
    parts = bbox.split(",")
    if len(parts) != 4:
        raise ValueError(
            f"bbox must be 'min_lon,min_lat,max_lon,max_lat', got: {bbox!r}"
        )
    try:
        min_lon, min_lat, max_lon, max_lat = (float(p) for p in parts)
    except ValueError as exc:
        raise ValueError(f"bbox values must be numbers, got: {bbox!r}") from exc
    _check_lon_lat_ranges(min_lon, min_lat, max_lon, max_lat, bbox)
    if min_lon >= max_lon or min_lat >= max_lat:
        raise ValueError(f"bbox min must be < max on both axes, got: {bbox!r}")
    return min_lon, min_lat, max_lon, max_lat


def _parse_wkt_envelope(wkt: str) -> Envelope:
    """Envelope of a (MULTI)POLYGON WKT from its coordinate pairs.

    Shape-level validation only (prefix, balanced parens, >= 4 vertices,
    lon/lat ranges); Sedona's ST_GeomFromText is the authoritative parser
    when the area is applied.
    """
    body = wkt.strip()
    if not _WKT_CHARS.fullmatch(body):
        raise ValueError(f"test_area WKT contains unexpected characters: {wkt!r}")
    if body.count("(") == 0 or body.count("(") != body.count(")"):
        raise ValueError(f"test_area WKT has unbalanced parentheses: {wkt!r}")
    pairs = _COORD_PAIR.findall(body)
    if len(pairs) < 4:
        raise ValueError(f"test_area WKT needs at least 4 vertices, got: {wkt!r}")
    lons = [float(lon) for lon, _ in pairs]
    lats = [float(lat) for _, lat in pairs]
    envelope = (min(lons), min(lats), max(lons), max(lats))
    _check_lon_lat_ranges(*envelope, wkt)
    return envelope


def parse_area_envelope(area: str) -> Envelope:
    """Validate a test_area (bbox or WKT) and return its lon/lat envelope."""
    if is_wkt_area(area):
        return _parse_wkt_envelope(area)
    return parse_bbox(area)


def validate_area(area: str) -> None:
    """Raise ValueError when a non-empty test_area is malformed."""
    if area:
        parse_area_envelope(area)


def bbox_polygon_wkt(bbox: str) -> str:
    """Polygon WKT covering the bbox (counter-clockwise ring)."""
    min_lon, min_lat, max_lon, max_lat = parse_bbox(bbox)
    return (
        f"POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, "
        f"{max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"
    )


def area_polygon_wkt(area: str) -> str:
    """WKT for the test_area: the WKT itself, or a polygon covering the bbox."""
    if is_wkt_area(area):
        _parse_wkt_envelope(area)
        return area.strip()
    return bbox_polygon_wkt(area)


def area_intersects_sql(area: str, geometry_expr: str) -> str:
    """SQL predicate: geometry_expr intersects the area (NULL geometry passes).

    geometry_expr must evaluate to a Sedona geometry (e.g.
    ``ST_GeomFromWKB(geometry)``).
    """
    wkt = area_polygon_wkt(area)
    return (
        f"({geometry_expr} IS NULL "
        f"OR ST_Intersects({geometry_expr}, ST_GeomFromText('{wkt}')))"
    )


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

    Rows with NULL geometry are kept (see module docstring).
    """
    if not area:
        return df
    geometry_expr = f"ST_GeomFromWKB({geometry_col})" if is_wkb else geometry_col
    return df.where(
        f"{geometry_col} IS NULL "
        f"OR ST_Intersects({geometry_expr}, "
        f"ST_GeomFromText('{area_polygon_wkt(area)}'))"
    )
