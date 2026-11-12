"""Shared contract for an optional 'test_area' string parameter.

A test area scopes a dev or test run to a region instead of the full planet.
It is one geometry string: a bbox "min_lon,min_lat,max_lon,max_lat" (e.g.
Iceland: "-25,63,-13,67") or a POLYGON / MULTIPOLYGON WKT in lon/lat. The
empty string means full planet. This module is the one place that defines
that contract, for every entry point that accepts a test area (Airflow DAGs,
Spark jobs, CLIs).

A WKT area must be a valid, non-empty polygon: a spatial predicate such as
ST_Intersects can raise a JTS TopologyException on an invalid one. That check
needs shapely (the ``test-area`` extra), imported lazily so that a bbox needs
nothing.
"""

import re
from typing import Tuple

from overture_core.bbox import validate_bbox

Envelope = Tuple[float, float, float, float]

_WKT_TYPE = re.compile(r"^(MULTIPOLYGON|POLYGON)\s*\(", re.IGNORECASE)
# Restrict WKT to its grammar's alphabet so it can be safely inlined in SQL.
_WKT_CHARS = re.compile(r"[A-Za-z0-9\s(),.+\-]+")


def is_wkt_area(area: str) -> bool:
    """True when the area string is a (MULTI)POLYGON WKT rather than a bbox.

    Matches the type keyword followed by whitespace/`(`, not a bare string
    prefix: a malformed type like ``POLYGONX (...)`` must not pass here. It
    is then validated as a bbox, and fails.
    """
    return bool(area) and bool(_WKT_TYPE.match(area.strip()))


def parse_bbox(bbox: str) -> Envelope:
    """Parse and validate a 'min_lon,min_lat,max_lon,max_lat' string."""
    if not bbox:
        raise ValueError(
            f"bbox must be 'min_lon,min_lat,max_lon,max_lat', got: {bbox!r}"
        )
    validate_bbox(bbox)
    min_lon, min_lat, max_lon, max_lat = (float(p) for p in bbox.split(","))
    return min_lon, min_lat, max_lon, max_lat


def _parse_wkt_envelope(wkt: str) -> Envelope:
    """Validate a (MULTI)POLYGON WKT and return its envelope."""
    from shapely import wkt as shapely_wkt
    from shapely.errors import ShapelyError
    from shapely.validation import explain_validity

    body = wkt.strip()
    if not _WKT_CHARS.fullmatch(body):
        raise ValueError(f"test_area WKT contains unexpected characters: {wkt!r}")
    try:
        geom = shapely_wkt.loads(body)
    except ShapelyError as exc:
        raise ValueError(f"test_area WKT could not be parsed ({exc}): {wkt!r}") from exc
    if geom.is_empty:
        raise ValueError(f"test_area WKT is empty: {wkt!r}")
    if not geom.is_valid:
        raise ValueError(
            f"test_area WKT is not a valid polygon ({explain_validity(geom)}): {wkt!r}"
        )
    min_lon, min_lat, max_lon, max_lat = geom.bounds
    if not (-180.0 <= min_lon and max_lon <= 180.0):
        raise ValueError(f"test_area longitudes must be in [-180, 180], got: {wkt!r}")
    if not (-90.0 <= min_lat and max_lat <= 90.0):
        raise ValueError(f"test_area latitudes must be in [-90, 90], got: {wkt!r}")
    return min_lon, min_lat, max_lon, max_lat


def parse_area_envelope(area: str) -> Envelope:
    """Validate a test_area (bbox or WKT) and return its lon/lat envelope."""
    if is_wkt_area(area):
        return _parse_wkt_envelope(area)
    return parse_bbox(area)


def validate_area(area: str) -> None:
    """Raise ValueError when a non-empty test_area is outside the contract."""
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
