"""Shared validation helpers for an optional 'bbox' string parameter.

The bbox format is "min_lon,min_lat,max_lon,max_lat", with the empty
string meaning full planet. Consumers (Airflow DAGs, jobs, CLIs) share
these helpers so the same contract is enforced wherever a bbox enters
the system.
"""

# JSON-schema pattern for an optional 'bbox' param: empty (full planet)
# or exactly four comma-separated plain numbers. Intended for entry-point
# validation (e.g. Airflow trigger-time param schemas — including conf
# forwarded between DAGs) so SQL/WKT interpolation sites (see
# BBOX_WKT_JINJA) never see malformed or quote-bearing input.
# validate_bbox is the semantic gate (ranges, min < max).
BBOX_PARAM_PATTERN = (
    r"^(?:|-?[0-9]+(?:\.[0-9]+)?(?:,-?[0-9]+(?:\.[0-9]+)?){3})$(?![\s\S])"
)


def validate_bbox(bbox: str) -> None:
    """Semantic validation of an optional bbox param ('' = full planet).

    BBOX_PARAM_PATTERN only constrains the shape; this enforces the
    contract (lon/lat ranges, min < max). Call it for fail-fast
    validation before a bbox is used — e.g. before interpolating
    BBOX_WKT_JINJA into a query, which would otherwise execute with a
    contract-invalid box.
    """
    if not bbox:
        return
    parts = bbox.split(",")
    if len(parts) != 4:
        raise ValueError(
            f"bbox must be 'min_lon,min_lat,max_lon,max_lat', got: {bbox!r}"
        )
    try:
        min_lon, min_lat, max_lon, max_lat = (float(p) for p in parts)
    except ValueError as exc:
        raise ValueError(f"bbox values must be numbers, got: {bbox!r}") from exc
    if not (-180.0 <= min_lon <= 180.0 and -180.0 <= max_lon <= 180.0):
        raise ValueError(f"bbox longitudes must be in [-180, 180], got: {bbox!r}")
    if not (-90.0 <= min_lat <= 90.0 and -90.0 <= max_lat <= 90.0):
        raise ValueError(f"bbox latitudes must be in [-90, 90], got: {bbox!r}")
    if min_lon >= max_lon or min_lat >= max_lat:
        raise ValueError(f"bbox min must be < max on both axes, got: {bbox!r}")


# Jinja template expression rendering an optional 'bbox' param
# ("min_lon,min_lat,max_lon,max_lat") as a SQL POLYGON WKT literal
# (Athena/Trino-compatible). Only valid inside a `{% if params.bbox %}`
# guard. The `float` filter guarantees each rendered component is a
# numeric literal even if a value sidesteps BBOX_PARAM_PATTERN
# validation, so the WKT string cannot be broken out of.
BBOX_WKT_JINJA = (
    "{% set _b = params.bbox.split(',') %}"
    "POLYGON(({{ _b[0] | float }} {{ _b[1] | float }}, "
    "{{ _b[2] | float }} {{ _b[1] | float }}, "
    "{{ _b[2] | float }} {{ _b[3] | float }}, "
    "{{ _b[0] | float }} {{ _b[3] | float }}, "
    "{{ _b[0] | float }} {{ _b[1] | float }}))"
)
