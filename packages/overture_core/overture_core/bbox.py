"""Shared bbox parameter validation helpers for Airflow DAGs."""

# JSON-schema pattern for the DAGs' optional 'bbox' param: empty (full
# planet) or exactly four comma-separated plain numbers. Param validation
# runs at trigger time — including on conf forwarded by
# dataset_osm_orchestrator_dag — and guards the SQL/WKT interpolation
# sites (see BBOX_WKT_JINJA) against malformed or quote-bearing input.
# Job-side, overture_spark.bbox.parse_bbox stays the semantic gate
# (ranges, min < max).
BBOX_PARAM_PATTERN = (
    r"^(?:|-?[0-9]+(?:\.[0-9]+)?(?:,-?[0-9]+(?:\.[0-9]+)?){3})$(?![\s\S])"
)


def validate_bbox(bbox: str) -> None:
    """Semantic validation of an optional bbox param ('' = full planet).

    BBOX_PARAM_PATTERN only constrains the shape at trigger time; this
    enforces the contract (lon/lat ranges, min < max). It is the DAG-side
    equivalent of overture_spark.bbox.parse_bbox for fail-fast validation
    where no Spark job runs — the Athena-only queries interpolate
    BBOX_WKT_JINJA directly and would otherwise execute with a
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


# Jinja template expression rendering the DAG's optional 'bbox' param
# ("min_lon,min_lat,max_lon,max_lat") as an Athena/Trino POLYGON WKT literal.
# Only valid inside a `{% if params.bbox %}` guard. The `float` filter
# guarantees each rendered component is a numeric literal even if a value
# sidesteps BBOX_PARAM_PATTERN validation, so the WKT string cannot be
# broken out of.
BBOX_WKT_JINJA = (
    "{% set _b = params.bbox.split(',') %}"
    "POLYGON(({{ _b[0] | float }} {{ _b[1] | float }}, "
    "{{ _b[2] | float }} {{ _b[1] | float }}, "
    "{{ _b[2] | float }} {{ _b[3] | float }}, "
    "{{ _b[0] | float }} {{ _b[3] | float }}, "
    "{{ _b[0] | float }} {{ _b[1] | float }}))"
)
