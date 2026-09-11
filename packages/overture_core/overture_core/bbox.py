"""Shared bbox parameter validation pattern for Airflow DAGs."""

# JSON-schema pattern for the DAGs' optional 'bbox' param: empty (full
# planet) or exactly four comma-separated plain numbers. Param validation
# runs at trigger time — including on conf forwarded by
# dataset_osm_orchestrator_dag — and guards the SQL/WKT interpolation
# sites (see BBOX_WKT_JINJA) against malformed or quote-bearing input.
# Job-side, overture_spark.bbox.parse_bbox stays the semantic gate
# (ranges, min < max).
BBOX_PARAM_PATTERN = r"^(?:|-?[0-9]+(?:\.[0-9]+)?(?:,-?[0-9]+(?:\.[0-9]+)?){3})$(?![\s\S])"
