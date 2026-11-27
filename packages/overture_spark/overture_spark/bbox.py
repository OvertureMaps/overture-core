"""The Overture ``bbox`` struct: its field order and the Spark SQL that builds it.

Overture features carry a ``bbox`` struct column with the bounds of their
geometry. GeoParquet 1.1 requires the fields of a bbox covering to be ordered
``xmin, ymin, xmax, ymax``, so every job that builds or checks the struct
takes the order from ``BBOX_FIELDS`` instead of writing it out again.

``bbox_from_geometry_sql`` returns float32 bounds, rounded outward. A double
cast to float32 can land on the wrong side of the original value, which would
leave the geometry slightly outside its own bbox, so a bound that moved the
wrong way is nudged back past the original. Each bound is the nearest float32
on the outer side of the geometry's bound, or the one after it, so the result
always contains the geometry.

The nudge is relative (one part in 2**23), which is too small to move a value
below the smallest normal float32 (about 1.2e-38), where float32 values are a
fixed 2**-149 apart. Those bounds are moved by that fixed gap instead.

A NULL geometry gives a struct of four NULLs, not a NULL struct.

To store the bounds as another type, cast the struct to a type whose fields
are listed in ``BBOX_FIELDS`` order. Spark casts structs by position, so a
target type in any other order relabels the values without an error.
"""

__all__ = ["BBOX_FIELDS", "bbox_from_geometry_sql"]

# Field order GeoParquet 1.1 requires for a bounding box column.
BBOX_FIELDS = ("xmin", "ymin", "xmax", "ymax")

# One float32 mantissa step, as a factor: 1 - 2**-23 and 1 + 2**-23.
_RND_TOWARDS_ZERO = "0.9999998807907104"
_RND_AWAY_FROM_ZERO = "1.0000001192092896"

# The smallest normal float32 (2**-126). Below it, float32 values are a fixed
# 2**-149 apart, and the relative factors above no longer move a value.
_MIN_NORMAL = "1.1754943508222875E-38"
_SUBNORMAL_GAP = "CAST(1.401298464324817E-45 AS FLOAT)"


def _next_float_below_sql(value_sql: str) -> str:
    """SQL for a float32 at or below the double ``value_sql``.

    It is the nearest float32 at or below the value, or the one before it.
    """
    return (
        f"(CASE WHEN CAST({value_sql} AS FLOAT) > {value_sql} "
        f"AND abs({value_sql}) < {_MIN_NORMAL} "
        f"THEN CAST({value_sql} AS FLOAT) - {_SUBNORMAL_GAP} "
        f"WHEN CAST({value_sql} AS FLOAT) > {value_sql} AND {value_sql} < 0 "
        f"THEN CAST({value_sql} * {_RND_AWAY_FROM_ZERO} AS FLOAT) "
        f"WHEN CAST({value_sql} AS FLOAT) > {value_sql} "
        f"THEN CAST({value_sql} * {_RND_TOWARDS_ZERO} AS FLOAT) "
        f"ELSE CAST({value_sql} AS FLOAT) END)"
    )


def _next_float_above_sql(value_sql: str) -> str:
    """SQL for a float32 at or above the double ``value_sql``.

    It is the nearest float32 at or above the value, or the one after it.
    """
    return (
        f"(CASE WHEN CAST({value_sql} AS FLOAT) < {value_sql} "
        f"AND abs({value_sql}) < {_MIN_NORMAL} "
        f"THEN CAST({value_sql} AS FLOAT) + {_SUBNORMAL_GAP} "
        f"WHEN CAST({value_sql} AS FLOAT) < {value_sql} AND {value_sql} < 0 "
        f"THEN CAST({value_sql} * {_RND_TOWARDS_ZERO} AS FLOAT) "
        f"WHEN CAST({value_sql} AS FLOAT) < {value_sql} "
        f"THEN CAST({value_sql} * {_RND_AWAY_FROM_ZERO} AS FLOAT) "
        f"ELSE CAST({value_sql} AS FLOAT) END)"
    )


def bbox_from_geometry_sql(geometry_expr: str = "ST_GeomFromWKB(geometry)") -> str:
    """Spark SQL expression: the ``bbox`` struct of a Sedona geometry.

    ``geometry_expr`` must evaluate to a Sedona geometry. The fields are in
    ``BBOX_FIELDS`` order and float32, rounded outward (see module
    docstring).
    """
    bounds = {
        "xmin": _next_float_below_sql(f"ST_XMin({geometry_expr})"),
        "ymin": _next_float_below_sql(f"ST_YMin({geometry_expr})"),
        "xmax": _next_float_above_sql(f"ST_XMax({geometry_expr})"),
        "ymax": _next_float_above_sql(f"ST_YMax({geometry_expr})"),
    }
    fields = ", ".join(f"'{name}', {bounds[name]}" for name in BBOX_FIELDS)
    return f"named_struct({fields})"
