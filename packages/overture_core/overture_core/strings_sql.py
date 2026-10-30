"""SQL-string helpers for the Overture schema's string constraints."""

_SUPPORTED_ENGINES = ("spark",)

# Unescapes to the Java regex `[\s\p{Cc}]` inside a Spark SQL string literal.
_BOUNDARY_CLASS = r"[\\s\\p{Cc}]"


def stripped_string_sql(value_sql: str, *, engine: str = "spark") -> str:
    """Build a SQL expression that makes `value_sql` a schema `StrippedString`.

    Strips leading and trailing Unicode whitespace and control characters
    (which `trim()` misses, e.g. NBSP, DEL, C1), and returns NULL when nothing
    remains. Interior characters are kept. Spark only; raises ValueError for
    any other `engine`.
    """
    if engine not in _SUPPORTED_ENGINES:
        raise ValueError(
            f"Unsupported engine {engine!r}; expected one of {_SUPPORTED_ENGINES}"
        )
    return (
        f"nullif(regexp_replace({value_sql}, "
        f"'(?U)^{_BOUNDARY_CLASS}+|{_BOUNDARY_CLASS}+\\\\z', ''), '')"
    )
