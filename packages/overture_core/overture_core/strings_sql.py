"""SQL-string helpers for the Overture schema's string constraints."""

_STRIP_PATTERNS = {
    # Java regex; Spark unescapes `\\` inside SQL string literals.
    "spark": r"(?U)^[\\s\\p{Cc}]+|[\\s\\p{Cc}]+\\z",
}


def stripped_string_sql(value_sql: str, *, engine: str = "spark") -> str:
    """Build a SQL expression that makes `value_sql` a schema `StrippedString`.

    Strips leading and trailing Unicode whitespace and control characters
    (which `trim()` misses, e.g. NBSP, DEL, C1), and returns NULL when nothing
    remains. Interior characters are kept. Spark only; raises ValueError for
    any other `engine`.
    """
    if engine not in _STRIP_PATTERNS:
        raise ValueError(
            f"Unsupported engine {engine!r}; expected one of {tuple(_STRIP_PATTERNS)}"
        )
    return f"nullif(regexp_replace({value_sql}, '{_STRIP_PATTERNS[engine]}', ''), '')"
