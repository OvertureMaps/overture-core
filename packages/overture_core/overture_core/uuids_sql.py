"""SQL-string generators for the RFC 4122 UUIDs in `overture_core.uuids`.

Builds SQL expressions equivalent to `generate_uuid3`/`generate_uuid4`/
`generate_uuid5`, for embedding directly into a Spark or Trino query instead
of round-tripping through a Python UDF. `name_sql` is any SQL expression that
produces the string to hash (a column reference, a literal, a `concat(...)`
call, and so on), not just a literal.

Spark's `md5()`/`sha1()` return lowercase hex strings directly. Trino's
operate on and return `varbinary`, so the digest is hex-encoded explicitly
via `to_hex()`. Both dialects then share the same portable, engine-neutral
`CASE` expression to force the UUID variant nibble into RFC 4122's required
8-b range, which avoids depending on either dialect's bitwise or
base-conversion functions.

`generate_uuid3_sql_legacy_spark_bug` is a separate, deprecated function
that reproduces a known bug in `tf-data-platform`'s `uuid_v3_sql` byte for
byte, see its docstring and OvertureMaps/tf-data-platform#5051.
"""

import uuid

_SUPPORTED_ENGINES = ("spark", "trino")

# RFC 4122 requires the variant nibble (character 16, 0-indexed, of the raw
# digest) to have its two high bits set to `10`, i.e. `nibble & 0x3 | 0x8`.
# Every hex digit maps to exactly one of 8/9/a/b via `8 + (nibble % 4)`, so
# the substitution is a plain lookup rather than a bitwise expression.
_VARIANT_NIBBLE_GROUPS = {"8": "048c", "9": "159d", "a": "26ae", "b": "37bf"}


def _check_engine(engine: str) -> None:
    if engine not in _SUPPORTED_ENGINES:
        raise ValueError(
            f"Unsupported engine {engine!r}; expected one of {_SUPPORTED_ENGINES}"
        )


def _variant_nibble_sql(nibble_sql: str) -> str:
    whens = " ".join(
        f"WHEN '{digit}' THEN '{result}'"
        for result, digits in _VARIANT_NIBBLE_GROUPS.items()
        for digit in digits
    )
    return f"(CASE {nibble_sql} {whens} END)"


def _uuid_from_digest_hex_sql(digest_hex_sql: str, version: str) -> str:
    """Assemble an 8-4-4-4-12 UUID string from a lowercase hex digest expression.

    `digest_hex_sql` must evaluate to a hex string of at least 32 characters;
    a longer digest (for example SHA-1's 40 hex characters) is fine since a
    UUID only consumes the first 16 bytes.
    """
    nibble_sql = _variant_nibble_sql(f"substring({digest_hex_sql}, 17, 1)")
    return (
        "concat_ws('-', "
        f"substring({digest_hex_sql}, 1, 8), "
        f"substring({digest_hex_sql}, 9, 4), "
        f"concat('{version}', substring({digest_hex_sql}, 14, 3)), "
        f"concat({nibble_sql}, substring({digest_hex_sql}, 18, 3)), "
        f"substring({digest_hex_sql}, 21, 12))"
    )


def _digest_hex_sql(
    algorithm: str, namespace: uuid.UUID, name_sql: str, engine: str
) -> str:
    namespace_hex = namespace.hex
    if engine == "spark":
        return f"lower({algorithm}(unhex('{namespace_hex}') || encode({name_sql}, 'UTF-8')))"
    return f"lower(to_hex({algorithm}(concat(from_hex('{namespace_hex}'), to_utf8({name_sql})))))"


def generate_uuid3_sql(
    namespace: uuid.UUID, name_sql: str, *, engine: str = "spark"
) -> str:
    """Build a SQL expression equivalent to `generate_uuid3(namespace, name_sql)`.

    `namespace` is embedded as a literal; `name_sql` is any SQL expression
    (column reference, literal, `concat(...)`, etc.) producing the string to
    hash. Raises ValueError for an unsupported `engine`.

    >>> generate_uuid3_sql(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), "name")
    "concat_ws('-', substring(lower(md5(unhex('6ba7b8109dad11d180b400c04fd430c8') || encode(name, 'UTF-8'))), 1, 8), substring(lower(md5(unhex('6ba7b8109dad11d180b400c04fd430c8') || encode(name, 'UTF-8'))), 9, 4), concat('3', substring(lower(md5(unhex('6ba7b8109dad11d180b400c04fd430c8') || encode(name, 'UTF-8'))), 14, 3)), concat((CASE substring(lower(md5(unhex('6ba7b8109dad11d180b400c04fd430c8') || encode(name, 'UTF-8'))), 17, 1) WHEN '0' THEN '8' WHEN '4' THEN '8' WHEN '8' THEN '8' WHEN 'c' THEN '8' WHEN '1' THEN '9' WHEN '5' THEN '9' WHEN '9' THEN '9' WHEN 'd' THEN '9' WHEN '2' THEN 'a' WHEN '6' THEN 'a' WHEN 'a' THEN 'a' WHEN 'e' THEN 'a' WHEN '3' THEN 'b' WHEN '7' THEN 'b' WHEN 'b' THEN 'b' WHEN 'f' THEN 'b' END), substring(lower(md5(unhex('6ba7b8109dad11d180b400c04fd430c8') || encode(name, 'UTF-8'))), 18, 3)), substring(lower(md5(unhex('6ba7b8109dad11d180b400c04fd430c8') || encode(name, 'UTF-8'))), 21, 12))"
    """
    _check_engine(engine)
    return _uuid_from_digest_hex_sql(
        _digest_hex_sql("md5", namespace, name_sql, engine), "3"
    )


def generate_uuid5_sql(
    namespace: uuid.UUID, name_sql: str, *, engine: str = "spark"
) -> str:
    """Build a SQL expression equivalent to `generate_uuid5(namespace, name_sql)`.

    Same contract as `generate_uuid3_sql`, but SHA-1 based (RFC 4122 v5).
    """
    _check_engine(engine)
    return _uuid_from_digest_hex_sql(
        _digest_hex_sql("sha1", namespace, name_sql, engine), "5"
    )


def generate_uuid4_sql(*, engine: str = "spark") -> str:
    """Build a SQL expression for a random UUID (v4) using the engine's native `uuid()`.

    Trino's `uuid()` returns the `uuid` type rather than a string, so it is
    cast to `varchar` to match `generate_uuid4`'s `str` return type.

    >>> generate_uuid4_sql(engine="trino")
    'cast(uuid() as varchar)'
    """
    _check_engine(engine)
    return "uuid()" if engine == "spark" else "cast(uuid() as varchar)"


def generate_uuid3_sql_legacy_spark_bug(namespace: uuid.UUID, name_sql: str) -> str:
    """Reproduce `tf-data-platform`'s current `uuid_v3_sql`, bug included.

    Deprecated: only for a caller that already has this function's output
    baked into a stable ID and needs an exact byte-for-byte match while it
    plans a migration. Use `generate_uuid3_sql` for anything new.

    The reference implementation wraps `md5()`'s output in an extra `hex()`.
    Spark's `md5()` already returns a 32-character lowercase hex string, so
    the extra `hex()` re-encodes each of those hex characters as its own
    2-digit hex byte value, producing a 64-character string. Only the first
    32 characters of that string ever reach the final UUID, which means only
    the first 16 hex digits of the real 32-digit digest are used. The
    digest's second half is discarded, halving the hash's effective entropy
    (128 bits down to about 64). See
    OvertureMaps/tf-data-platform#5051 for the bug report and the planned
    migration off this function.

    Spark-only: the reference implementation this mirrors never had a Trino
    equivalent, so there's no `engine` parameter here.

    >>> generate_uuid3_sql_legacy_spark_bug(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), "name")
    "concat_ws('-', substring(lower(hex(md5(unhex('6ba7b8109dad11d180b400c04fd430c8') || encode(name, 'UTF-8')))), 1, 8), substring(lower(hex(md5(unhex('6ba7b8109dad11d180b400c04fd430c8') || encode(name, 'UTF-8')))), 9, 4), concat('3', substring(lower(hex(md5(unhex('6ba7b8109dad11d180b400c04fd430c8') || encode(name, 'UTF-8')))), 14, 3)), concat((CASE substring(lower(hex(md5(unhex('6ba7b8109dad11d180b400c04fd430c8') || encode(name, 'UTF-8')))), 17, 1) WHEN '0' THEN '8' WHEN '4' THEN '8' WHEN '8' THEN '8' WHEN 'c' THEN '8' WHEN '1' THEN '9' WHEN '5' THEN '9' WHEN '9' THEN '9' WHEN 'd' THEN '9' WHEN '2' THEN 'a' WHEN '6' THEN 'a' WHEN 'a' THEN 'a' WHEN 'e' THEN 'a' WHEN '3' THEN 'b' WHEN '7' THEN 'b' WHEN 'b' THEN 'b' WHEN 'f' THEN 'b' END), substring(lower(hex(md5(unhex('6ba7b8109dad11d180b400c04fd430c8') || encode(name, 'UTF-8')))), 18, 3)), substring(lower(hex(md5(unhex('6ba7b8109dad11d180b400c04fd430c8') || encode(name, 'UTF-8')))), 21, 12))"
    """
    namespace_hex = namespace.hex
    digest_hex_sql = (
        f"lower(hex(md5(unhex('{namespace_hex}') || encode({name_sql}, 'UTF-8'))))"
    )
    return _uuid_from_digest_hex_sql(digest_hex_sql, "3")
