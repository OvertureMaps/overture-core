"""Unit tests for the SQL-string UUID generators.

`TestSparkDialectMatchesPython` is the "cheap CI" check: it runs the
generated Spark-dialect SQL against DuckDB (no JVM required) and asserts it
reproduces `generate_uuid3`/`generate_uuid5`'s output exactly. DuckDB's
`md5()`/`sha1()` return lowercase hex strings directly, matching documented
Spark semantics, so it is a fast, dependency-light stand-in for a real Spark
session; genuine Spark/Trino syntax is exercised separately in CI, in the
engine-specific integration job.
"""

import uuid
from uuid import UUID

import duckdb
import pytest

from overture_core.uuids import generate_uuid3, generate_uuid5
from overture_core.uuids_sql import (
    generate_uuid3_sql,
    generate_uuid3_sql_legacy_spark_bug,
    generate_uuid4_sql,
    generate_uuid5_sql,
)

NAMESPACE_DNS = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
NAMES = ["example.com", "overture maps", "", "unicode-\u00e9\u00e8-name"]


class TestGenerateUuid3Sql:
    """Structural checks on the generated SQL, no engine required."""

    def test_returns_str(self):
        assert isinstance(generate_uuid3_sql(NAMESPACE_DNS, "name"), str)

    def test_defaults_to_spark(self):
        assert generate_uuid3_sql(NAMESPACE_DNS, "name") == generate_uuid3_sql(
            NAMESPACE_DNS, "name", engine="spark"
        )

    def test_spark_uses_md5_unhex_encode(self):
        sql = generate_uuid3_sql(NAMESPACE_DNS, "name", engine="spark")
        assert "md5(" in sql
        assert "unhex(" in sql
        assert "encode(name, 'UTF-8')" in sql

    def test_trino_uses_md5_from_hex_to_utf8(self):
        sql = generate_uuid3_sql(NAMESPACE_DNS, "name", engine="trino")
        assert "to_hex(md5(" in sql
        assert "from_hex(" in sql
        assert "to_utf8(name)" in sql

    def test_embeds_namespace_as_hex_literal(self):
        sql = generate_uuid3_sql(NAMESPACE_DNS, "name", engine="spark")
        assert NAMESPACE_DNS.hex in sql

    def test_embeds_name_sql_verbatim(self):
        sql = generate_uuid3_sql(NAMESPACE_DNS, "concat(a, b)", engine="spark")
        assert "concat(a, b)" in sql

    def test_unsupported_engine_raises(self):
        with pytest.raises(ValueError, match="Unsupported engine"):
            generate_uuid3_sql(NAMESPACE_DNS, "name", engine="postgres")


class TestGenerateUuid5Sql:
    """Structural checks on the generated SQL, no engine required."""

    def test_spark_uses_sha1_unhex_encode(self):
        sql = generate_uuid5_sql(NAMESPACE_DNS, "name", engine="spark")
        assert "sha1(" in sql
        assert "unhex(" in sql
        assert "encode(name, 'UTF-8')" in sql

    def test_trino_uses_sha1_from_hex_to_utf8(self):
        sql = generate_uuid5_sql(NAMESPACE_DNS, "name", engine="trino")
        assert "to_hex(sha1(" in sql
        assert "from_hex(" in sql
        assert "to_utf8(name)" in sql

    def test_differs_from_uuid3_sql(self):
        assert generate_uuid3_sql(NAMESPACE_DNS, "name") != generate_uuid5_sql(
            NAMESPACE_DNS, "name"
        )

    def test_unsupported_engine_raises(self):
        with pytest.raises(ValueError, match="Unsupported engine"):
            generate_uuid5_sql(NAMESPACE_DNS, "name", engine="postgres")


class TestGenerateUuid4Sql:
    """generate_uuid4_sql just wraps each engine's native uuid() builtin."""

    def test_spark_is_bare_uuid_call(self):
        assert generate_uuid4_sql(engine="spark") == "uuid()"

    def test_trino_casts_uuid_to_varchar(self):
        assert generate_uuid4_sql(engine="trino") == "cast(uuid() as varchar)"

    def test_unsupported_engine_raises(self):
        with pytest.raises(ValueError, match="Unsupported engine"):
            generate_uuid4_sql(engine="postgres")


class TestGenerateUuid3SqlLegacySparkBug:
    """Covers the deprecated bug-for-bug-compatible variant.

    See `TestSparkDialectMatchesPython.test_legacy_matches_known_buggy_value`
    for the DuckDB-executed proof that this reproduces
    `tf-data-platform`'s actual `uuid_v3_sql` output.
    """

    def test_returns_str(self):
        assert isinstance(
            generate_uuid3_sql_legacy_spark_bug(NAMESPACE_DNS, "name"), str
        )

    def test_wraps_md5_in_extra_hex(self):
        sql = generate_uuid3_sql_legacy_spark_bug(NAMESPACE_DNS, "name")
        assert "hex(md5(" in sql

    def test_differs_from_correct_generate_uuid3_sql(self):
        assert generate_uuid3_sql(
            NAMESPACE_DNS, "name"
        ) != generate_uuid3_sql_legacy_spark_bug(NAMESPACE_DNS, "name")

    def test_embeds_namespace_as_hex_literal(self):
        sql = generate_uuid3_sql_legacy_spark_bug(NAMESPACE_DNS, "name")
        assert NAMESPACE_DNS.hex in sql

    def test_embeds_name_sql_verbatim(self):
        sql = generate_uuid3_sql_legacy_spark_bug(NAMESPACE_DNS, "concat(a, b)")
        assert "concat(a, b)" in sql


class TestSparkDialectMatchesPython:
    """Executes the generated Spark-dialect SQL against DuckDB.

    DuckDB's single-argument `encode(string)` differs from Spark's two-arg
    `encode(string, charset)`; the charset argument is stripped before
    execution here since this test targets digest-construction parity, not
    the literal generated SQL text (the real Spark syntax is exercised by
    the engine-specific CI job).
    """

    @classmethod
    @pytest.fixture(scope="class")
    def con(cls):
        return duckdb.connect()

    @pytest.mark.parametrize("name", NAMES)
    def test_uuid3_sql_matches_generate_uuid3(self, con, name):
        expected = generate_uuid3(NAMESPACE_DNS, name)
        sql = generate_uuid3_sql(NAMESPACE_DNS, "name_col", engine="spark").replace(
            ", 'UTF-8')", ")"
        )
        got = con.execute(
            f"select {sql} from (select ? as name_col)", [name]
        ).fetchone()[0]
        assert got == expected

    @pytest.mark.parametrize("name", NAMES)
    def test_uuid5_sql_matches_generate_uuid5(self, con, name):
        expected = generate_uuid5(NAMESPACE_DNS, name)
        sql = generate_uuid5_sql(NAMESPACE_DNS, "name_col", engine="spark").replace(
            ", 'UTF-8')", ")"
        )
        got = con.execute(
            f"select {sql} from (select ? as name_col)", [name]
        ).fetchone()[0]
        assert got == expected

    def test_other_namespace_still_matches(self, con):
        namespace = uuid.UUID("3d6a33ba-1abe-4aaa-abcd-5aa7ccb6ca42")
        expected = generate_uuid3(namespace, "example.com")
        sql = generate_uuid3_sql(namespace, "name_col", engine="spark").replace(
            ", 'UTF-8')", ")"
        )
        got = con.execute(
            f"select {sql} from (select ? as name_col)", ["example.com"]
        ).fetchone()[0]
        assert got == expected

    def test_legacy_matches_known_buggy_value(self, con):
        # Value from OvertureMaps/tf-data-platform#5047's before/after example:
        # `uuid_v3_sql` for NAMESPACE_DNS + "example.com" produces this, not a
        # valid v3 UUID, instead of the correct `9073926b-929f-31c2-...`.
        sql = generate_uuid3_sql_legacy_spark_bug(NAMESPACE_DNS, "name_col").replace(
            ", 'UTF-8')", ")"
        )
        got = con.execute(
            f"select {sql} from (select ? as name_col)", ["example.com"]
        ).fetchone()[0]
        assert got == "39303733-3932-3662-b932-396664316332"
