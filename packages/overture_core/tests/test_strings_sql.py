"""Unit tests for the StrippedString SQL helper (no engine required).

Execution is tested against real Spark in `test_strings_sql_engines.py`.
DuckDB isn't a stand-in here: its `(?U)` means "ungreedy".
"""

import pytest

from overture_core.strings_sql import stripped_string_sql


class TestStrippedStringSql:
    def test_returns_str(self):
        assert isinstance(stripped_string_sql("name"), str)

    def test_defaults_to_spark(self):
        assert stripped_string_sql("name") == stripped_string_sql(
            "name", engine="spark"
        )

    def test_embeds_value_expression_verbatim(self):
        sql = stripped_string_sql("brand.names.primary")
        assert "regexp_replace(brand.names.primary, " in sql

    def test_empty_result_becomes_null(self):
        sql = stripped_string_sql("name")
        assert sql.startswith("nullif(")
        assert sql.endswith(", '')")

    def test_strips_only_the_boundary_class(self):
        sql = stripped_string_sql("name")
        assert r"'(?U)^[\\s\\p{Cc}]+|[\\s\\p{Cc}]+\\z'" in sql
        assert sql.count("regexp_replace(") == 1  # no whitespace collapse

    @pytest.mark.parametrize("engine", ["trino", "duckdb", "SPARK"])
    def test_unsupported_engine_raises(self, engine):
        with pytest.raises(ValueError, match=repr(engine)):
            stripped_string_sql("name", engine=engine)
