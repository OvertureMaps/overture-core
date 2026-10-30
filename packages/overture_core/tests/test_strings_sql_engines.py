"""Tests for `stripped_string_sql` against real Spark. Run with `pytest -m spark`."""

import pytest

from overture_core.strings_sql import stripped_string_sql

# overture-schema-pyspark's StrippedString check (`_STRIPPED_PATTERN`). Copied
# because overture-core doesn't depend on the schema package.
_SCHEMA_STRIPPED_PATTERN = r"(?sU)^[^\s\p{Cc}](.*[^\s\p{Cc}])?\z"

CASES = [
    ("Costa", "Costa"),
    ("  Costa  ", "Costa"),
    ("\tCosta\n", "Costa"),
    ("Costa \n", "Costa"),
    ("\u00a0Costa\u00a0", "Costa"),  # NBSP
    ("\u3000Costa\u3000", "Costa"),  # ideographic space
    ("Costa\u2028", "Costa"),  # line separator
    ("Costa\u007f", "Costa"),  # DEL
    ("\u0091Costa\u009f", "Costa"),  # C1
    ("\u0000Costa\u001f", "Costa"),  # C0
    ("\u0085Costa", "Costa"),  # NEL
    # Interior characters are kept.
    ("Costa  Coffee", "Costa  Coffee"),
    ("コストコ\u3000ガス", "コストコ\u3000ガス"),
    ("Costa\n\nCoffee", "Costa\n\nCoffee"),
    ("a\u0000b", "a\u0000b"),
    # Format chars (Cf) and combining marks (Mn) are allowed by StrippedString,
    # so kept. Unlike overture_osm's clean_name_value_sql, U+200B isn't spaced.
    ("\u200bCosta\u200b", "\u200bCosta\u200b"),  # ZWSP
    ("\ufeffCosta", "\ufeffCosta"),  # BOM
    ("Ca\u0301fe\u0301", "Ca\u0301fe\u0301"),
    ("   ", None),
    ("\u007f\u00a0", None),
    ("", None),
    (None, None),
]


@pytest.mark.spark
class TestSparkEngine:
    @pytest.mark.parametrize(("value", "expected"), CASES)
    def test_strips_boundary_characters_only(self, spark, value, expected):
        df = spark.createDataFrame([(value,)], "v string")
        assert df.selectExpr(stripped_string_sql("v")).first()[0] == expected

    def test_accepts_a_struct_field_expression(self, spark):
        df = spark.createDataFrame([((" Costa ",),)], "s struct<v: string>")
        assert df.selectExpr(stripped_string_sql("s.v")).first()[0] == "Costa"

    def _checked(self, spark):
        from pyspark.sql.functions import col, expr

        df = spark.createDataFrame([(v,) for v, _ in CASES], "v string")
        return (
            df.select(
                "v",
                expr(stripped_string_sql("v")).alias("c"),
                col("v").rlike(_SCHEMA_STRIPPED_PATTERN).alias("v_ok"),
            )
            .select(
                "v", "c", "v_ok", col("c").rlike(_SCHEMA_STRIPPED_PATTERN).alias("c_ok")
            )
            .collect()
        )

    def test_output_satisfies_stripped_string(self, spark):
        for r in self._checked(spark):
            assert r.c is None or r.c_ok, repr(r.v)

    def test_already_stripped_input_is_unchanged(self, spark):
        rows = [r for r in self._checked(spark) if r.v_ok]
        assert rows
        for r in rows:
            assert r.c == r.v, repr(r.v)
