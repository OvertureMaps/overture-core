"""Structural checks only.

The design is expected to move, so nothing here pins down a rule's name or what it
means. These check that the package holds together: that the schema matches the
types, that the exports match, and that the id derivation both a writer and a
reader depend on agrees with itself. Each one fails on a genuine mistake and stays
quiet through a redesign.
"""

import dataclasses

import overture_cairn
import overture_cairn.core
from overture_cairn import (
    ENDPOINTS,
    OP_COLUMNS,
    EdgeKind,
    IdSpec,
    Op,
    Problems,
    RecordsCaptured,
    check_run,
    op_id_for,
    op_row,
    slug,
)


def test_the_top_level_re_exports_the_whole_core():
    """Moving a name between core modules is easy to do without updating both
    export lists, and an adapter is what finds out."""
    assert set(overture_cairn.core.__all__) <= set(overture_cairn.__all__)


def test_every_export_actually_exists():
    for name in overture_cairn.__all__:
        assert hasattr(overture_cairn, name), name


def test_the_ops_schema_and_the_op_type_describe_the_same_thing():
    """A column with no field, or a field with no column, means an adapter writes a
    table nobody can read back."""
    assert {c.name for c in OP_COLUMNS} == {f.name for f in dataclasses.fields(Op)}


def test_op_row_renders_every_column_in_schema_order():
    op = Op(
        op_id=op_id_for("r1", "read feed"),
        run_id="r1",
        name="read feed",
        reason="the esri ingest",
        records_captured=RecordsCaptured.COMPLETE,
        output_id_cols=IdSpec.of("id"),
        physical_source=["s3://b/esri"],
    )
    row = op_row(op)
    assert list(row) == [c.name for c in OP_COLUMNS]
    assert row["records_captured"] == "complete"
    assert row["output_id_cols"] == ["id"]
    assert row["physical_dest"] is None


def test_every_kind_says_which_ends_it_carries():
    assert set(ENDPOINTS) == set(EdgeKind)


def test_ids_come_from_names_and_stay_scoped_to_a_run():
    assert op_id_for("r1", "Drop Weak Overlaps") == op_id_for(
        "r1", "drop weak overlaps"
    )
    assert op_id_for("r1", "match") != op_id_for("r2", "match")
    assert slug("ingest//esri!!") == "ingest-esri"


def test_a_composite_id_needs_no_special_case():
    assert IdSpec.of("provider", "id").columns == ("provider", "id")


def test_checking_a_sound_run_finds_nothing_to_report():
    op = Op(
        op_id=op_id_for("r1", "check bucket"),
        run_id="r1",
        name="check bucket",
        reason="fail early if the output path is unwritable",
        records_captured=RecordsCaptured.NOT_APPLICABLE,
    )
    assert check_run([op], Problems()).items == []
