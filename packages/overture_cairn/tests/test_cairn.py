"""Checks that the package holds together and that the rules in the design spec
hold for the cases they name.

The structural checks fail on a genuine mistake and stay quiet through a redesign.
The rule checks pin down each validation the spec lists, one test apiece, so a rule
that gets dropped or inverted shows up here.
"""

from datetime import datetime, timezone

import pytest

import overture_cairn
import overture_cairn.core
from overture_cairn import (
    ENDPOINTS,
    OP_FIELD_NAMES,
    OPERATION_COLUMNS,
    Kind,
    Op,
    OpType,
    Problems,
    Run,
    changes_grain,
    check_acyclic,
    check_operations,
    check_row_detail,
    check_row_detail_row,
    op_id_for,
    op_row,
    op_type_of,
    row_detail_row,
    slug,
)

AT = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)


def an_op(op_key: str, **kwargs) -> Op:
    return Op(
        op_id=op_id_for("r1", op_key),
        run_id="r1",
        op_key=op_key,
        description=kwargs.pop("description", "does a thing"),
        timestamp=AT,
        **kwargs,
    )


# Structure


def test_the_top_level_re_exports_the_whole_core():
    """Moving a name between core modules is easy to do without updating both
    export lists, and an adapter is what finds out."""
    assert set(overture_cairn.core.__all__) <= set(overture_cairn.__all__)


def test_every_export_actually_exists():
    for name in overture_cairn.__all__:
        assert hasattr(overture_cairn, name), name


def test_the_operations_schema_and_the_op_type_describe_the_same_thing():
    """A column with no field, or a field with no column, means an adapter writes a
    table nobody can read back."""
    assert {c.name for c in OPERATION_COLUMNS} == set(OP_FIELD_NAMES)


def test_op_row_renders_every_column_in_schema_order():
    op = an_op("read_feed", physical_source="s3://b/esri", output_key_columns=["id"])
    row = op_row(op)
    assert list(row) == [c.name for c in OPERATION_COLUMNS]
    assert row["output_key_columns"] == ["id"]
    assert row["physical_dest"] is None
    assert row["input_op_ids"] == []


def test_every_kind_says_which_ends_it_carries():
    assert set(ENDPOINTS) == set(Kind)


def test_ids_come_from_keys_and_stay_scoped_to_a_run():
    assert op_id_for("r1", "Drop Weak Overlaps") == op_id_for(
        "r1", "drop weak overlaps"
    )
    assert op_id_for("r1", "match") != op_id_for("r2", "match")
    assert slug("ingest//esri!!") == "ingest-esri"


# Operation shape


@pytest.mark.parametrize(
    "source,dest,expected",
    [
        (None, None, OpType.TRANSFORM),
        ("s3://in", None, OpType.READ),
        (None, "s3://out", OpType.WRITE),
        ("s3://in", "s3://out", OpType.COPY),
    ],
)
def test_shape_is_read_off_the_locations(source, dest, expected):
    """Nothing stores the shape, so an operation cannot disagree with itself about
    what it is."""
    op = an_op("o", physical_source=source, physical_dest=dest)
    assert op_type_of(op) is expected


def test_a_read_consumes_no_operation():
    ops = [an_op("read", physical_source="s3://in"), an_op("t")]
    bad = an_op("read2", physical_source="s3://in", input_op_ids=[ops[1].op_id])
    problems = check_operations(ops + [bad], Problems())
    assert any(p.rule == "op.input_op_ids" for p in problems.items)


def test_a_write_takes_exactly_one_input():
    upstream = an_op("t1")
    other = an_op("t2")
    bad = an_op(
        "w",
        physical_dest="s3://out",
        input_op_ids=[upstream.op_id, other.op_id],
    )
    problems = check_operations([upstream, other, bad], Problems())
    assert any("exactly one input" in p.message for p in problems.items)


def test_nothing_may_consume_a_write():
    """A materialized handoff stays visible because the next operation reads the
    location back."""
    upstream = an_op("t")
    write = an_op("w", physical_dest="s3://out", input_op_ids=[upstream.op_id])
    after = an_op("t2", input_op_ids=[write.op_id])
    problems = check_operations([upstream, write, after], Problems())
    assert any("read that location back" in p.message for p in problems.items)


def test_only_a_transform_can_have_row_detail():
    bad = an_op("w", physical_dest="s3://out", has_row_detail=True, input_op_ids=["x"])
    problems = Problems()
    check_operations([bad], problems)
    assert any(p.rule == "op.has_row_detail" for p in problems.items)


def test_an_operation_needs_a_description():
    problems = Problems()
    check_operations([an_op("o", description="  ")], problems)
    assert any(p.rule == "op.description" for p in problems.items)


def test_an_input_must_name_an_operation_in_this_cairn():
    problems = Problems()
    check_operations([an_op("o", input_op_ids=["r1.nobody"])], problems)
    assert any("not an operation in this Cairn" in p.message for p in problems.items)


def test_a_sound_operations_table_finds_nothing_to_report():
    read = an_op("read_land", physical_source="s3://in", output_key_columns=["id"])
    drop = an_op("drop_invalid", has_row_detail=True, input_op_ids=[read.op_id])
    write = an_op("write_land", physical_dest="s3://out", input_op_ids=[drop.op_id])
    problems = check_operations([read, drop, write], Problems())
    check_acyclic([read, drop, write], problems)
    assert problems.items == []


# Cycles


def test_a_cycle_is_reported_once():
    a = an_op("a", input_op_ids=[op_id_for("r1", "b")])
    b = an_op("b", input_op_ids=[op_id_for("r1", "a")])
    problems = check_acyclic([a, b], Problems())
    assert len(problems.items) == 1
    assert "cycle" in problems.items[0].message


def test_an_operation_cannot_feed_itself():
    op = an_op("a", input_op_ids=[op_id_for("r1", "a")])
    problems = check_acyclic([op], Problems())
    assert len(problems.items) == 1


# Row detail rules


def test_a_well_formed_entry_breaks_nothing():
    row = row_detail_row("r1.o", Kind.DROPPED, input_id=["w1"], detail="bad geometry")
    assert check_row_detail_row(row) == []


@pytest.mark.parametrize(
    "kind,input_id,output_id",
    [
        (Kind.DROPPED, ["a"], None),
        (Kind.MINTED, None, ["a"]),
        (Kind.DERIVED_FROM, ["a"], ["b"]),
        (Kind.CONTENT_CHANGED, ["a"], ["a"]),
        (Kind.FLAGGED, ["a"], ["a"]),
    ],
)
def test_each_kind_accepts_the_ends_it_should(kind, input_id, output_id):
    row = row_detail_row("r1.o", kind, input_id=input_id, output_id=output_id)
    if kind is Kind.CONTENT_CHANGED:
        row["detail"] = "moved"
    assert check_row_detail_row(row) == []


def test_a_drop_carries_no_output_id():
    row = row_detail_row("r1.o", Kind.DROPPED, input_id=["a"], output_id=["a"])
    assert "row.output_id" in check_row_detail_row(row)


def test_a_mint_carries_no_input_id():
    row = row_detail_row("r1.o", Kind.MINTED, input_id=["a"], output_id=["b"])
    assert "row.input_id" in check_row_detail_row(row)


def test_a_content_change_keeps_the_same_id():
    row = row_detail_row("r1.o", Kind.CONTENT_CHANGED, input_id=["a"], output_id=["b"])
    assert "row.same_ids" in check_row_detail_row(row)


def test_a_derived_from_changes_the_id():
    row = row_detail_row("r1.o", Kind.DERIVED_FROM, input_id=["a"], output_id=["a"])
    assert "row.derived_from_ids" in check_row_detail_row(row)


def test_only_a_content_change_states_a_fate():
    row = row_detail_row(
        "r1.o",
        Kind.DERIVED_FROM,
        input_id=["a"],
        output_id=["b"],
        affected_output_columns=["height"],
        column_change="set",
    )
    assert "row.column_change" in check_row_detail_row(row)


def test_a_content_change_may_leave_the_fate_unstated():
    """Null means nobody said, so a hand-built entry can skip the comparison."""
    row = row_detail_row(
        "r1.o",
        Kind.CONTENT_CHANGED,
        input_id=["a"],
        output_id=["a"],
        affected_output_columns=["height"],
    )
    assert check_row_detail_row(row) == []


def test_an_unknown_fate_is_rejected():
    row = row_detail_row(
        "r1.o",
        Kind.CONTENT_CHANGED,
        input_id=["a"],
        output_id=["a"],
        affected_output_columns=["height"],
        column_change="changed",
    )
    assert "row.column_change" in check_row_detail_row(row)


def test_a_whole_record_kind_names_no_columns():
    row = row_detail_row(
        "r1.o", Kind.DROPPED, input_id=["a"], affected_output_columns=["height"]
    )
    assert "row.affected_output_columns" in check_row_detail_row(row)


def test_an_empty_column_list_says_nothing():
    """Null already means every column, so an empty list is a mistake."""
    row = row_detail_row(
        "r1.o",
        Kind.CONTENT_CHANGED,
        input_id=["a"],
        output_id=["a"],
        affected_output_columns=[],
        detail="moved",
    )
    assert "row.affected_output_columns" in check_row_detail_row(row)


def test_a_content_change_says_what_changed():
    """Naming no columns and giving no detail claims something moved without saying
    what."""
    row = row_detail_row("r1.o", Kind.CONTENT_CHANGED, input_id=["a"], output_id=["a"])
    assert "row.empty_content_change" in check_row_detail_row(row)


@pytest.mark.parametrize(
    "columns,detail",
    [
        (["height"], None),
        (None, "geometry snapped to grid"),
        (["height"], "from lidar"),
    ],
)
def test_either_columns_or_detail_satisfies_a_content_change(columns, detail):
    row = row_detail_row(
        "r1.o",
        Kind.CONTENT_CHANGED,
        input_id=["a"],
        output_id=["a"],
        affected_output_columns=columns,
        detail=detail,
    )
    assert check_row_detail_row(row) == []


def test_a_flag_needs_neither_columns_nor_detail():
    """The operation a flag belongs to is the finding, so the entry can be bare."""
    row = row_detail_row("r1.o", Kind.FLAGGED, input_id=["a"], output_id=["a"])
    assert check_row_detail_row(row) == []


def test_an_unknown_kind_stops_the_check():
    assert check_row_detail_row({"kind": "mangled"}) == ["row.kind"]


# Row detail against the operations it names


def test_an_entry_must_name_an_operation_that_exists():
    rows = [row_detail_row("r1.ghost", Kind.DROPPED, input_id=["a"])]
    problems = check_row_detail(rows, [an_op("o")], Problems())
    assert any(p.rule == "row.op_id" for p in problems.items)


def test_a_physical_operation_carries_no_entries():
    read = an_op("read", physical_source="s3://in")
    rows = [row_detail_row(read.op_id, Kind.DROPPED, input_id=["a"])]
    problems = check_row_detail(rows, [read], Problems())
    assert any("touches no record contents" in p.message for p in problems.items)


def test_an_operation_with_entries_says_so():
    op = an_op("o", has_row_detail=False)
    rows = [row_detail_row(op.op_id, Kind.DROPPED, input_id=["a"])]
    problems = check_row_detail(rows, [op], Problems())
    assert any(p.rule == "op.has_row_detail" for p in problems.items)


def test_the_cross_table_check_runs_the_per_entry_rules():
    """A malformed entry gets reported through the operation it belongs to, so the
    two halves of the row rules do not have to be called separately."""
    op = an_op("o", has_row_detail=True)
    rows = [row_detail_row(op.op_id, Kind.DROPPED, input_id=["a"], output_id=["a"])]
    problems = check_row_detail(rows, [op], Problems())
    assert any(p.rule == "row.output_id" for p in problems.items)


def test_sound_row_detail_finds_nothing_to_report():
    op = an_op("drop_invalid", has_row_detail=True)
    rows = [
        row_detail_row(op.op_id, Kind.DROPPED, input_id=["a"], detail="bad geometry"),
        row_detail_row(op.op_id, Kind.DROPPED, input_id=["b"], detail="bad geometry"),
    ]
    assert check_row_detail(rows, [op], Problems()).items == []


# Grain


def test_a_grain_change_is_visible():
    """Absence means unchanged, and that claim says nothing once the output records
    stop being the same kind of thing as the inputs."""
    records = an_op("records", output_key_columns=["id"])
    counts = an_op("counts", output_key_columns=["category"])
    same = an_op("tidy", output_key_columns=["id"])
    assert changes_grain(counts, [records]) is True
    assert changes_grain(same, [records]) is False


# Which input an entry came in on


def test_one_input_needs_no_pointer():
    row = row_detail_row("r1.o", Kind.DROPPED, input_id=["a"])
    assert check_row_detail_row(row, input_op_ids=["r1.up"]) == []


def test_several_inputs_need_a_pointer():
    row = row_detail_row("r1.o", Kind.DROPPED, input_id=["a"])
    broken = check_row_detail_row(row, input_op_ids=["r1.feed", "r1.corpus"])
    assert "row.input_op_id" in broken


def test_the_pointer_must_name_an_input_the_operation_consumed():
    row = row_detail_row(
        "r1.o", Kind.DROPPED, input_id=["a"], input_op_id="r1.elsewhere"
    )
    broken = check_row_detail_row(row, input_op_ids=["r1.feed", "r1.corpus"])
    assert "row.input_op_id" in broken


def test_a_mint_has_no_input_to_attribute():
    row = row_detail_row("r1.o", Kind.MINTED, output_id=["a"], input_op_id="r1.feed")
    assert "row.input_op_id" in check_row_detail_row(row)


# The run


def test_a_run_records_the_four_shapes():
    run = Run("20260908T120000", code_version="a1b2c3d", output_key_columns=["id"])
    read = run.read("read_land", "Read staged land", "s3://in/land/")
    clean = run.transform(
        "drop_invalid", "Remove invalid geometry", inputs=[read], has_row_detail=True
    )
    write = run.write("write_land", "Write cleaned land", "s3://out/land/", input=clean)
    mirror = run.copy(
        "mirror_land",
        "Mirror to archive",
        "s3://out/land/",
        "s3://archive/land/",
        input=clean,
    )

    assert [op_type_of(o) for o in run] == [
        OpType.READ,
        OpType.TRANSFORM,
        OpType.WRITE,
        OpType.COPY,
    ]
    assert write.input_op_ids == (clean.op_id,)
    assert mirror.code_version == "a1b2c3d"
    assert clean.output_key_columns == ("id",)


def test_a_run_fills_in_where_the_call_came_from():
    run = Run("r1")
    op = run.transform("t", "does a thing", inputs=[])
    assert op.code_ref.endswith("test_a_run_fills_in_where_the_call_came_from")


def test_a_run_catches_a_repeated_key():
    """Two invocations of the same code in one run collide unless each says which
    invocation it is."""
    run = Run("r1")
    run.transform("match", "match against corpus", inputs=[])
    run.transform("match", "match against corpus", inputs=[])
    assert any(p.rule == "op.op_key" for p in run.problems.items)


def test_a_run_validates_its_own_table():
    run = Run("r1")
    read = run.read("read", "Read input", "s3://in/")
    clean = run.transform("clean", "Tidy it up", inputs=[read])
    run.write("write", "Write output", "s3://out/", input=clean)
    assert run.validate().items == []
    assert len(run.operation_rows()) == 3
