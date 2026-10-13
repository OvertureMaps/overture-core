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
    IdentityCaptureStatus,
    Kind,
    Op,
    OpType,
    Problems,
    Run,
    check_acyclic,
    check_operations,
    check_row_detail,
    check_row_detail_row,
    keeps_grain_of,
    op_id_for,
    op_row,
    op_type_of,
    row_detail_row,
    resolve_input_op_id,
    slug,
)

AT = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)
STORAGE_OPERATIONS = [("write", ("out",)), ("copy", ("in", "out"))]


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
    assert row["identity_capture_status"] == "partial"
    assert type(row["identity_capture_status"]) is str
    assert "has_row_detail" not in row


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


@pytest.mark.parametrize(
    "source,dest",
    [("in", None), (None, "out"), ("in", "out")],
)
def test_only_a_transform_can_have_row_detail(source, dest):
    op = an_op(
        "io", physical_source=source, physical_dest=dest, output_key_columns=["id"]
    )
    rows = [row_detail_row(op.op_id, Kind.MINTED, output_id=["a"])]
    problems = check_row_detail(rows, [op], Problems())
    assert any("touches no record contents" in p.message for p in problems.items)


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
    drop = an_op(
        "drop_invalid",
        output_key_columns=["id"],
        input_op_ids=[read.op_id],
    )
    write = an_op(
        "write_land",
        physical_dest="s3://out",
        output_key_columns=["id"],
        input_op_ids=[drop.op_id],
        passthrough_input_op_ids=[drop.op_id],
    )
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
        (Kind.DERIVED_FROM, ["a"], ["a"]),
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
    """Use null for unrecorded column detail."""
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


# What an absent entry is allowed to mean


def test_one_input_keyed_like_the_output_passes_through_by_default():
    """Filters and value rewrites are the bulk of a pipeline, so they should not
    have to restate this on every call."""
    run = Run("r1")
    read = run.read("read", "Read input", "s3://in/", output_key_columns=["id"])
    op = run.transform(
        "drop_invalid",
        "Remove invalid geometry",
        inputs=[read],
        output_key_columns=["id"],
    )
    assert op.passthrough_input_op_ids == (read.op_id,)


def test_an_empty_list_says_nothing_survives():
    """A group-by keyed on the column it grouped is indistinguishable from a filter
    by key columns alone, so it says so outright."""
    run = Run("r1")
    read = run.read("read", "Read input", "s3://in/", output_key_columns=["id"])
    op = run.transform(
        "count_per_id",
        "One row per id",
        inputs=[read],
        output_key_columns=["id"],
        passthrough=[],
    )
    assert op.passthrough_input_op_ids == ()


def test_a_rekeyed_output_defaults_to_nothing():
    run = Run("r1")
    read = run.read("read", "Read input", "s3://in/", output_key_columns=["id"])
    op = run.transform(
        "totals", "Total per category", inputs=[read], output_key_columns=["category"]
    )
    assert op.passthrough_input_op_ids == ()


def test_several_inputs_default_to_nothing():
    """Two inputs keyed alike play different roles often enough that a guess is
    unsafe, so the author says which one continues."""
    run = Run("r1")
    feed = run.read(
        "read_feed", "Read the feed", "s3://feed/", output_key_columns=["id"]
    )
    corpus = run.read(
        "read_corpus", "Read the corpus", "s3://corpus/", output_key_columns=["id"]
    )
    op = run.transform(
        "match",
        "Match feed to corpus",
        inputs=[feed, corpus],
        output_key_columns=["id"],
    )
    assert op.passthrough_input_op_ids == ()


def test_a_filter_declares_its_one_input():
    run = Run("r1")
    read = run.read("read", "Read input", "s3://in/", output_key_columns=["id"])
    op = run.transform(
        "drop_invalid", "Remove invalid geometry", inputs=[read], passthrough=[read]
    )
    assert op.passthrough_input_op_ids == (read.op_id,)
    assert run.validate().items == []


def test_a_union_declares_both_inputs():
    """One primary input could not express this, which is why the column is a
    list."""
    run = Run("r1")
    a = run.read("read_a", "Read A", "s3://a/", output_key_columns=["id"])
    b = run.read("read_b", "Read B", "s3://b/", output_key_columns=["id"])
    op = run.transform(
        "union", "Concatenate A and B", inputs=[a, b], passthrough=[a, b]
    )
    assert op.passthrough_input_op_ids == (a.op_id, b.op_id)


def test_an_enrichment_declares_only_its_base_table():
    """Both inputs are keyed by id here, so nothing about the keys could have told
    a reader that reference records stay out of the output."""
    run = Run("r1")
    feed = run.read(
        "read_feed", "Read the feed", "s3://feed/", output_key_columns=["id"]
    )
    corpus = run.read(
        "read_corpus", "Read the corpus", "s3://corpus/", output_key_columns=["id"]
    )
    op = run.transform(
        "match",
        "Match the feed against the corpus",
        inputs=[feed, corpus],
        passthrough=[feed],
        output_key_columns=["id"],
    )
    assert op.passthrough_input_op_ids == (feed.op_id,)
    assert run.validate().items == []


def test_a_passthrough_input_must_be_an_input():
    bad = an_op("o", input_op_ids=["r1.a"], passthrough_input_op_ids=["r1.b"])
    problems = Problems()
    check_operations([an_op("a"), bad], problems)
    assert any(p.rule == "op.passthrough_input_op_ids" for p in problems.items)


def test_a_passthrough_input_is_named_once():
    bad = an_op("o", input_op_ids=["r1.a"], passthrough_input_op_ids=["r1.a", "r1.a"])
    problems = Problems()
    check_operations([an_op("a"), bad], problems)
    assert any("names an input twice" in p.message for p in problems.items)


def test_a_write_passes_its_input_through():
    """A write holds no row detail, so absence is the only reading available and
    the helper fills it in."""
    run = Run("r1")
    read = run.read("read", "Read input", "s3://in/", output_key_columns=["id"])
    write = run.write("write", "Write it out", "s3://out/", input=read)
    assert write.passthrough_input_op_ids == (read.op_id,)


def test_a_write_that_claims_otherwise_is_reported():
    bad = an_op("w", physical_dest="s3://out", input_op_ids=["r1.a"])
    problems = Problems()
    check_operations([an_op("a"), bad], problems)
    assert any("carries every record it handles" in p.message for p in problems.items)


def test_a_read_has_no_input_to_pass_through_from():
    bad = an_op("r", physical_source="s3://in", passthrough_input_op_ids=["r1.a"])
    problems = Problems()
    check_operations([an_op("a"), bad], problems)
    assert any(p.rule == "op.passthrough_input_op_ids" for p in problems.items)


# Key columns


def test_an_output_id_needs_an_output_key():
    op = an_op("create")
    rows = [row_detail_row(op.op_id, Kind.MINTED, output_id=["a"])]
    problems = check_row_detail(rows, [op], Problems())
    assert any(p.rule == "op.output_key_columns" for p in problems.items)


@pytest.mark.parametrize("keys", [[], ["id", "id"], ["id", " "]])
def test_a_malformed_key_is_reported(keys):
    problems = Problems()
    check_operations([an_op("o", output_key_columns=keys)], problems)
    assert any(p.rule == "op.output_key_columns" for p in problems.items)


def test_an_output_id_has_as_many_parts_as_the_key():
    op = an_op("o", output_key_columns=["provider", "id"])
    row = row_detail_row(op.op_id, Kind.MINTED, output_id=["only-one"])
    assert "row.output_id" in check_row_detail_row(
        row, output_key_columns=op.output_key_columns
    )


def test_an_input_id_is_read_against_the_operation_that_made_it():
    """An id on the way in belongs to whichever operation emitted that record, so
    its width comes from that operation's key rather than this one's."""
    source = an_op("source", output_key_columns=["provider", "id"])
    consumer = an_op(
        "consumer",
        output_key_columns=["id"],
        input_op_ids=[source.op_id],
    )
    rows = [row_detail_row(consumer.op_id, Kind.DROPPED, input_id=["just-an-id"])]
    problems = check_row_detail(rows, [source, consumer], Problems())
    assert any(p.rule == "row.input_id" for p in problems.items)


def test_a_composite_key_entry_checks_out():
    source = an_op("source", output_key_columns=["provider", "id"])
    consumer = an_op(
        "consumer",
        output_key_columns=["provider", "id"],
        input_op_ids=[source.op_id],
    )
    rows = [
        row_detail_row(
            consumer.op_id, Kind.DROPPED, input_id=["tomtom", "42"], detail="stale"
        )
    ]
    assert check_row_detail(rows, [source, consumer], Problems()).items == []


def test_an_operation_consuming_nothing_has_no_input_records():
    row = row_detail_row("r1.o", Kind.DROPPED, input_id=["a"])
    assert "row.input_id" in check_row_detail_row(row, input_op_ids=[])


# Row detail against the operations it names


def test_an_entry_must_name_an_operation_that_exists():
    rows = [row_detail_row("r1.ghost", Kind.DROPPED, input_id=["a"])]
    problems = check_row_detail(rows, [an_op("o")], Problems())
    assert any(p.rule == "row.op_id" for p in problems.items)


def test_partial_capture_can_have_row_detail():
    op = an_op("create", output_key_columns=["id"])
    rows = [row_detail_row(op.op_id, Kind.MINTED, output_id=["a"])]
    assert op.identity_capture_status is IdentityCaptureStatus.PARTIAL
    assert check_row_detail(rows, [op], Problems()).items == []


def test_the_cross_table_check_runs_the_per_entry_rules():
    """A malformed entry gets reported through the operation it belongs to, so the
    two halves of the row rules do not have to be called separately."""
    op = an_op("o")
    rows = [row_detail_row(op.op_id, Kind.DROPPED, input_id=["a"], output_id=["a"])]
    problems = check_row_detail(rows, [op], Problems())
    assert any(p.rule == "row.output_id" for p in problems.items)


def test_sound_row_detail_finds_nothing_to_report():
    source = an_op("read_land", physical_source="s3://in", output_key_columns=["id"])
    op = an_op(
        "drop_invalid",
        output_key_columns=["id"],
        input_op_ids=[source.op_id],
    )
    rows = [
        row_detail_row(op.op_id, Kind.DROPPED, input_id=["a"], detail="bad geometry"),
        row_detail_row(op.op_id, Kind.DROPPED, input_id=["b"], detail="bad geometry"),
    ]
    assert check_row_detail(rows, [source, op], Problems()).items == []


# Grain


def test_an_aggregate_keeps_nobody_grain():
    """Grouping by category changes what one record represents."""
    records = an_op("records", output_key_columns=["id"])
    counts = an_op("counts", output_key_columns=["category"])
    assert keeps_grain_of(counts, [records]) == ()


def test_an_enrichment_keeps_the_grain_of_its_base_table():
    """One input being keyed differently says nothing about the others, so this
    reports which inputs match rather than whether any differ."""
    base = an_op("base", output_key_columns=["id"])
    reference = an_op("reference", output_key_columns=["ref_id"])
    enriched = an_op("enriched", output_key_columns=["id"])
    assert keeps_grain_of(enriched, [base, reference]) == (base,)


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
        "drop_invalid",
        "Remove invalid geometry",
        inputs=[read],
        passthrough=[read],
        identity_capture_status=IdentityCaptureStatus.COMPLETE,
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


@pytest.mark.parametrize("status", [None, "", "unknown", True])
def test_identity_capture_status_must_be_partial_or_complete(status):
    problems = check_operations(
        [an_op("o", identity_capture_status=status)], Problems()
    )
    assert any(p.rule == "op.identity_capture_status" for p in problems.items)


@pytest.mark.parametrize("status", [*IdentityCaptureStatus, "partial", "complete"])
def test_valid_capture_status_is_serialized_as_a_string(status):
    op = an_op("o", identity_capture_status=status, output_key_columns=["id"])
    assert check_operations([op], Problems()).items == []
    assert type(op_row(op)["identity_capture_status"]) is str
    assert op_row(op)["identity_capture_status"] == status


def test_complete_capture_needs_known_output_keys():
    problems = check_operations(
        [an_op("o", identity_capture_status=IdentityCaptureStatus.COMPLETE)], Problems()
    )
    assert any(p.rule == "op.output_key_columns" for p in problems.items)


def test_complete_capture_needs_known_input_keys():
    source = an_op("source")
    op = an_op(
        "o",
        input_op_ids=[source.op_id],
        output_key_columns=["id"],
        identity_capture_status=IdentityCaptureStatus.COMPLETE,
    )
    problems = check_operations([source, op], Problems())
    assert any(p.rule == "op.input_key_columns" for p in problems.items)


def test_a_complete_filter_with_no_rejects_needs_no_entries():
    run = Run("r1", output_key_columns=["id"])
    source = run.read("read", "Read records", "input")
    run.transform(
        "filter",
        "Keep valid records",
        inputs=[source],
        passthrough=[source],
        identity_capture_status=IdentityCaptureStatus.COMPLETE,
    )
    assert run.validate().items == []
    assert check_row_detail([], run.ops, Problems()).items == []


@pytest.mark.parametrize("method,locations", STORAGE_OPERATIONS)
@pytest.mark.parametrize("use_id", [False, True])
def test_writes_and_copies_inherit_the_actual_input_keys(method, locations, use_id):
    run = Run("r1", output_key_columns=["id"])
    source = run.read(
        "source", "Read categories", "in", output_key_columns=["category"]
    )
    op = getattr(run, method)(
        "save",
        "Save categories",
        *locations,
        input=source.op_id if use_id else source,
        identity_capture_status=IdentityCaptureStatus.COMPLETE,
    )
    assert op.output_key_columns == source.output_key_columns
    assert op.passthrough_input_op_ids == (source.op_id,)
    assert run.validate().items == []


@pytest.mark.parametrize("method,locations", STORAGE_OPERATIONS)
def test_writes_and_copies_preserve_an_unknown_input_key(method, locations):
    source = an_op("source")
    run = Run("r1", output_key_columns=["id"])
    op = getattr(run, method)("save", "Save records", *locations, input=source)
    assert op.output_key_columns is None
    assert check_operations([source, op], Problems()).items == []


@pytest.mark.parametrize("method,locations", STORAGE_OPERATIONS)
def test_writes_and_copies_reject_an_explicit_key_change(method, locations):
    run = Run("r1", output_key_columns=["id"])
    source = run.read("source", "Read records", "in")
    getattr(run, method)(
        "save",
        "Save records",
        *locations,
        input=source,
        output_key_columns=["other_id"],
    )
    assert any(p.rule == "op.output_key_columns" for p in run.validate().items)


def test_a_drop_needs_input_keys_but_no_output_key():
    source = an_op("source", output_key_columns=["id"])
    op = an_op("drop", input_op_ids=[source.op_id])
    rows = [row_detail_row(op.op_id, Kind.DROPPED, input_id=["a"])]
    assert check_row_detail(rows, [source, op], Problems()).items == []

    unknown_source = an_op("source")
    problems = check_row_detail(rows, [unknown_source, op], Problems())
    assert any(p.rule == "op.input_key_columns" for p in problems.items)


@pytest.mark.parametrize("bad_id", ["abc", [], 1, [None], [["a"]]])
def test_bad_id_shapes_are_reported_without_crashing_grouped_checks(bad_id):
    source = an_op("source", output_key_columns=["id"])
    op = an_op("drop", input_op_ids=[source.op_id])
    row = row_detail_row(op.op_id, Kind.DROPPED, input_id=["a"])
    row["input_id"] = bad_id
    problems = check_row_detail([row], [source, op], Problems())
    assert any(p.rule == "row.input_id" for p in problems.items)


def test_a_bad_input_pointer_is_reported_without_crashing_grouped_checks():
    source = an_op("source", output_key_columns=["id"])
    op = an_op("drop", input_op_ids=[source.op_id])
    row = row_detail_row(op.op_id, Kind.DROPPED, input_id=["a"])
    row["input_op_id"] = []
    problems = check_row_detail([row], [source, op], Problems())
    assert any(p.rule == "row.input_op_id" for p in problems.items)


@pytest.mark.parametrize(
    "kind", [Kind.DERIVED_FROM, Kind.CONTENT_CHANGED, Kind.FLAGGED]
)
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("multiple_inputs", [False, True])
def test_a_drop_conflicts_with_same_id_survival(kind, reverse, multiple_inputs):
    source = an_op("source", output_key_columns=["id"])
    other = an_op("other", output_key_columns=["id"])
    inputs = [source.op_id, other.op_id] if multiple_inputs else [source.op_id]
    op = an_op("o", input_op_ids=inputs, output_key_columns=["id"])
    rows = [
        row_detail_row(op.op_id, Kind.DROPPED, input_id=["a"]),
        row_detail_row(
            op.op_id, kind, input_id=["a"], output_id=["a"], detail="changed"
        ),
    ]
    if multiple_inputs:
        for row in rows:
            row["input_op_id"] = source.op_id
    rows[1]["output_id"] = ("a",)
    problems = check_row_detail(
        reversed(rows) if reverse else iter(rows), [source, other, op], Problems()
    )
    assert [p.rule for p in problems.items] == ["row.conflicting_outcomes"]


def test_a_drop_can_also_contribute_to_a_different_id():
    source = an_op("source", output_key_columns=["id"])
    op = an_op("o", input_op_ids=[source.op_id], output_key_columns=["id"])
    rows = [
        row_detail_row(op.op_id, Kind.DROPPED, input_id=["a"]),
        row_detail_row(op.op_id, Kind.DERIVED_FROM, input_id=["a"], output_id=["b"]),
    ]
    assert check_row_detail(rows, [source, op], Problems()).items == []


@pytest.mark.parametrize("separate_inputs", [False, True])
def test_conflict_groups_keep_operations_and_inputs_separate(separate_inputs):
    sources = [an_op(name, output_key_columns=["id"]) for name in ("a", "b")]
    inputs = [source.op_id for source in sources]
    ops = [
        an_op(name, input_op_ids=inputs, output_key_columns=["id"])
        for name in ("first", "second")
    ]
    rows = [
        row_detail_row(
            ops[0].op_id, Kind.DROPPED, input_op_id=inputs[0], input_id=["42"]
        ),
        row_detail_row(
            ops[0 if separate_inputs else 1].op_id,
            Kind.FLAGGED,
            input_op_id=inputs[1 if separate_inputs else 0],
            input_id=["42"],
            output_id=["42"],
        ),
    ]
    assert check_row_detail(rows, sources + ops, Problems()).items == []


@pytest.mark.parametrize("multiple_inputs", [False, True])
def test_input_resolution_handles_single_and_multiple_inputs(multiple_inputs):
    inputs = ["r1.a", "r1.b"] if multiple_inputs else ["r1.a"]
    op = an_op("o", input_op_ids=inputs)
    row = row_detail_row(
        op.op_id,
        Kind.DROPPED,
        input_op_id=inputs[0] if multiple_inputs else None,
        input_id=["42"],
    )
    assert resolve_input_op_id(row, op) == inputs[0]
    mint = row_detail_row(op.op_id, Kind.MINTED, output_id=["42"])
    assert resolve_input_op_id(mint, op) is None
