"""Run small record-reading examples against known links and missing evidence."""

from datetime import datetime, timezone

import pytest

from overture_cairn import (
    IdentityCaptureStatus,
    InvariantViolation,
    Kind,
    Op,
    op_row,
    row_detail_row,
)
from record_reader import RecordReader

AT = datetime(2026, 9, 13, tzinfo=timezone.utc)


def operation(name, inputs=(), passthrough=(), *, complete=True, **kwargs):
    return Op(
        op_id=f"example.{name}",
        run_id="example",
        op_key=name,
        description=f"Run {name}.",
        timestamp=AT,
        output_key_columns=("id",),
        input_op_ids=tuple(op.op_id for op in inputs),
        passthrough_input_op_ids=tuple(op.op_id for op in passthrough),
        identity_capture_status=(
            IdentityCaptureStatus.COMPLETE
            if complete
            else IdentityCaptureStatus.PARTIAL
        ),
        **kwargs,
    )


def read(name="source"):
    return operation(name, physical_source=f"memory://{name}")


def record(op, identity="A"):
    return (op.op_id, (identity,))


def entry(op, kind, input_id=None, output_id=None, *, source=None, **kwargs):
    return row_detail_row(
        op.op_id,
        kind,
        input_id=None if input_id is None else [input_id],
        output_id=None if output_id is None else [output_id],
        input_op_id=None if source is None else source.op_id,
        **kwargs,
    )


def test_complete_filters_compose_without_rejects_or_retained_intermediates():
    source = read()
    first = operation("first_filter", [source], [source])
    second = operation("second_filter", [first], [first])
    reader = RecordReader([source, first, second])

    forward = reader.forward(first.op_id, record(source))
    assert forward.links == {record(first): "inferred"}
    assert not forward.gaps
    next_forward = reader.forward(second.op_id, next(iter(forward.links)))
    assert next_forward.links == {record(second): "inferred"}
    assert not next_forward.gaps

    backward = reader.backward(record(second))
    assert backward.links == {record(first): "inferred"}
    assert not backward.gaps
    previous = reader.backward(next(iter(backward.links)))
    assert previous.links == {record(source): "inferred"}
    assert not previous.gaps
    origin = reader.backward(next(iter(previous.links)))
    assert origin.links == {}
    assert origin.facts == {"Physical source: memory://source"}
    assert origin.gaps == {(source.op_id, None, "External version link is missing.")}


def test_partial_filter_reports_the_drop_and_unknown_remaining_fates():
    source = read()
    filtered = operation("filter", [source], [source], complete=False)
    reader = RecordReader([source, filtered], [entry(filtered, Kind.DROPPED, "A")])
    dropped = reader.forward(filtered.op_id, record(source))
    assert dropped.links == {}
    assert dropped.facts == {"Dropped under the input identity."}
    assert dropped.gaps == {(filtered.op_id, source.op_id, "Capture is partial.")}
    survivor = reader.forward(filtered.op_id, record(source, "B"))
    assert survivor.links == {}
    assert survivor.gaps == dropped.gaps
    backward = reader.backward(record(filtered, "B"))
    assert backward.links == {}
    assert backward.gaps == {(filtered.op_id, None, "Capture is partial.")}


def test_partial_links_keep_composing_alongside_the_gap():
    source = read()
    split = operation("split", [source], [source], complete=False)
    filtered = operation("filter", [split], [split])
    reader = RecordReader(
        [source, split, filtered], [entry(split, Kind.DERIVED_FROM, "A", "B")]
    )
    known = reader.forward(split.op_id, record(source))
    assert known.links == {record(split, "B"): "recorded"}
    assert known.gaps == {(split.op_id, source.op_id, "Capture is partial.")}
    continued = reader.forward(filtered.op_id, next(iter(known.links)))
    assert continued.links == {record(filtered, "B"): "inferred"}
    assert known.gaps | continued.gaps == known.gaps
    backward = reader.backward(record(split, "B"))
    assert backward.links == {record(source): "recorded"}
    assert backward.gaps == {(split.op_id, None, "Capture is partial.")}


def test_overlapping_base_and_reference_ids_do_not_prove_base_membership():
    base, reference = read("base"), read("reference")
    enriched = operation("enrich", [base, reference], [base])
    rows = [
        entry(
            enriched,
            Kind.DERIVED_FROM,
            "A",
            "A",
            source=reference,
            affected_output_columns=["height"],
        )
    ]
    reader = RecordReader([base, reference, enriched], rows)
    assert reader.forward(enriched.op_id, record(base)).links == {
        record(enriched): "inferred"
    }
    assert reader.forward(enriched.op_id, record(reference)).links == {
        record(enriched): "recorded"
    }
    unused = reader.forward(enriched.op_id, record(reference, "unused"))
    assert unused.links == {}
    assert unused.facts == {"No direct contribution."}
    assert not unused.gaps
    backward = reader.backward(record(enriched))
    assert backward.links == {record(reference): "recorded"}
    assert backward.gaps == {
        (enriched.op_id, base.op_id, "Input membership is unknown.")
    }

    retained = RecordReader(
        [base, reference, enriched], rows, {record(base): True}
    ).backward(record(enriched))
    assert retained.links == {
        record(base): "inferred",
        record(reference): "recorded",
    }
    assert not retained.gaps
    absent = RecordReader(
        [base, reference, enriched], rows, {record(base): False}
    ).backward(record(enriched))
    assert absent.links == {record(reference): "recorded"}
    assert not absent.gaps


def test_record_only_merge_retains_an_id_without_a_content_change_entry():
    source = read()
    merged = operation("merge", [source])
    rows = [entry(merged, Kind.DERIVED_FROM, identity, "A") for identity in ("A", "B")]
    reader = RecordReader([source, merged], rows)
    backward = reader.backward(record(merged))
    assert backward.links == {
        record(source): "recorded",
        record(source, "B"): "recorded",
    }
    assert not backward.gaps
    for identity in ("A", "B"):
        assert reader.forward(merged.op_id, record(source, identity)).links == {
            record(merged): "recorded"
        }
    assert all(row["affected_output_columns"] is None for row in rows)


def test_retained_split_and_duplicate_kinds_count_endpoints_once():
    source = read()
    split = operation("split", [source], [source])
    reader = RecordReader(
        [source, split],
        [
            entry(split, Kind.DERIVED_FROM, "A", "A"),
            entry(split, Kind.DERIVED_FROM, "A", "B"),
            entry(split, Kind.FLAGGED, "A", "A"),
            entry(split, Kind.CONTENT_CHANGED, "A", "A", detail="Trimmed names."),
        ],
        membership={record(source, "B"): False},
    )
    assert reader.forward(split.op_id, record(source)).links == {
        record(split): "recorded",
        record(split, "B"): "recorded",
    }
    for identity in ("A", "B"):
        answer = reader.backward(record(split, identity))
        assert answer.links == {record(source): "recorded"}
        assert not answer.gaps


def test_a_rebind_does_not_invent_a_retained_split_link():
    source = read()
    split = operation("split", [source], [source])
    rows = [entry(split, Kind.DERIVED_FROM, "A", "B")]
    reader = RecordReader([source, split], rows)
    answer = reader.forward(split.op_id, record(source))
    assert answer.links == {record(split, "B"): "recorded"}
    assert not answer.gaps
    assert reader.backward(record(split, "B")).links == {record(source): "recorded"}


@pytest.mark.parametrize("kind", [Kind.CONTENT_CHANGED, Kind.FLAGGED])
def test_a_survival_fact_does_not_replace_a_complete_splits_retained_link(kind):
    source = read()
    split = operation("split", [source], [source])
    rows = [
        entry(split, Kind.DERIVED_FROM, "A", "B"),
        entry(split, kind, "A", "A", detail="Recorded survival."),
    ]
    with pytest.raises(InvariantViolation, match="row.missing_retained_link"):
        RecordReader([source, split], rows)


@pytest.mark.parametrize("kind", [Kind.CONTENT_CHANGED, Kind.FLAGGED])
def test_a_partial_split_keeps_known_survival_even_without_its_contribution_link(kind):
    source = read()
    split = operation("split", [source], [source], complete=False)
    reader = RecordReader(
        [source, split],
        [
            entry(split, Kind.DERIVED_FROM, "A", "B"),
            entry(split, kind, "A", "A", detail="Recorded survival."),
        ],
    )
    answer = reader.forward(split.op_id, record(source))
    assert answer.links == {
        record(split): "recorded",
        record(split, "B"): "recorded",
    }
    assert answer.gaps == {(split.op_id, source.op_id, "Capture is partial.")}
    for identity in ("A", "B"):
        backward = reader.backward(record(split, identity))
        assert backward.links == {record(source): "recorded"}
        assert backward.gaps == {(split.op_id, None, "Capture is partial.")}


def test_a_drop_can_contribute_under_another_id():
    source = read()
    merged = operation("merge", [source], [source])
    reader = RecordReader(
        [source, merged],
        [
            entry(merged, Kind.DROPPED, "A"),
            entry(merged, Kind.DERIVED_FROM, "A", "B"),
        ],
    )
    answer = reader.forward(merged.op_id, record(source))
    assert answer.links == {record(merged, "B"): "recorded"}
    assert answer.facts == {"Dropped under the input identity."}
    assert not answer.gaps


@pytest.mark.parametrize("kind", [Kind.DROPPED, Kind.DERIVED_FROM])
def test_backward_excludes_a_dropped_or_rebound_pass_through_candidate(kind):
    left, right = read("left"), read("right")
    union = operation("union", [left, right], [left, right])
    rows = [
        entry(
            union,
            kind,
            "A",
            None if kind is Kind.DROPPED else "B",
            source=left,
        )
    ]
    answer = RecordReader([left, right, union], rows).backward(record(union))
    assert answer.links == {record(right): "inferred"}
    assert not answer.gaps


def test_union_requires_membership_evidence_for_each_input():
    left, right = read("left"), read("right")
    union = operation("union", [left, right], [left, right])
    operations = [left, right, union]
    ambiguous = RecordReader(operations).backward(record(union))
    assert ambiguous.links == {}
    assert ambiguous.gaps == {
        (union.op_id, left.op_id, "Input membership is unknown."),
        (union.op_id, right.op_id, "Input membership is unknown."),
    }
    reader = RecordReader(
        operations,
        membership={
            record(left): True,
            record(right): False,
            record(left, "B"): False,
            record(right, "B"): True,
        },
    )
    for identity, source in (("A", left), ("B", right)):
        answer = reader.backward(record(union, identity))
        assert answer.links == {record(source, identity): "inferred"}
        assert not answer.gaps
    sole_candidate = RecordReader(
        operations, membership={record(right): False}
    ).backward(record(union))
    assert sole_candidate.links == {record(left): "inferred"}
    assert not sole_candidate.gaps


def test_one_present_union_input_does_not_prove_the_other_is_absent():
    left, right = read("left"), read("right")
    union = operation("union", [left, right], [left, right])
    answer = RecordReader(
        [left, right, union], membership={record(left): True}
    ).backward(record(union))
    assert answer.links == {record(left): "inferred"}
    assert answer.gaps == {(union.op_id, right.op_id, "Input membership is unknown.")}


@pytest.mark.parametrize("kind", [Kind.CONTENT_CHANGED, Kind.FLAGGED])
def test_explicit_same_id_survival_does_not_claim_unchanged_values(kind):
    source = read()
    changed = operation("normalize", [source], [source])
    reader = RecordReader(
        [source, changed],
        [entry(changed, kind, "A", "A", detail="Trimmed names.")],
    )
    answer = reader.forward(changed.op_id, record(source))
    assert answer.links == {record(changed): "recorded"}
    assert answer.facts == set()
    assert not answer.gaps
    assert reader.backward(record(changed)).links == {record(source): "recorded"}


def test_an_explicit_mint_does_not_establish_pass_through_membership():
    source = read()
    transform = operation("transform", [source], [source])
    answer = RecordReader(
        [source, transform], [entry(transform, Kind.MINTED, output_id="A")]
    ).backward(record(transform))
    assert answer.links == {}
    assert answer.facts == {"Minted by this operation."}
    assert answer.gaps == {
        (transform.op_id, source.op_id, "Input membership is unknown.")
    }


def test_matching_mutable_paths_do_not_link_a_read_to_a_write():
    source = read()
    write = operation("write", [source], [source], physical_dest="memory://latest")
    reread = operation("reread", physical_source="memory://latest")
    reader = RecordReader([source, write, reread])
    assert reader.backward(record(write)).links == {record(source): "inferred"}
    answer = reader.backward(record(reread))
    assert answer.links == {}
    assert answer.facts == {"Physical source: memory://latest"}
    assert answer.gaps == {(reread.op_id, None, "External version link is missing.")}


def test_older_serialized_operations_default_to_partial():
    source = read()
    transform = operation("transform", [source], [source])
    old_row = op_row(transform)
    del old_row["identity_capture_status"]
    old_row["has_row_detail"] = True
    reader = RecordReader([op_row(source), old_row])
    answer = reader.forward(transform.op_id, record(source))
    assert answer.links == {}
    assert answer.gaps == {(transform.op_id, source.op_id, "Capture is partial.")}
    complete = RecordReader([op_row(source), op_row(transform)])
    assert complete.forward(transform.op_id, record(source)).links == {
        record(transform): "inferred"
    }


@pytest.mark.parametrize(
    "kind", [Kind.DERIVED_FROM, Kind.CONTENT_CHANGED, Kind.FLAGGED]
)
def test_grouped_conflicts_are_errors_not_unknown_links(kind):
    source = read()
    transform = operation("transform", [source], [source])
    with pytest.raises(InvariantViolation, match="row.conflicting_outcomes"):
        RecordReader(
            [source, transform],
            [
                entry(transform, Kind.DROPPED, "A"),
                entry(transform, kind, "A", "A", detail="Recorded survival."),
            ],
        )


def test_membership_cannot_contradict_recorded_links():
    source = read()
    transform = operation("transform", [source])
    with pytest.raises(ValueError, match="known absent"):
        RecordReader(
            [source, transform],
            [entry(transform, Kind.DERIVED_FROM, "A", "B")],
            {record(source): False},
        )


def test_a_complete_output_requires_a_possible_origin():
    source = read()
    transform = operation("transform", [source], [source])
    reader = RecordReader([source, transform], [entry(transform, Kind.DROPPED, "A")])
    with pytest.raises(ValueError, match="cannot explain"):
        reader.backward(record(transform))
