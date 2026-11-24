"""Tests for the artifacts module (tree traversal, base I/O, registry)."""

import json
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from overture_core.artifacts import (
    ARTIFACT_REGISTRY,
    MetadataArtifact,
    find_nodes,
    find_nodes_with_context,
    has_keys,
    match_node,
    register_artifact,
)


class TestMatchNode:
    def test_literal_match(self):
        assert match_node({"a": 1, "b": 2}, {"a": 1}) is True

    def test_literal_mismatch(self):
        assert match_node({"a": 1}, {"a": 2}) is False

    def test_callable_predicate(self):
        assert match_node({"a": [1, 2]}, {"a": lambda v: len(v) == 2}) is True

    def test_callable_predicate_fails(self):
        assert match_node({"a": [1]}, {"a": lambda v: len(v) == 2}) is False

    def test_missing_key(self):
        assert match_node({"a": 1}, {"b": 1}) is False

    def test_non_dict_returns_false(self):
        assert match_node("not a dict", {"a": 1}) is False
        assert match_node(42, {"a": 1}) is False
        assert match_node(None, {"a": 1}) is False

    def test_empty_pattern_matches_any_dict(self):
        assert match_node({"a": 1}, {}) is True

    def test_multiple_constraints(self):
        node = {"x": 10, "y": "hello", "z": [1]}
        assert match_node(node, {"x": 10, "y": "hello"}) is True
        assert match_node(node, {"x": 10, "y": "wrong"}) is False


class TestFindNodes:
    def test_flat_dict_match(self):
        tree = {"a": 1, "b": 2}
        results = list(find_nodes(tree, {"a": 1}))
        assert results == [{"a": 1, "b": 2}]

    def test_nested_dict(self):
        tree = {"outer": {"a": 1, "b": 2}}
        results = list(find_nodes(tree, {"a": 1}))
        assert results == [{"a": 1, "b": 2}]

    def test_list_of_dicts(self):
        tree = [{"a": 1}, {"a": 2}, {"a": 1, "extra": True}]
        results = list(find_nodes(tree, {"a": 1}))
        assert len(results) == 2
        assert results[0] == {"a": 1}
        assert results[1] == {"a": 1, "extra": True}

    def test_deeply_nested(self):
        tree = {"l1": {"l2": {"l3": {"target": True, "val": 42}}}}
        results = list(find_nodes(tree, {"target": True}))
        assert results == [{"target": True, "val": 42}]

    def test_no_matches(self):
        tree = {"a": 1, "nested": {"b": 2}}
        assert list(find_nodes(tree, {"c": 3})) == []

    def test_empty_tree(self):
        assert list(find_nodes({}, {"a": 1})) == []
        assert list(find_nodes([], {"a": 1})) == []

    def test_multiple_matches_at_different_depths(self):
        tree = {
            "x": 1,
            "child": {"x": 1, "deeper": {"x": 1}},
        }
        results = list(find_nodes(tree, {"x": 1}))
        assert len(results) == 3


class TestHasKeys:
    def test_all_present(self):
        pred = has_keys("a", "b")
        assert pred({"a": 1, "b": 2, "c": 3}) is True

    def test_missing_key(self):
        pred = has_keys("a", "b")
        assert pred({"a": 1}) is False

    def test_non_dict(self):
        pred = has_keys("a")
        assert pred("string") is False
        assert pred(42) is False
        assert pred(None) is False

    def test_empty_keys(self):
        pred = has_keys()
        assert pred({}) is True
        assert pred({"a": 1}) is True


class TestFindNodesWithContext:
    def test_threads_context_through_ancestors(self):
        tree = {"path": "a", "child": [{"path": "b", "hit": 1}]}
        found = list(
            find_nodes_with_context(
                tree, {"hit": 1}, lambda node, ctx: ctx + [node["path"]], []
            )
        )
        assert found == [({"path": "b", "hit": 1}, ["a", "b"])]


class _Echo(MetadataArtifact):
    """Copies metadata to echo.json, then reads it back to echo.txt."""

    name = "echo"

    def generate(self, metadata, bundle_uri):
        self._write_json(bundle_uri, "echo.json", metadata)
        self._write_text(
            bundle_uri, "echo.txt", str(self._read_json(bundle_uri, "echo.json"))
        )


class TestMetadataArtifactIO:
    @pytest.fixture()
    def s3_store(self, monkeypatch):
        """Replace boto3.client("s3") with an in-memory fake."""
        store: dict[tuple[str, str], bytes] = {}
        mock_client = MagicMock()

        def _put_object(Bucket, Key, Body, **_kwargs):
            store[(Bucket, Key)] = Body if isinstance(Body, bytes) else Body.encode()

        def _get_object(Bucket, Key):
            if (Bucket, Key) not in store:
                error = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}
                raise ClientError(error, "GetObject")
            body = MagicMock()
            body.read.return_value = store[(Bucket, Key)]
            return {"Body": body}

        mock_client.put_object = MagicMock(side_effect=_put_object)
        mock_client.get_object = MagicMock(side_effect=_get_object)
        monkeypatch.setattr(
            "overture_core.cloud.aws.object.boto3",
            MagicMock(client=MagicMock(return_value=mock_client)),
        )
        return store

    def test_round_trips_json_and_text(self, s3_store):
        _Echo().generate({"k": "v"}, "s3://bucket/out/")
        assert json.loads(s3_store[("bucket", "out/echo.json")]) == {"k": "v"}
        assert s3_store[("bucket", "out/echo.txt")] == b"{'k': 'v'}"

    def test_bucket_root(self, s3_store):
        _Echo().generate({}, "s3://bucket")
        assert ("bucket", "echo.json") in s3_store

    def test_read_missing_raises(self, s3_store):
        with pytest.raises(FileNotFoundError, match="s3://bucket/out/nope.json"):
            MetadataArtifact._read_json("s3://bucket/out", "nope.json")


class TestArtifactRegistry:
    @pytest.fixture(autouse=True)
    def _restore_registry(self):
        saved = dict(ARTIFACT_REGISTRY)
        yield
        ARTIFACT_REGISTRY.clear()
        ARTIFACT_REGISTRY.update(saved)

    def test_register_keeps_order_and_is_idempotent(self):
        first, second = _Echo(), _Echo()
        second.name = "second"
        assert register_artifact(first) is first
        register_artifact(second)
        register_artifact(first)
        assert list(ARTIFACT_REGISTRY.items()) == [("echo", first), ("second", second)]

    def test_rejects_a_different_artifact_with_the_same_name(self):
        register_artifact(_Echo())
        with pytest.raises(ValueError, match="'echo' is already registered"):
            register_artifact(_Echo())
