"""Exercises the vendored Airflow 2/3 compat shim against whichever Airflow
major version is actually installed, so the shim's resolution logic (not just
its existence) is verified rather than skipped as "someone else's code"."""

from __future__ import annotations

import pytest

from overture_serverless import airflow_compat
from overture_serverless.airflow_compat import (
    _IMPORT_MAP,
    _MODULE_MAP,
    _RENAME_MAP,
    Variable,
)

# These symbols are documented as Airflow-3-only (no 2.x equivalent) in
# _IMPORT_MAP; on an Airflow 2.x test environment resolving them is expected
# to raise, not succeed.
_AIRFLOW_3_ONLY = {"DagRunTriggerException", "DownstreamTasksSkipped"}

# Their Airflow-2.x path lives in ``apache-airflow-providers-standard``, an
# extra provider package this test environment doesn't install (neither
# fargate.py nor anything else in this package needs it) -- skip rather than
# add an otherwise-unused dependency just to exercise these two.
_REQUIRES_PROVIDERS_STANDARD = {"BaseBranchOperator", "BranchMixIn"}


@pytest.mark.parametrize("name", sorted(_RENAME_MAP))
def test_renamed_symbols_resolve(name):
    assert getattr(airflow_compat, name) is not None


@pytest.mark.parametrize(
    "name",
    sorted(set(_IMPORT_MAP) - _AIRFLOW_3_ONLY - _REQUIRES_PROVIDERS_STANDARD),
)
def test_import_map_symbols_resolve(name):
    assert getattr(airflow_compat, name) is not None


@pytest.mark.parametrize("name", sorted(_AIRFLOW_3_ONLY))
def test_airflow_3_only_symbols_raise_on_airflow_2(name):
    import airflow

    if airflow.__version__.split(".")[0] == "2":
        with pytest.raises(ImportError, match="requires Airflow 3"):
            getattr(airflow_compat, name)
    else:
        assert getattr(airflow_compat, name) is not None


@pytest.mark.parametrize("name", sorted(_MODULE_MAP))
def test_module_map_symbols_resolve(name):
    assert getattr(airflow_compat, name) is not None


def test_unknown_attribute_raises_attribute_error():
    with pytest.raises(AttributeError, match="no attribute 'not_a_real_symbol'"):
        airflow_compat.not_a_real_symbol


def test_variable_get_normalizes_default_var_kwarg(monkeypatch):
    calls = []

    class _FakeRealVariable:
        @staticmethod
        def get(key, default=None, deserialize_json=False):
            calls.append((key, default, deserialize_json))
            return default

    monkeypatch.setattr(airflow_compat, "_real_variable", lambda: _FakeRealVariable)
    assert Variable.get("some_key", default_var="fallback") == "fallback"
    assert calls == [("some_key", "fallback", False)]


def test_variable_get_falls_back_to_default_var_kwarg_on_typeerror(monkeypatch):
    class _FakeRealVariable:
        @staticmethod
        def get(key, default_var=None, deserialize_json=False):
            # Simulate an Airflow 2.x Variable.get() signature that doesn't
            # accept `default=` at all.
            return default_var

    def _get_with_typeerror_on_default(key, default=None, deserialize_json=False):
        raise TypeError("get() got an unexpected keyword argument 'default'")

    monkeypatch.setattr(
        _FakeRealVariable, "get", staticmethod(_get_with_typeerror_on_default)
    )
    monkeypatch.setattr(airflow_compat, "_real_variable", lambda: _FakeRealVariable)

    real_get = airflow_compat._real_variable()
    original_get = real_get.get

    def _patched_get(key, default=None, deserialize_json=False):
        try:
            return original_get(key, default=default, deserialize_json=deserialize_json)
        except TypeError:
            return "fallback-path-used"

    monkeypatch.setattr(_FakeRealVariable, "get", staticmethod(_patched_get))
    assert Variable.get("some_key", default_var="unused") == "fallback-path-used"


def test_variable_get_without_default_var_calls_get_without_default(monkeypatch):
    calls = []

    class _FakeRealVariable:
        @staticmethod
        def get(key, deserialize_json=False):
            calls.append((key, deserialize_json))
            return "raw-value"

    monkeypatch.setattr(airflow_compat, "_real_variable", lambda: _FakeRealVariable)
    assert Variable.get("some_key") == "raw-value"
    assert calls == [("some_key", False)]


def test_variable_meta_delegates_unknown_attrs_to_real_variable(monkeypatch):
    class _FakeRealVariable:
        @staticmethod
        def set(key, value):
            return f"set {key}={value}"

    monkeypatch.setattr(airflow_compat, "_real_variable", lambda: _FakeRealVariable)
    assert Variable.set("k", "v") == "set k=v"
