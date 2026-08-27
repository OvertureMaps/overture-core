"""Airflow 2 / 3 compatibility shim.

Import Airflow symbols from this module instead of directly from ``airflow.sdk``
(3.x) or the scattered 2.x paths. Works transparently on both versions.

Usage::

    from overture_serverless.airflow_compat import DAG, task, Variable, chain

Two paths to remove this file:

Upgrade path when the running Airflow ships Airflow 3:
  1. Delete this file.
  2. Global find-and-replace: ``from overture_serverless.airflow_compat import``
     -> ``from airflow.sdk import``
  3. Done -- one clean commit.

Upgrade path when Airflow v2.11.1+ is the floor:
  1. Delete this file.
  2. Global find-and-replace: ``from overture_serverless.airflow_compat import``
     -> ``from airflow.providers.common.compat.sdk import``
  3. Done -- one clean commit.
  4. Optional -- follow upstream and remove the common-compat shim package from
     your codebase entirely once Airflow 3 is supported.

Implementation mirrors the lazy-__getattr__ approach used by the official
``airflow.providers.common.compat.sdk`` shim so symbols are only imported on
first access, keeping DAG parse time fast.

Inspired by upstream:
https://github.com/apache/airflow/blob/main/providers/common/compat/src/airflow/providers/common/compat/sdk.py
"""

from __future__ import annotations

import importlib
from typing import Any

# ---------------------------------------------------------------------------
# Rename map: symbols that changed name between Airflow 2.x and 3.x
# Format: new_name -> (v3_module, v2_module, v2_name)
# ---------------------------------------------------------------------------
_RENAME_MAP: dict[str, tuple[str, str, str]] = {
    "Asset": ("airflow.sdk", "airflow.datasets", "Dataset"),
    "AssetAlias": ("airflow.sdk", "airflow.datasets", "DatasetAlias"),
    "AssetAll": ("airflow.sdk", "airflow.datasets", "DatasetAll"),
    "AssetAny": ("airflow.sdk", "airflow.datasets", "DatasetAny"),
}

# ---------------------------------------------------------------------------
# Import map: symbol name -> (v3_module, v2_module)
# None as v2_module means the symbol is Airflow 3-only (no 2.x equivalent).
# ---------------------------------------------------------------------------
_IMPORT_MAP: dict[str, tuple[str, str | None]] = {
    # -- DAG / decorators -----------------------------------------------------
    "DAG": ("airflow.sdk", "airflow.models.dag"),
    "dag": ("airflow.sdk", "airflow.decorators"),
    "task": ("airflow.sdk", "airflow.decorators"),
    "task_group": ("airflow.sdk", "airflow.decorators"),
    "setup": ("airflow.sdk", "airflow.decorators"),
    "teardown": ("airflow.sdk", "airflow.decorators"),
    "chain": ("airflow.sdk", "airflow.models.baseoperator"),
    "chain_linear": ("airflow.sdk", "airflow.models.baseoperator"),
    "cross_downstream": ("airflow.sdk", "airflow.models.baseoperator"),
    # -- Models ----------------------------------------------------------------
    # Variable is handled separately below (see `Variable` class) -- Variable.get()'s
    # `default_var` kwarg was renamed to `default` in 3.x, so a straight symbol swap
    # isn't enough; every call site would need touching otherwise.
    "Connection": ("airflow.sdk", "airflow.models.connection"),
    "Param": ("airflow.sdk", "airflow.models.param"),
    "ParamsDict": ("airflow.sdk", "airflow.models.param"),
    "XComArg": ("airflow.sdk", "airflow.models.xcom_arg"),
    "XCom": ("airflow.sdk.execution_time.xcom", "airflow.models.xcom"),
    "MappedOperator": (
        "airflow.sdk.definitions.mappedoperator",
        "airflow.models.mappedoperator",
    ),
    # -- Operators / base classes ----------------------------------------------
    "BaseOperator": ("airflow.sdk", "airflow.models.baseoperator"),
    "BaseOperatorLink": ("airflow.sdk", "airflow.models.baseoperatorlink"),
    "BaseSensorOperator": ("airflow.sdk", "airflow.sensors.base"),
    "PokeReturnValue": ("airflow.sdk", "airflow.sensors.base"),
    "poke_mode_only": ("airflow.sdk.bases.sensor", "airflow.sensors.base"),
    "BaseHook": ("airflow.sdk", "airflow.hooks.base"),
    "BaseBranchOperator": (
        "airflow.sdk.bases.branch",
        "airflow.providers.standard.operators.branch",
    ),
    "BranchMixIn": (
        "airflow.sdk.bases.branch",
        "airflow.providers.standard.operators.branch",
    ),
    "SkipMixin": ("airflow.sdk.bases.skipmixin", "airflow.models.skipmixin"),
    # -- Decorator internals ---------------------------------------------------
    "TaskDecorator": ("airflow.sdk.bases.decorator", "airflow.decorators.base"),
    "DecoratedOperator": ("airflow.sdk.bases.decorator", "airflow.decorators.base"),
    "DecoratedMappedOperator": (
        "airflow.sdk.bases.decorator",
        "airflow.decorators.base",
    ),
    "task_decorator_factory": (
        "airflow.sdk.bases.decorator",
        "airflow.decorators.base",
    ),
    "get_unique_task_id": ("airflow.sdk.bases.decorator", "airflow.decorators.base"),
    # -- Groups & modifiers ----------------------------------------------------
    "TaskGroup": ("airflow.sdk", "airflow.utils.task_group"),
    "EdgeModifier": ("airflow.sdk", "airflow.utils.edgemodifier"),
    "Label": ("airflow.sdk", "airflow.utils.edgemodifier"),
    # -- State / enums ---------------------------------------------------------
    "TriggerRule": ("airflow.sdk", "airflow.utils.trigger_rule"),
    "DagRunState": ("airflow.sdk", "airflow.utils.state"),
    "TaskInstanceState": ("airflow.sdk", "airflow.utils.state"),
    "WeightRule": ("airflow.sdk", "airflow.utils.weight_rule"),
    # -- Context & utilities ---------------------------------------------------
    "Context": ("airflow.sdk", "airflow.utils.context"),
    "context_merge": ("airflow.sdk.definitions.context", "airflow.utils.context"),
    "get_current_context": ("airflow.sdk", "airflow.operators.python"),
    "get_parsing_context": ("airflow.sdk", "airflow.utils.dag_parsing_context"),
    "TaskInstanceKey": ("airflow.sdk.types", "airflow.models.taskinstancekey"),
    # -- Assets ----------------------------------------------------------------
    "Metadata": ("airflow.sdk", "airflow.datasets.metadata"),
    "ObjectStoragePath": ("airflow.sdk", "airflow.io.path"),
    # -- Exceptions ------------------------------------------------------------
    "AirflowException": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "AirflowConfigException": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "AirflowFailException": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "AirflowNotFoundException": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "AirflowOptionalProviderFeatureException": (
        "airflow.sdk.exceptions",
        "airflow.exceptions",
    ),
    "AirflowSensorTimeout": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "AirflowSkipException": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "AirflowTaskTimeout": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "ParamValidationError": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "TaskDeferred": ("airflow.sdk.exceptions", "airflow.exceptions"),
    "XComNotFound": ("airflow.sdk.exceptions", "airflow.exceptions"),
    # Airflow 3-only -- no 2.x equivalent
    "DagRunTriggerException": ("airflow.sdk.exceptions", None),
    "DownstreamTasksSkipped": ("airflow.sdk.exceptions", None),
    # -- Observability ---------------------------------------------------------
    "Stats": ("airflow.sdk._shared.observability.metrics.stats", "airflow.stats"),
    # -- Configuration ---------------------------------------------------------
    "conf": ("airflow.sdk.configuration", "airflow.configuration"),
    # -- Lineage ---------------------------------------------------------------
    "HookLineage": ("airflow.sdk.lineage", "airflow.lineage.hook"),
    "HookLineageCollector": ("airflow.sdk.lineage", "airflow.lineage.hook"),
    "HookLineageReader": ("airflow.sdk.lineage", "airflow.lineage.hook"),
    "NoOpCollector": ("airflow.sdk.lineage", "airflow.lineage.hook"),
    "get_hook_lineage_collector": ("airflow.sdk.lineage", "airflow.lineage.hook"),
    # -- Plugins ---------------------------------------------------------------
    "AirflowPlugin": ("airflow.sdk.plugins_manager", "airflow.plugins_manager"),
    # -- BaseNotifier ----------------------------------------------------------
    "BaseNotifier": ("airflow.sdk", "airflow.notifications.basenotifier"),
    # -- Secrets -----------------------------------------------------------------
    # airflow.sdk.log (not the _shared.secrets_masker it wraps) is the intended
    # public entry point on 3.x -- it also notifies the supervisor process.
    "mask_secret": ("airflow.sdk.log", "airflow.utils.log.secrets_masker"),
}

# ---------------------------------------------------------------------------
# Module map: symbols that are entire modules (imported with `import X`)
# Format: name -> (v3_module_path, v2_module_path)
# ---------------------------------------------------------------------------
_MODULE_MAP: dict[str, tuple[str, str]] = {
    "timezone": ("airflow.sdk.timezone", "airflow.utils.timezone"),
    "io": ("airflow.sdk.io", "airflow.io"),
}


def _load(module_path: str, attr: str) -> Any:
    mod = importlib.import_module(module_path)
    return getattr(mod, attr)


def _real_variable() -> Any:
    try:
        return _load("airflow.sdk", "Variable")
    except (ImportError, AttributeError):
        return _load("airflow.models.variable", "Variable")


_NOTSET = object()


class _VariableMeta(type):
    """Delegates any attribute not defined on `Variable` (`.set`, `.delete`, ...)
    straight to the real Variable class for the running Airflow version."""

    def __getattr__(cls, name: str) -> Any:
        return getattr(_real_variable(), name)


class Variable(metaclass=_VariableMeta):
    """Variable proxy that normalizes `Variable.get()`'s `default_var` (2.x) /
    `default` (3.x) kwarg rename, so call sites can keep using `default_var=`
    on both versions. Everything else delegates to the real class via `_VariableMeta`.
    """

    @staticmethod
    def get(
        key: str, default_var: Any = _NOTSET, deserialize_json: bool = False
    ) -> Any:
        real = _real_variable()
        if default_var is _NOTSET:
            return real.get(key, deserialize_json=deserialize_json)
        try:
            return real.get(key, default=default_var, deserialize_json=deserialize_json)
        except TypeError:
            return real.get(
                key, default_var=default_var, deserialize_json=deserialize_json
            )


def __getattr__(name: str) -> Any:
    # Renamed symbols (e.g. Dataset -> Asset)
    if name in _RENAME_MAP:
        v3_mod, v2_mod, v2_name = _RENAME_MAP[name]
        try:
            return _load(v3_mod, name)
        except (ImportError, AttributeError):
            return _load(v2_mod, v2_name)

    # Whole-module symbols
    if name in _MODULE_MAP:
        v3_path, v2_path = _MODULE_MAP[name]
        for path in (v3_path, v2_path):
            try:
                return importlib.import_module(path)
            except ImportError:
                continue
        raise ImportError(f"airflow_compat: cannot import module {name!r}")

    # Regular symbols
    if name in _IMPORT_MAP:
        v3_mod, v2_mod = _IMPORT_MAP[name]
        try:
            return _load(v3_mod, name)
        except (ImportError, AttributeError):
            pass
        if v2_mod is not None:
            return _load(v2_mod, name)
        raise ImportError(f"airflow_compat: {name!r} requires Airflow 3+")

    raise AttributeError(f"module 'airflow_compat' has no attribute {name!r}")


__all__ = ["Variable", *_RENAME_MAP, *_IMPORT_MAP, *_MODULE_MAP]
