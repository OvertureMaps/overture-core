"""`BundleStatus` and the `BundleStatusReport` that `Bundle.status` returns."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

log = logging.getLogger(__name__)


class BundleStatus(StrEnum):
    """Where an existing bundle is in its lifecycle, as reported by `Bundle.status`.

    - ``SUCCESS`` -- the ``success`` marker exists.
    - ``FAILED`` -- no ``success`` marker, and a ``metadata.json`` section
      has ``"status": "failed"``.
    - ``IN_PROGRESS`` -- anything else: being written, or stopped before
      `Bundle.finalize` (including a rerun that `Bundle.initialize`
      restarted, and a run finalized with a status other than
      ``"success"`` or ``"failed"``).

    A `StrEnum`, so members compare equal to (and serialise as) their values,
    e.g. ``BundleStatus.SUCCESS == "success"``.  A bundle that doesn't exist
    has no status: `BundleStatusReport.status` is ``None``.
    """

    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass(frozen=True)
class BundleStatusReport:
    """What `Bundle.status` found for one bundle.

    Attributes
    ----------
    status : BundleStatus | None
        The bundle's status, or ``None`` when the bundle doesn't exist.
    exists : bool
        Whether any object exists under the bundle's prefix.
    has_success : bool
        Whether the ``success`` marker exists.
    has_metadata : bool
        Whether ``metadata.json`` exists, even if it can't be parsed.
    sections : Mapping[str, str | None]
        The ``status`` of each ``metadata.json`` section, keyed by section
        name (the DAG id); ``None`` for a section without a string
        ``status``.  Empty when there is no readable ``metadata.json``.
    """

    status: BundleStatus | None
    exists: bool
    has_success: bool
    has_metadata: bool
    sections: Mapping[str, str | None] = field(default_factory=dict, hash=False)


def _metadata_section_statuses(body: bytes, uri: str) -> dict[str, str | None]:
    """Return ``{section: status}`` for the ``metadata.json`` *body* read from *uri*.

    Values that aren't JSON objects (sections) are skipped.  Unparseable
    JSON, or a document that isn't an object, is logged and gives ``{}``.
    """
    try:
        document = json.loads(body)
    except ValueError as exc:  # JSONDecodeError and UnicodeDecodeError
        log.warning("Unreadable metadata.json at %s: %s", uri, exc)
        return {}
    if not isinstance(document, dict):
        log.warning("metadata.json at %s is not a JSON object", uri)
        return {}
    statuses: dict[str, str | None] = {}
    for name, section in document.items():
        if isinstance(section, dict):
            status = section.get("status")
            statuses[name] = status if isinstance(status, str) else None
    return statuses
