"""`BundleStatus` and the `BundleStatusReport` that `Bundle.status` returns."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

log = logging.getLogger(__name__)


class BundleStatus(StrEnum):
    """Lifecycle state of an existing bundle, as reported by `Bundle.status`.

    - ``SUCCESS`` -- the ``success`` marker exists.
    - ``FAILED`` -- no marker, and any ``metadata.json`` section has
      ``"status": "failed"``, even while another section is running.
    - ``IN_PROGRESS`` -- no marker and no failed section, e.g. a running or
      restarted run, or one finalized with another status.
    """

    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass(frozen=True)
class BundleStatusReport:
    """What `Bundle.status` found for one bundle."""

    status: BundleStatus | None  # None when nothing exists under the prefix
    exists: bool  # any object under the bundle's prefix
    has_success: bool
    has_metadata: bool  # metadata.json exists, parsed or not
    # ``status`` of each metadata.json section (DAG id), str or None
    sections: Mapping[str, str | None] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "sections", dict(self.sections))


def _metadata_section_statuses(body: bytes, uri: str) -> dict[str, str | None]:
    """Map each section of the ``metadata.json`` *body* to its ``status``.

    Non-object sections are skipped.  Unparseable or non-object JSON is
    logged (with *uri*) and gives ``{}``.
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
