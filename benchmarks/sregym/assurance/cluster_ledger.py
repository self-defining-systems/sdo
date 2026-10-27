"""Cluster ledger checks: harness and SDO must agree on the restored application before a row.

``drift_empty`` reads the controller's own drift assessment (the ``drift.unexplained`` detector row in its
runtime state) and names every object in the restored namespaces that differs from accepted source. Any entry is
a harness/source mismatch: fix it in the app source or in the fault's restore, never with a production special
case. Admission calls this; it does not call the cluster itself.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Any

DRIFT_DETECTOR = "drift.unexplained"
_FINGERPRINT_PREFIX = (DRIFT_DETECTOR, "unexplained")


class DriftAssessmentUnavailable(RuntimeError):
    """The controller published a drift row that is neither clear nor firing, or an unreadable fingerprint."""


@dataclass(frozen=True)
class DriftMismatch:
    """One object the controller finds different from accepted source."""

    namespace: str
    kind: str
    name: str
    fingerprint: str

    @classmethod
    def from_fingerprint(cls, fingerprint: str) -> DriftMismatch:
        parts = fingerprint.split("/")
        if len(parts) != 6 or tuple(parts[:2]) != _FINGERPRINT_PREFIX or not all(parts[3:5]):
            raise DriftAssessmentUnavailable(f"unreadable drift fingerprint: {fingerprint!r}")
        return cls(namespace=parts[2], kind=parts[3], name=parts[4], fingerprint=fingerprint)


def drift_empty(state: Mapping[str, Any], namespaces: Collection[str]) -> list[DriftMismatch]:
    """Mismatches the controller reports in ``namespaces``; an empty list means source and cluster agree.

    ``state`` is the controller runtime state (``history`` holds the latest row per detector). The controller
    publishes a drift row only once it has evaluated drift, so no row means nothing is reported.
    """
    scope = set(namespaces)
    if not scope:
        raise ValueError("drift_empty requires at least one restored namespace")
    latest = {row.get("detector_id"): row for row in (state.get("history") or [])}
    row = latest.get(DRIFT_DETECTOR)
    if row is None or row.get("status") == "clear":
        return []
    if row.get("status") != "firing":
        raise DriftAssessmentUnavailable(f"drift assessment is {row.get('status')!r}: {row.get('reason') or row}")
    mismatches = [DriftMismatch.from_fingerprint(fp) for fp in row.get("fingerprints") or []]
    return sorted(
        (m for m in mismatches if m.namespace in scope), key=lambda m: (m.namespace, m.kind, m.name)
    )
