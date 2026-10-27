"""Cluster ledger checks: harness and SDO must agree on the restored application before a row.

``drift_empty`` reads the controller's own drift assessment (the ``drift.unexplained`` detector row in its
runtime state) and names every object in the restored namespaces that differs from accepted source. Any entry is
a harness/source mismatch: fix it in the app source or in the fault's restore, never with a production special
case. Admission calls this; it does not call the cluster itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping

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

    Fails closed: ``state`` is the controller runtime state, and an empty list needs proof that drift was
    evaluated. The controller must have run an evaluation (``evaluation_iteration > 0``) and ``history``, a log
    capped at 100 entries over all detectors, must still hold a ``drift.unexplained`` row (the latest one counts).
    A missing row, a row that is neither ``clear`` nor ``firing``, a ``firing`` row without fingerprints and an
    unreadable fingerprint all raise ``DriftAssessmentUnavailable``.
    """
    scope = set(namespaces)
    if not scope:
        raise ValueError("drift_empty requires at least one restored namespace")
    if int(state.get("evaluation_iteration") or 0) <= 0:
        raise DriftAssessmentUnavailable("drift assessment unavailable: the controller has not completed an evaluation")
    latest = {row.get("detector_id"): row for row in (state.get("history") or [])}
    row = latest.get(DRIFT_DETECTOR)
    if row is None:
        raise DriftAssessmentUnavailable(
            "drift assessment not published: no drift.unexplained row in the evaluation history"
        )
    if row.get("status") == "clear":
        return []
    if row.get("status") != "firing":
        raise DriftAssessmentUnavailable(f"drift assessment is {row.get('status')!r}: {row.get('reason') or row}")
    fingerprints = row.get("fingerprints") or []
    if not fingerprints:
        raise DriftAssessmentUnavailable("drift assessment is firing but names no object")
    mismatches = [DriftMismatch.from_fingerprint(fp) for fp in fingerprints]
    return sorted((m for m in mismatches if m.namespace in scope), key=lambda m: (m.namespace, m.kind, m.name))
