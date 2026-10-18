"""Bounded outcome views for the reflection request.

Reflection is told about the incident it just closed and about earlier
incidents in the same memory. Serializing every ``OutcomeRecord`` whole costs
tokens in proportion to the operator's incident count: each record carries a
per-timestamp ``detector_history``, full finding evidence, and usage. That
text is re-read on every model request of the reflection session. These views
keep what learning is grounded in (findings, root causes, repairs, playbooks,
detector transitions) and drop repeated evaluations and, for past incidents,
raw evidence.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sdo.contracts import DetectorEvaluation, ObjectRef, OutcomeRecord


def _timestamp(evaluation: DetectorEvaluation) -> str:
    return str(evaluation.model_dump(mode="json")["evaluated_at"])


def collapse_detector_history(evaluations: Sequence[DetectorEvaluation]) -> list[dict[str, Any]]:
    """Run-length encode each detector's evaluations by (status, fingerprints, error).

    Consecutive identical evaluations of one detector become a single run with
    its first and last timestamps and count; every status or fingerprint
    transition is kept, in first-seen detector order.
    """

    runs: dict[str, list[dict[str, Any]]] = {}
    for evaluation in evaluations:
        detector_runs = runs.setdefault(evaluation.detector_id, [])
        signature = (evaluation.status.value, evaluation.fingerprints, evaluation.error)
        if detector_runs and detector_runs[-1]["_signature"] == signature:
            detector_runs[-1]["last_evaluated_at"] = _timestamp(evaluation)
            detector_runs[-1]["evaluations"] += 1
            continue
        detector_runs.append(
            {
                "_signature": signature,
                "detector_id": evaluation.detector_id,
                "status": evaluation.status.value,
                "fingerprints": list(evaluation.fingerprints),
                "error": evaluation.error,
                "first_evaluated_at": _timestamp(evaluation),
                "last_evaluated_at": _timestamp(evaluation),
                "evaluations": 1,
            }
        )
    return [
        {key: value for key, value in run.items() if key != "_signature"}
        for detector_runs in runs.values()
        for run in detector_runs
    ]


def current_outcome_view(outcome: OutcomeRecord) -> dict[str, Any]:
    """The incident being reflected on: every field verbatim except the collapsed detector history."""

    view = outcome.model_dump(mode="json")
    view["detector_history"] = collapse_detector_history(outcome.detector_history)
    return view


def _resource(ref: ObjectRef) -> str:
    return f"{ref.kind}/{ref.name}"


def prior_outcome_view(record: OutcomeRecord) -> dict[str, Any]:
    """A past incident reduced to the signal learning generalizes from."""

    return {
        "incident_id": record.incident_id,
        "classification": record.classification.value,
        "findings": [
            {
                "detector_id": finding.detector_id,
                "rule_id": finding.rule_id,
                "summary": finding.summary,
                "fingerprint": finding.fingerprint,
                "primary_resource": _resource(finding.primary_resource),
            }
            for finding in record.findings
        ],
        "confirmed_root_causes": [
            {
                "summary": cause.summary,
                "resources": [_resource(ref) for ref in cause.resources],
                "explained_detectors": cause.explained_detectors,
            }
            for cause in record.confirmed_root_causes
        ],
        "repair_actions": [
            {"kind": action.kind, "target": action.target, "summary": action.summary, "success": action.success}
            for action in record.repair_actions
        ],
        "applied_playbooks": record.applied_playbooks,
        "confirmed_playbooks": record.confirmed_playbooks,
        "rejected_playbooks": record.rejected_playbooks,
        "diagnosis_verification": [
            {"verdict": check.verdict, "summary": check.summary} for check in record.diagnosis_verification
        ],
        "detector_history": collapse_detector_history(record.detector_history),
        "memory_commit": record.memory_commit,
    }


def history_view(history: Sequence[OutcomeRecord], *, current_incident_id: str) -> list[dict[str, Any]]:
    """Prior incidents only: the current incident is already shown in full."""

    return [prior_outcome_view(record) for record in history if record.incident_id != current_incident_id]
