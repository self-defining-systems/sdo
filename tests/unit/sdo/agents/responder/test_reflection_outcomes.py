from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sdo.agents.responder.reflection import SessionReflector
from sdo.agents.responder.reflection_outcomes import (
    collapse_detector_history,
    current_outcome_view,
    history_view,
    prior_outcome_view,
)
from sdo.contracts import (
    ConfirmedRootCause,
    DetectorEvaluation,
    DetectorEvaluationStatus,
    Finding,
    ObjectRef,
    OutcomeClassification,
    OutcomeRecord,
    RepairActionReceipt,
)

if TYPE_CHECKING:
    from pathlib import Path

_T0 = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
_EVIDENCE = "FailedMount: configmap mongo-geo-script not found " * 40


def _evaluations(detector_id: str, statuses: list[str], *, fingerprint: str = "fp") -> list[DetectorEvaluation]:
    return [
        DetectorEvaluation(
            detector_id=detector_id,
            evaluated_at=_T0 + timedelta(seconds=index),
            status=DetectorEvaluationStatus(status),
            fingerprints=[fingerprint] if status == "firing" else [],
        )
        for index, status in enumerate(statuses)
    ]


def _outcome(incident_id: str, *, evaluations: int = 20) -> OutcomeRecord:
    ref = ObjectRef(api_version="v1", kind="ConfigMap", namespace="app", name="mongo-geo-script")
    return OutcomeRecord(
        incident_id=incident_id,
        source_commit="source",
        deployed_commit="deployed",
        classification=OutcomeClassification.SUCCESS,
        responder_backend="codex",
        responder_model="gpt-test",
        detector_history=_evaluations("health-objective", ["firing"] * evaluations)
        + _evaluations("service-endpoints", ["firing"] * evaluations),
        findings=[
            Finding(
                detector_id="health-objective",
                rule_id="service-without-ready-endpoints",
                status="active",
                severity="critical",
                summary="Service mongodb-geo has no ready endpoints",
                evidence=_EVIDENCE,
                primary_resource=ref,
                fingerprint="health-objective/no-endpoints",
            )
        ],
        applied_playbooks=["missing-configmap"],
        confirmed_root_causes=[
            ConfirmedRootCause(summary=f"{incident_id}: required ConfigMap was removed", resources=[ref]),
        ],
        repair_actions=[
            RepairActionReceipt(
                action_id="restore",
                kind="apply",
                target="ConfigMap/mongo-geo-script",
                summary="restore the ConfigMap from tracked source",
                details="kubectl apply -f kubernetes/geo/mongo-geo-script-configmap.yaml " * 20,
                started_at=_T0,
                completed_at=_T0,
                success=True,
                reversible=True,
            )
        ],
        timestamps={"detected_at": _T0.isoformat(), "dispatched_at": _T0.isoformat(), "completed_at": _T0.isoformat()},
    )


def test_collapse_detector_history_keeps_every_status_transition_but_not_repeats() -> None:
    history = _evaluations("health-objective", ["firing", "firing", "firing", "clear", "clear", "firing"])

    runs = collapse_detector_history(history)

    assert [(run["status"], run["evaluations"]) for run in runs] == [("firing", 3), ("clear", 2), ("firing", 1)]
    assert runs[0]["first_evaluated_at"] != runs[0]["last_evaluated_at"]
    assert runs[0]["detector_id"] == "health-objective"
    assert runs[0]["fingerprints"] == ["fp"]


def test_collapse_detector_history_splits_runs_when_fingerprints_change() -> None:
    history = _evaluations("d", ["firing"], fingerprint="a") + _evaluations("d", ["firing"], fingerprint="b")

    assert [run["fingerprints"] for run in collapse_detector_history(history)] == [["a"], ["b"]]


def test_collapse_detector_history_tracks_each_detector_independently() -> None:
    interleaved = [
        _evaluations("a", ["firing"])[0],
        _evaluations("b", ["firing"])[0],
        _evaluations("a", ["firing"])[0],
        _evaluations("b", ["firing"])[0],
    ]

    runs = collapse_detector_history(interleaved)

    assert {(run["detector_id"], run["evaluations"]) for run in runs} == {("a", 2), ("b", 2)}


def test_current_outcome_keeps_findings_evidence_and_repairs_verbatim() -> None:
    outcome = _outcome("inc-1")

    view = current_outcome_view(outcome)

    full = outcome.model_dump(mode="json")
    assert {key: value for key, value in view.items() if key != "detector_history"} == {
        key: value for key, value in full.items() if key != "detector_history"
    }
    assert len(view["detector_history"]) == 2
    assert len(json.dumps(view)) < len(json.dumps(full))


def test_prior_outcome_keeps_learning_signal_and_drops_bulk() -> None:
    view = prior_outcome_view(_outcome("inc-0"))
    rendered = json.dumps(view)

    assert view["incident_id"] == "inc-0"
    assert view["classification"] == "success"
    assert "inc-0: required ConfigMap was removed" in rendered
    assert "restore the ConfigMap from tracked source" in rendered
    assert "missing-configmap" in rendered
    assert "health-objective/no-endpoints" in rendered
    assert "service-without-ready-endpoints" in rendered
    # Raw evidence text, action details, and usage are the bulk reflection does not need for past incidents.
    assert "FailedMount" not in rendered
    assert "kubectl apply" not in rendered
    assert "usage" not in view
    assert len(rendered) < 2_500


def test_history_view_excludes_the_current_incident_and_keeps_order() -> None:
    records = [_outcome("inc-0"), _outcome("inc-1"), _outcome("inc-2")]

    view = history_view(records, current_incident_id="inc-2")

    assert [entry["incident_id"] for entry in view] == ["inc-0", "inc-1"]


class _Backend:
    def __init__(self) -> None:
        self.prompt = ""

    def resume(self, *, session_id: str, worktree: Path, prompt: str, idempotency_key: str) -> object:
        del session_id, worktree, idempotency_key
        self.prompt = prompt
        raise RuntimeError("stop")

    fresh = resume


def _prompt(history: list[OutcomeRecord], current: OutcomeRecord, tmp_path: Path) -> str:
    from tests.unit.sdo.agents.responder.test_reflection import _fresh_closure

    backend = _Backend()
    closure = _fresh_closure(tmp_path)
    closure.request = closure.request.model_copy(update={"incident_id": current.incident_id})
    try:
        SessionReflector(backend).resume(  # type: ignore[arg-type]
            session_id="s",
            incident_id=current.incident_id,
            worktree=tmp_path,
            outcome=current,
            history=history,
            outcome_commit="sha",
            closure=closure,
        )
    except RuntimeError:
        pass
    return backend.prompt


def test_reflection_prompt_growth_per_prior_outcome_is_bounded(tmp_path: Path) -> None:
    current = _outcome("inc-current")
    small = _prompt([current], current, tmp_path)
    priors = [_outcome(f"inc-{index}") for index in range(6)]
    large = _prompt([*priors, current], current, tmp_path)

    per_prior = (len(large) - len(small)) / len(priors)
    assert per_prior < 2_500
    # The current incident is never repeated inside the history.
    assert large.count("inc-current: required ConfigMap was removed") == 1
    for prior in priors:
        assert f"{prior.incident_id}: required ConfigMap was removed" in large


def test_reflection_prompt_without_prior_outcomes_says_so(tmp_path: Path) -> None:
    current = _outcome("inc-current")

    prompt = _prompt([current], current, tmp_path)

    assert "Outcome history (prior incidents, compact): none" in prompt
