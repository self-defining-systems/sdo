from __future__ import annotations

from datetime import datetime, timezone

import pytest

from sdo.agents.responder.cause_admissibility import (
    CAUSE_ADMISSIBILITY_ENV,
    AdmissibilityReview,
    CauseAdmissibilityError,
    CauseAdmissibilityPolicy,
    apply_cause_admissibility,
    is_admissible,
    review_confirmed_causes,
)
from sdo.contracts import (
    ConfirmedRootCause,
    DetectorEvaluation,
    Finding,
    IncidentRequest,
    IncidentResult,
    ObjectRef,
    RootCauseEvidence,
)

_NOW = datetime(2026, 7, 9, 18, 0, 0, tzinfo=timezone.utc)


def _finding(detector_id: str, *, status: str = "active") -> Finding:
    return Finding(
        detector_id=detector_id,
        rule_id=detector_id,
        status=status,  # type: ignore[arg-type]
        severity="critical",  # type: ignore[arg-type]
        summary=f"{detector_id} fired",
        evidence="evidence",
        primary_resource=ObjectRef(kind="Deployment", name="geo", namespace="demo"),
    )


def _request(*, firing: tuple[str, ...] = ("missing-configmap",)) -> IncidentRequest:
    first = firing[0] if firing else "missing-configmap"
    return IncidentRequest(
        application="hotel",
        namespace="demo",
        incident_id="inc-1",
        findings=[_finding(first)],
        detector_history=[
            DetectorEvaluation(detector_id=detector, evaluated_at=_NOW, status="firing")  # type: ignore[arg-type]
            for detector in firing
        ]
        or [DetectorEvaluation(detector_id="health", evaluated_at=_NOW, status="firing")],  # type: ignore[arg-type]
        source_commit="a" * 40,
        deployed_commit="b" * 40,
        architecture_summary_path=".sdo/arch.md",
        health_objective_path=".sdo/goal.md",
        repository_worktree="/workspace/app",
        repository_base_commit="c" * 40,
        response_deadline=_NOW,
        cancellation_token="token",
    )


def _resource(name: str, kind: str = "Deployment") -> ObjectRef:
    return ObjectRef(api_version="apps/v1", kind=kind, namespace="demo", name=name)


def _cause(
    summary: str,
    *,
    evidence: list[RootCauseEvidence],
    explained: list[str],
    resources: list[ObjectRef] | None = None,
) -> ConfirmedRootCause:
    return ConfirmedRootCause(
        summary=summary,
        resources=resources or [_resource("geo")],
        evidence=evidence,
        explained_detectors=explained,
    )


def _result(causes: list[ConfirmedRootCause]) -> IncidentResult:
    return IncidentResult(
        incident_id="inc-1",
        status="completed",  # type: ignore[arg-type]
        confirmed_root_causes=causes,
        usage={"llm_calls": 1, "input_tokens": 1, "output_tokens": 1},  # type: ignore[arg-type]
        timing={"started_at": _NOW, "completed_at": _NOW},  # type: ignore[arg-type]
    )


def _detector_evidence(source: str) -> RootCauseEvidence:
    return RootCauseEvidence(kind="detector-finding", source=source, observation="detector fired")


def _state_change_evidence(source: str) -> RootCauseEvidence:
    return RootCauseEvidence(kind="state-change", source=source, observation="object changed since healthy")


def _narrative_evidence() -> RootCauseEvidence:
    return RootCauseEvidence(kind="live-observation", source="kubectl get pods", observation="pods look off")


def test_cause_with_corroborating_evidence_is_admitted() -> None:
    cause = _cause(
        "geo referenced an absent ConfigMap",
        evidence=[_detector_evidence("missing-configmap"), _narrative_evidence()],
        explained=["missing-configmap"],
    )

    admitted, reason = is_admissible(cause, firing_detectors=frozenset({"missing-configmap"}))

    assert admitted
    assert "corroborating" in reason


def test_purely_narrative_cause_without_a_fired_detector_is_withheld() -> None:
    """A benign-drift/decoy cause the responder can only narrate is weakly evidenced."""

    cause = _cause(
        "LOG_LEVEL env drift looked suspicious",
        evidence=[_narrative_evidence()],
        explained=["some-detector-that-never-fired"],
    )

    admitted, reason = is_admissible(cause, firing_detectors=frozenset({"missing-configmap"}))

    assert not admitted
    assert "unverifiable live observations" in reason


def test_narrative_cause_is_admitted_when_it_explains_a_fired_detector() -> None:
    """A late-landing composite component can be narrated but still explain a real firing detector."""

    cause = _cause(
        "second component",
        evidence=[_narrative_evidence()],
        explained=["late-component"],
    )

    admitted, reason = is_admissible(cause, firing_detectors=frozenset({"missing-configmap", "late-component"}))

    assert admitted
    assert "late-component" in reason


def test_review_withholds_only_the_weakly_evidenced_cause() -> None:
    strong = _cause(
        "real cause",
        evidence=[_detector_evidence("missing-configmap")],
        explained=["missing-configmap"],
    )
    weak = _cause(
        "benign drift",
        evidence=[_narrative_evidence()],
        explained=["never-fired"],
    )

    review = review_confirmed_causes(_result([strong, weak]), _request())

    assert isinstance(review, AdmissibilityReview)
    assert [cause.summary for cause in review.admitted] == ["real cause"]
    assert [decision.summary for decision in review.withheld] == ["benign drift"]


def test_every_component_of_a_genuine_composite_is_admitted() -> None:
    request = _request(firing=("missing-configmap", "bad-selector"))
    first = _cause(
        "missing ConfigMap",
        evidence=[_detector_evidence("missing-configmap")],
        explained=["missing-configmap"],
        resources=[_resource("geo")],
    )
    second = _cause(
        "selector drift",
        evidence=[_state_change_evidence("Service/frontend")],
        explained=["bad-selector"],
        resources=[_resource("frontend", kind="Service")],
    )

    gated = apply_cause_admissibility(_result([first, second]), request, policy=CauseAdmissibilityPolicy(mode="on"))

    assert [cause.summary for cause in gated.confirmed_root_causes] == ["missing ConfigMap", "selector drift"]


def test_off_mode_returns_the_result_unchanged() -> None:
    weak = _cause("benign drift", evidence=[_narrative_evidence()], explained=["never-fired"])
    result = _result([weak])

    gated = apply_cause_admissibility(result, _request(), policy=CauseAdmissibilityPolicy(mode="off"))

    assert gated is result
    assert [cause.summary for cause in gated.confirmed_root_causes] == ["benign drift"]


def test_default_environment_is_off_and_passes_the_result_through(monkeypatch) -> None:
    monkeypatch.delenv(CAUSE_ADMISSIBILITY_ENV, raising=False)
    weak = _cause("benign drift", evidence=[_narrative_evidence()], explained=["never-fired"])
    result = _result([weak])

    gated = apply_cause_admissibility(result, _request())

    assert gated is result
    assert [cause.summary for cause in gated.confirmed_root_causes] == ["benign drift"]


def test_on_mode_drops_the_weakly_evidenced_cause_from_the_result() -> None:
    weak = _cause("benign drift", evidence=[_narrative_evidence()], explained=["never-fired"])

    gated = apply_cause_admissibility(_result([weak]), _request(), policy=CauseAdmissibilityPolicy(mode="on"))

    assert gated.confirmed_root_causes == []


def test_policy_reads_the_environment_and_defaults_off(monkeypatch) -> None:
    monkeypatch.delenv(CAUSE_ADMISSIBILITY_ENV, raising=False)
    assert not CauseAdmissibilityPolicy.from_environment().enabled
    assert not CauseAdmissibilityPolicy().enabled

    monkeypatch.setenv(CAUSE_ADMISSIBILITY_ENV, "on")
    assert CauseAdmissibilityPolicy.from_environment().enabled

    monkeypatch.setenv(CAUSE_ADMISSIBILITY_ENV, "OFF")
    assert not CauseAdmissibilityPolicy.from_environment().enabled


def test_invalid_mode_is_rejected() -> None:
    with pytest.raises(CauseAdmissibilityError):
        CauseAdmissibilityPolicy(mode="maybe")
