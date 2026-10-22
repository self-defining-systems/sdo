from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from google.protobuf import json_format
from google.protobuf.timestamp_pb2 import Timestamp

from sdo.agents.responder.reflection import SessionReflector
from sdo.agents.responder.reflection_outcomes import (
    collapse_detector_history,
    current_outcome_view,
    history_view,
    prior_outcome_view,
    split_quiet_program_runs,
)
from sdo.contracts.proto import (
    Binding,
    ConfirmedRootCause,
    DetectorEvaluation,
    Finding,
    ObjectRef,
    OutcomeRecord,
    OutcomeTimestamps,
    ProgramRun,
    RepairActionReceipt,
    Verdict,
)

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

_T0 = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
_EVIDENCE = "FailedMount: configmap mongo-geo-script not found " * 40


def _stamp(at: datetime) -> Timestamp:
    value = Timestamp()
    value.FromDatetime(at)
    return value


def _evaluations(detector_id: str, statuses: list[str], *, fingerprint: str = "fp") -> list[DetectorEvaluation]:
    return [
        DetectorEvaluation(
            detector_id=detector_id,
            evaluated_at=_stamp(_T0 + timedelta(seconds=index)),
            status=status,
            fingerprints=[fingerprint] if status == "firing" else [],
        )
        for index, status in enumerate(statuses)
    ]


def _outcome(incident_id: str, *, evaluations: int = 20) -> OutcomeRecord:
    ref = ObjectRef(api_version="v1", kind="ConfigMap", namespace="app", name="mongo-geo-script")
    return OutcomeRecord(
        schema_version=1,
        incident_id=incident_id,
        source_commit="source",
        deployed_commit="deployed",
        classification="success",
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
                started_at=_stamp(_T0),
                completed_at=_stamp(_T0),
                success=True,
                reversible=True,
            )
        ],
        timestamps=OutcomeTimestamps(detected_at=_stamp(_T0), dispatched_at=_stamp(_T0), completed_at=_stamp(_T0)),
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

    full = json_format.MessageToDict(
        outcome, preserving_proto_field_name=True, always_print_fields_with_no_presence=True
    )
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
    closure.request.incident_id = current.incident_id
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


def _program_outcome(*, sweeps: int = 52) -> OutcomeRecord:
    """Hand-written three-binding fixture shaped like the I2 repeated sweeps."""
    outcome = _outcome("inc-program")
    for sweep in range(sweeps):
        for name in ("frontend", "search", "rate"):
            outcome.program_runs.append(
                ProgramRun(
                    program_id="image-pull",
                    program_revision="revision-a",
                    binding_source="sweep",
                    bindings=[
                        Binding(
                            name="deployment",
                            object=ObjectRef(api_version="apps/v1", kind="Deployment", namespace="app", name=name),
                        )
                    ],
                    verdict=Verdict(result=Verdict.RESULT_NO_MATCH),
                    duration={"nanos": (sweep + 1) * 1000},
                )
            )
    outcome.program_failure_signals.append("image-pull/search: unsupported replay")
    for _ in range(5):
        outcome.program_tool_calls.add(
            command="source",
            request_json='{"ref":"Deployment/search"}',
            response_json=json.dumps(
                {"source": {"manifest": "image: accepted\n" * 1200}, "corpus_ref": "captured/open@detect.json"}
            ),
            summary="Accepted search source",
            at=_stamp(_T0),
        )
    return outcome


def test_current_outcome_summarizes_no_match_sweeps_without_mutating_durable_evidence() -> None:
    outcome = _program_outcome()
    before = outcome.SerializeToString()
    view = current_outcome_view(outcome)
    assert view["program_runs"] == []
    assert view["program_run_summary"] == [
        {"program_id": "image-pull", "program_revision": "revision-a", "no_match": 156, "unknown": 0}
    ]
    assert view["reflection_compaction"]["program_runs_omitted_duplicates"] == 153
    assert view["program_failure_signals"] == list(outcome.program_failure_signals)
    assert outcome.SerializeToString() == before


def test_current_outcome_keeps_match_runs_and_summarizes_the_rest_per_program_revision() -> None:
    outcome = _program_outcome(sweeps=1)
    run = outcome.program_runs[1]
    for verdict, revision, error in [
        (Verdict.RESULT_MATCH, "revision-a", ""),
        (Verdict.RESULT_NO_MATCH, "revision-a", ""),
        (Verdict.RESULT_UNKNOWN, "revision-a", "read unavailable"),
        (Verdict.RESULT_UNKNOWN, "revision-b", "read unavailable"),
        (Verdict.RESULT_UNKNOWN, "revision-b", "forbidden"),
    ]:
        updated = ProgramRun()
        updated.CopyFrom(run)
        updated.verdict.result = verdict
        updated.program_revision = revision
        updated.error = error
        outcome.program_runs.append(updated)
    view = current_outcome_view(outcome)
    assert [kept["verdict"]["result"] for kept in view["program_runs"]] == ["RESULT_MATCH"]
    assert view["program_run_summary"] == [
        {
            "program_id": "image-pull",
            "program_revision": "revision-a",
            "no_match": 4,
            "unknown": 1,
            "unknown_reasons": ["read unavailable"],
        },
        {
            "program_id": "image-pull",
            "program_revision": "revision-b",
            "no_match": 0,
            "unknown": 2,
            "unknown_reasons": ["read unavailable", "forbidden"],
        },
    ]
    assert view["repair_actions"]
    assert view["confirmed_root_causes"]
    assert view["findings"][0]["evidence"] == _EVIDENCE


def test_current_outcome_bounds_tool_responses_and_keeps_corpus_refs() -> None:
    outcome = _program_outcome()
    view = current_outcome_view(outcome)
    calls = view["program_tool_calls"]
    assert len(calls) == 1
    assert calls[0]["evaluations"] == 5
    assert view["reflection_compaction"]["program_tool_calls_omitted_duplicates"] == 4
    assert len(json.dumps(calls)) < 6000
    assert calls[0]["corpus_refs"] == ["captured/open@detect.json"]
    assert calls[0]["response_json_omitted_chars"] > 0


def test_reflection_prompt_repeated_sweep_payload_is_bounded(tmp_path: Path) -> None:
    small = _program_outcome(sweeps=1)
    large = _program_outcome(sweeps=52)
    small_prompt = _prompt([], small, tmp_path)
    large_prompt = _prompt([], large, tmp_path)
    assert len(large_prompt) - len(small_prompt) < 100
    assert large_prompt.count("FailedMount") == 40


def test_reflection_prompt_size_reduction_on_synthetic_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import sdo.agents.responder.reflection as reflection

    outcome = _program_outcome()
    full = json_format.MessageToDict(
        outcome, preserving_proto_field_name=True, always_print_fields_with_no_presence=True
    )
    # The old model view only collapsed detector_history. Keep the same prompt
    # instructions on both sides to isolate the outcome-projection savings.
    full["detector_history"] = collapse_detector_history(outcome.detector_history)
    with monkeypatch.context() as baseline:
        baseline.setattr(reflection, "current_outcome_view", lambda _: full)
        before = _prompt([], outcome, tmp_path)
    after = _prompt([], outcome, tmp_path)
    reduction = 100 * (len(before) - len(after)) / len(before)
    print(
        json.dumps({"before_chars": len(before), "after_chars": len(after), "reduction_percent": round(reduction, 2)})
    )
    assert reduction > 85


def test_program_run_summary_caps_reasons_and_programs() -> None:
    def unknown(program: str, error: str) -> dict[str, object]:
        return {"program_id": program, "program_revision": "r", "verdict": {"result": "RESULT_UNKNOWN"}, "error": error}

    runs = [unknown("p0", f"reason {index} " + "x" * 400) for index in range(5)]
    runs += [unknown(f"p{index}", "") for index in range(1, 33)]

    kept, summary = split_quiet_program_runs(runs)

    assert kept == []
    assert len(summary[0]["unknown_reasons"]) == 3
    assert all(len(reason) <= 160 for reason in summary[0]["unknown_reasons"])
    assert summary[0]["unknown_reasons_omitted"] == 2
    assert summary[0]["unknown"] == 5
    assert summary[-1] == {"programs_omitted": 3}
    assert len(summary) == 31
