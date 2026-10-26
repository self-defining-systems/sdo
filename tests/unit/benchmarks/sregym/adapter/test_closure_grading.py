"""Grading is an eval-only closure effect, independent of reflection."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import pytest

from benchmarks.sregym.adapter.grading import ClosureGrader


def ledger() -> dict:
    return {
        "incident_id": "incident-1",
        "closure": {
            "request": {"incident_id": "incident-1"},
            "dispatched_at": "2026-10-26T00:00:00Z",
            "responder_completed_at": "2026-10-26T00:00:30Z",
            "verified_at": "2026-10-26T00:00:35Z",
            "result": {
                "confirmed_root_causes": [{"summary": "selector changed"}],
                "repair_actions": [{"summary": "restored selector"}],
            },
        },
        "reflection": {"learning_decision": "running"},
    }


def test_grade_precedes_terminal_receipt_and_survives_failed_reflection(tmp_path: Path) -> None:
    calls = []

    def submit(result: dict, api: str) -> dict:
        assert not (tmp_path / "sdo_production_receipt_strict.json").exists()
        assert result["native_ledger"]["reflection"]["learning_decision"] == "running"
        calls.append(api)
        return {"mitigation": {"success": True}}

    grader = ClosureGrader(
        tmp_path, "http://conductor", submit=submit, now=lambda: datetime(2026, 10, 26, 0, 0, 40, tzinfo=timezone.utc)
    )
    evidence = ledger()
    grader.observe(evidence)
    evidence["reflection"] = {"learning_decision": "failed"}
    grader.observe(evidence)
    # A restarted adapter also retains the frozen grade, never resubmitting.
    ClosureGrader(tmp_path, "http://conductor", submit=submit).observe(evidence)
    grade = json.loads((tmp_path / "sdo_incident_grade.json").read_text())
    assert calls == ["http://conductor"]
    assert grade["grade"] == {"mitigation": {"success": True}}
    assert grade["closure_to_grade_seconds"] == 5
    assert grade["responder_runtime_seconds"] == 30
    assert grade["graded_at"] == "2026-10-26T00:00:40+00:00"


@pytest.mark.parametrize("result", [None, "invalid"])
def test_closure_without_result_submits_nothing(tmp_path: Path, result: object) -> None:
    evidence = ledger()
    evidence["closure"]["result"] = result
    grader = ClosureGrader(tmp_path, "http://conductor", submit=lambda *_: pytest.fail("no result"))
    grader.observe(evidence)
    assert not (tmp_path / "sdo_incident_grade.json").exists()


def test_submission_failure_is_recorded_without_retrying_ambiguous_post(tmp_path: Path) -> None:
    calls = []

    def submit(*_args: object) -> dict:
        calls.append(1)
        raise RuntimeError("conductor disconnected after POST")

    grader = ClosureGrader(tmp_path, "http://conductor", submit=submit)
    grader.observe(ledger())
    grader.observe(ledger())
    assert calls == [1]
    grade = json.loads((tmp_path / "sdo_incident_grade.json").read_text())
    assert grade["status"] == "failed"
    assert "disconnected" in grade["error"]


def test_closure_monitor_grades_while_lifecycle_waits_for_reflection(tmp_path: Path) -> None:
    from threading import Event

    from benchmarks.sregym.adapter.grading import ClosureGradingMonitor

    submitted = Event()
    terminal = Event()

    def submit(_result: dict, _api: str) -> dict:
        assert not terminal.is_set()
        submitted.set()
        return {"done": {"status": "done"}}

    grader = ClosureGrader(tmp_path, "http://conductor", submit=submit)
    with ClosureGradingMonitor(grader, read=lambda: ledger(), poll_seconds=0.01):
        assert submitted.wait(2), "submission must not wait for reflection/receipt"
        terminal.set()
    assert json.loads(grader.path.read_text())["status"] == "graded"


def test_monitor_final_sample_catches_fast_closure(tmp_path: Path) -> None:
    from threading import Event

    from benchmarks.sregym.adapter.grading import ClosureGradingMonitor

    sampled = Event()
    closed = Event()
    calls = []

    def read() -> dict | None:
        sampled.set()
        return ledger() if closed.is_set() else None

    grader = ClosureGrader(tmp_path, "http://conductor", submit=lambda *_: calls.append(1) or {})
    with ClosureGradingMonitor(grader, read=read, poll_seconds=30):
        assert sampled.wait(2)
        closed.set()
    assert calls == [1]


@pytest.mark.parametrize("reflection_failed", [False, True])
def test_native_monitor_uses_correlated_ledger_and_skips_previous_incident(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reflection_failed: bool
) -> None:
    import subprocess
    from threading import Event

    from benchmarks.sregym.adapter import driver
    from benchmarks.sregym.adapter.native_operator import NativeInstallConfig

    closed = Event()
    submitted = Event()
    snapshots = iter(
        [
            {"last_acknowledged_incident_id": "previous"},
            {
                **(
                    {"failed_closures": [{"closure": {"request": {"incident_id": "incident-1"}}}]}
                    if reflection_failed
                    else {"pending_closure": {"request": {"incident_id": "incident-1"}}}
                ),
                "last_acknowledged_incident_id": "previous",
            },
        ]
    )
    latest = {}

    def kubectl(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess:
        nonlocal latest
        latest = next(snapshots, latest)
        return subprocess.CompletedProcess([], 0, json.dumps({"data": {"runtime-state.json": json.dumps(latest)}}))

    def evidence(_config: object, incident_id: str) -> dict:
        assert incident_id == "incident-1"
        assert not closed.is_set(), "reflection has not finished"
        return {"ledger": ledger()}

    def submit(_receipt: dict, _base: str) -> dict:
        assert not closed.is_set()
        submitted.set()
        return {"mitigation": {"success": True}}

    monkeypatch.setattr(driver, "kubectl", kubectl)
    monkeypatch.setattr(driver, "read_incident_evidence", evidence)
    monkeypatch.setattr(driver, "_submit_recorded_result", submit)
    native = NativeInstallConfig(
        repository=tmp_path,
        namespace="hotel",
        control_namespace="hotel-sdo",
        application="hotel",
        controller_image="controller:test",
        responder_image="responder:test",
        validator_image="validator:test",
        repository_pvc="repo",
        credentials_secret="credentials",
        model="test",
        kubeconfig=tmp_path / "config",
    )
    with driver._closure_grading_monitor(native, "http://conductor", tmp_path):
        assert submitted.wait(2)
        closed.set()
    assert json.loads((tmp_path / "sdo_incident_grade.json").read_text())["incident_id"] == "incident-1"
