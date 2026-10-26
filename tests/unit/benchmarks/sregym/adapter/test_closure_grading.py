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
