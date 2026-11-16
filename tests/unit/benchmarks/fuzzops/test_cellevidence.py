"""The per-cell evidence manifest and validator, on a minimal synthetic cell."""

from __future__ import annotations

import gzip
import json
from typing import TYPE_CHECKING

import pytest

from benchmarks.fuzzops import cellevidence as ce

if TYPE_CHECKING:
    from pathlib import Path

INC = "ab" * 16  # incident directory name (sha256 prefix)
SID = "11111111-2222-3333-4444-555555555555"
AGENT = "a0123456789abcdef"
T = "2026-01-01T00:00:{:02d}Z"


def _w(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value if isinstance(value, str) else json.dumps(value))


def make_cell(root: Path) -> Path:
    run = root / "exp" / "runs" / "r1"
    op = run / "operator"
    _w(root / "exp" / "spec.lock.yaml", "{}")
    _w(run / "run.json", {"id": "r1", "fault": "f", "lane": "l", "host": "h", "fuzzops": {"revision": "abc"}})
    window = {"Requests": 100, "BadSeconds": 5}
    _w(
        run / "record.json",
        {
            "window": window,
            "tail": {"BadSeconds": 0},
            "timing": {"window": 10.0},
            "interference": {"before": {}},
            "window_seconds": 50.0,
            "recovered_in_window": True,
        },
    )
    _w(op / "collect.json", {"fault": "f", "interval": [T.format(0), T.format(59)], "parts": {"memory": "ok"}})
    _w(op / "controller-state.json", {"a": 1})
    _w(op / "controller.log", "line\n")
    _w(op / ce.BUILD, {"sdo_revision": "s" * 40, "baseline_commit": "b" * 40})
    _w(op / ce.HOST_LOAD, {"load1": 0.1})
    _w(op / "evidence/layout.json", {"schema": "sdo.evidence/v1"})
    _w(op / "evidence/index.jsonl", '{"a": 1}\n')
    _w(op / "transcripts.json", {"incidents": [], "missing": []})
    _w(
        op / ce.OBSERVABILITY,
        {
            "prometheus": {"status": "absent"},
            "jaeger": {"status": "absent"},
            "applogs": {"status": "ok", "files": ["applogs/a/p.log.gz"]},
        },
    )
    (op / "applogs/a").mkdir(parents=True)
    (op / "applogs/a/p.log.gz").write_bytes(gzip.compress(b"x\n"))
    inc = op / "evidence/incidents" / INC
    _w(inc / "incident.json", {"incident_id": "i1"})
    _w(inc / "seal.json", {})
    ledger = {
        "incident_id": "i1",
        "closure": {
            "detected_at": T.format(1),
            "dispatched_at": T.format(2),
            "health_cleared_at": T.format(8),
            "verified_at": T.format(9),
        },
        "closure_phase_timestamps": {
            "closure_start": T.format(9),
            "closure_ack": T.format(12),
            "reflection_dispatch": T.format(12),
            "reflection_committed": T.format(20),
        },
        "reflection_dispatch_at": T.format(12),
        "reflection_decision": {"learning_decision": "updated", "playbook_ids": ["p"]},
        "outcome_document": {
            "classification": "success",
            "resolution_path": "responder",
            "usage": {"total_cost_usd": 1.5},
            "agent_changes": [{"at": T.format(5)}],
            "findings": [],
            "applied_playbooks": [],
        },
    }
    _w(inc / "ledger.json", ledger)
    _w(op / f"memory/.sdo/outcomes/{INC}ff.json", {"agent_changes": [], "findings": []})
    _w(
        inc / "claude-turns.jsonl",
        json.dumps({"event": "complete", "role": "responder", "session_id": SID, "usage": {"total_cost_usd": 1.5}})
        + "\n",
    )
    _w(inc / "reflection-usage.jsonl", json.dumps({"session_id": SID, "cost_usd": 0.2, "duration_seconds": 8.0}) + "\n")
    proj = inc / "claude/projects/p"
    _w(proj / f"{SID}.jsonl", json.dumps({"toolUseResult": f"agentId: {AGENT}"}) + "\n")
    _w(proj / SID / "subagents" / f"agent-{AGENT}.jsonl", '{"type": "user"}\n')
    return run


def test_complete_cell_passes_and_manifest_is_atomic(tmp_path: Path) -> None:
    run = make_cell(tmp_path)
    ev = ce.write_manifest(run)
    assert ev.problems == [], ev.problems
    again = ce.load(run)
    assert again.grader.journeys_per_second == 2.0
    assert again.timelines[0].closing_turn_cost_usd == pytest.approx(0.2)
    assert again.timelines[0].first_agent_change_at == T.format(5)
    assert again.agents[0].subagents
    assert again.agents[0].subagents_expected == 1
    assert not list((run / "operator").glob(".*.tmp"))


@pytest.mark.parametrize(
    ("damage", "expect"),
    [
        (lambda op: (op / "controller.log").write_text(""), "controller.log truncated"),
        (lambda op: (op / "controller-state.json").write_text('{"a"'), "controller-state.json truncated"),
        (lambda op: (op / ce.BUILD).unlink(), "build.json missing"),
        (lambda op: (op / "applogs/a/p.log.gz").unlink(), "observability applogs missing"),
        (
            lambda op: _w(op / ce.OBSERVABILITY, {"prometheus": {"status": "error", "detail": "down"}}),
            "observability prometheus incomplete",
        ),
        (lambda op: next((op / "evidence/incidents").rglob("agent-*.jsonl")).unlink(), "subagent a0123456 missing"),
        (
            lambda op: next((op / "evidence/incidents").glob("*/claude/projects/p/*.jsonl")).unlink(),
            "session 11111111 missing",
        ),
        (
            lambda op: next((op / "evidence/incidents").glob("*/claude/projects/p/*.jsonl")).write_text(
                '{"a": 1}\n{"b"'
            ),
            "session 11111111 truncated",
        ),
        (lambda op: (op / "evidence/incidents" / INC / "reflection-usage.jsonl").unlink(), "closing turn"),
        (lambda op: (op / "memory/.sdo/outcomes" / f"{INC}ff.json").unlink(), "outcome"),
    ],
)
def test_damage_fails_loudly(tmp_path: Path, damage, expect: str) -> None:
    run = make_cell(tmp_path)
    damage(run / "operator")
    ev = ce.build(run)
    assert any(expect in p for p in ev.problems), ev.problems
    assert ce.verdict(ev).startswith("evidence incomplete: ")


def test_collect_part_error_and_waiver(tmp_path: Path) -> None:
    run = make_cell(tmp_path)
    op = run / "operator"
    _w(op / "collect.json", {"interval": [], "parts": {"logs": "error: boom"}})
    assert any("collect part logs" in p for p in ce.build(run).problems)
    _w(op / ce.WAIVER, {"waive": ["collect part logs"], "reason": "known"})
    ev = ce.build(run)
    assert ev.problems == []
    assert ev.waived
    assert "known" in ev.waived[0]
    _w(op / ce.WAIVER, {"waive": ["collect part logs"]})  # a waiver needs a reason
    assert ce.build(run).problems


def test_atomic_write_leaves_no_partial_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "x.json"
    ce.atomic_write(target, "old")

    def boom(*_a: object) -> None:
        raise OSError("disk")

    monkeypatch.setattr(ce.os, "replace", boom)
    with pytest.raises(OSError, match="disk"):
        ce.atomic_write(target, "new")
    assert target.read_text() == "old"
    assert [p.name for p in tmp_path.iterdir()] == ["x.json"]


def test_cells_finds_runs(tmp_path: Path) -> None:
    run = make_cell(tmp_path)
    assert ce.cells(tmp_path / "exp") == [run]
    assert ce.cells(tmp_path) == [run]


def test_trial_needs_session_and_subagent_files(tmp_path: Path) -> None:
    trial = tmp_path / "trial0"
    _w(trial / "transcript.jsonl", json.dumps({"session_id": SID, "result": f"agentId: {AGENT}"}) + "\n")
    found = ce.trial_problems(trial)
    assert any("session 11111111 missing" in p for p in found)
    assert any("subagent a0123456 missing" in p for p in found)
    _w(trial / "session-dir/p" / f"{SID}.jsonl", "{}\n")
    _w(trial / "session-dir/p" / SID / "subagents" / f"agent-{AGENT}.jsonl", "{}\n")
    assert ce.trial_problems(trial) == []


def test_finish_runs_analysis_only_when_every_cell_is_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import subprocess

    calls: list[list[str]] = []
    monkeypatch.setattr(subprocess, "run", lambda cmd, **_: calls.append(cmd) or subprocess.CompletedProcess(cmd, 0))
    run = make_cell(tmp_path)
    assert ce.finish([tmp_path / "exp"]) == 0
    assert calls
    assert calls[0][-2:] == ["all", str(tmp_path / "exp")]
    calls.clear()
    (run / "operator" / "controller.log").write_text("")
    assert ce.finish([tmp_path / "exp"]) == 1
    assert not calls
    assert "analysis not run" in capsys.readouterr().out


@pytest.mark.parametrize("reason", ["closing_turn_nothing_to_script", "accepted_programs_no_new_evidence"])
def test_a_closing_turn_go_skipped_needs_no_usage_or_session(tmp_path: Path, reason: str) -> None:
    run = make_cell(tmp_path)
    inc = run / "operator/evidence/incidents" / INC
    ledger = json.loads((inc / "ledger.json").read_text())
    ledger["reflection_decision"] = {"learning_decision": "no_change", "no_change_reason": reason}
    ledger["reflection_results"] = [ledger["reflection_decision"]]
    ledger["closure_phase_timestamps"].pop("reflection_committed")
    ledger["closure_phase_timestamps"]["reflection_without_commit"] = T.format(12)
    _w(inc / "ledger.json", ledger)
    (inc / "reflection-usage.jsonl").unlink()
    assert ce.build(run).problems == []
