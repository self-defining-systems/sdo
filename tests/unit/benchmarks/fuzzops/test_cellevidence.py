"""The per-cell evidence manifest and validator, on a minimal synthetic cell."""

from __future__ import annotations

import gzip
import json
import shutil
from datetime import UTC, datetime
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


@pytest.mark.parametrize(
    "reason",
    [
        "closing_turn_nothing_to_script",
        "accepted_programs_no_new_evidence",
        "failed_no_learning",
        "cleared_without_action_no_learning",
    ],
)
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


def test_an_earlier_cells_leftover_incident_directory_is_outside_the_cell(tmp_path: Path) -> None:
    run = make_cell(tmp_path)
    op = run / "operator"
    _w(op / "collect.json", {"interval": ["2026-01-01T00:00:00Z", T.format(59)], "parts": {}})
    left = op / "evidence/incidents" / ("c" * 32)
    before = int(datetime(2025, 12, 31, tzinfo=UTC).timestamp() * 1e9)
    _w(left / "incident.json", {"incident_id": f"fg1-{before}"})
    ev = ce.build(run)
    assert ev.excluded == ["c" * 32]
    assert not any("ccccccccccc" in p for p in ev.problems)


def test_an_incident_still_open_at_collect_has_no_ledger_to_demand(tmp_path: Path) -> None:
    run = make_cell(tmp_path)
    op = run / "operator"
    _w(op / "collect.json", {"interval": ["2026-01-01T00:00:00Z", T.format(59)], "parts": {}})
    openi = op / "evidence/incidents" / ("d" * 32)
    _w(openi / "incident.json", {"incident_id": f"fg1-{int(datetime(2026, 1, 1, 0, 5, tzinfo=UTC).timestamp() * 1e9)}"})
    ev = ce.build(run)
    assert ev.open_incidents == ["d" * 32]
    assert ev.problems == []
    _w(openi / "seal.json", {})  # sealed means a ledger was owed
    assert any("ledger.json" in p for p in ce.build(run).problems)


def test_a_cell_is_complete_at_collect_before_the_manager_writes_run_json(tmp_path: Path) -> None:
    """collect runs inside the step; the manager writes run.json only after the step's directory is moved."""
    run = make_cell(tmp_path)
    (run / "run.json").unlink()
    assert "run.json" in " ".join(ce.build(run).problems)  # a later validate expects it
    pending = ce.build(run, run_pending=True)
    assert pending.problems == []
    assert pending.complete


def test_a_failed_closing_turn_has_an_end_time_and_is_not_missing_evidence(tmp_path: Path) -> None:
    """The turn ran and ended (the broker stamped reflection_failed); its failure is recorded, not a recording gap."""
    run = make_cell(tmp_path)
    inc = run / "operator/evidence/incidents" / INC
    ledger = json.loads((inc / "ledger.json").read_text())
    ledger["reflection_decision"] = {"learning_decision": "failed", "failure_reason": "agent_failed", "error": "x"}
    ledger["closure_phase_timestamps"].pop("reflection_committed")
    ledger["closure_phase_timestamps"]["reflection_failed"] = T.format(30)
    _w(inc / "ledger.json", ledger)
    ev = ce.build(run)
    assert not any("closing turn" in p for p in ev.problems)
    assert ev.timelines[0].closing_turn_end_at == T.format(30)


def test_collects_verdict_on_the_manifest_does_not_keep_a_repaired_cell_incomplete(tmp_path: Path) -> None:
    """collect.json records 'evidence-manifest' and the blocked 'reset' as errors when collect found gaps; once the
    gaps are repaired `validate` must be able to pass, so those two parts are not evidence themselves."""
    run = make_cell(tmp_path)
    collect = json.loads((run / "operator/collect.json").read_text())
    collect["parts"] = {
        "memory": "ok",
        "evidence-manifest": "error: evidence incomplete: run.json missing",
        "reset": "error: blocked, fix the missing evidence",
    }
    _w(run / "operator/collect.json", collect)
    assert ce.build(run).problems == []
    collect["parts"]["reset"] = "error: incidents reset did not acknowledge"
    collect["parts"]["memory"] = "error: pod gone"
    _w(run / "operator/collect.json", collect)
    problems = " ".join(ce.build(run).problems)
    assert "collect part reset" in problems
    assert "collect part memory" in problems


def test_copy_problems_of_a_complete_sink_copy_are_empty(tmp_path: Path) -> None:
    evidence = make_cell(tmp_path) / "operator" / "evidence"
    assert ce.copy_problems(evidence) == {}


@pytest.mark.parametrize(
    ("remove", "expect"),
    [
        (f"incidents/{INC}/claude/projects/p/{SID}.jsonl", "session"),
        (f"incidents/{INC}/claude/projects/p/{SID}/subagents/agent-{AGENT}.jsonl", "subagent"),
    ],
)
def test_copy_problems_name_the_incident_whose_transcript_is_missing(tmp_path: Path, remove: str, expect: str) -> None:
    evidence = make_cell(tmp_path) / "operator" / "evidence"
    (evidence / remove).unlink()
    problems = ce.copy_problems(evidence)
    assert list(problems) == [f"incidents/{INC}"]
    assert any(expect in text for text in problems[f"incidents/{INC}"])


def test_copy_problems_flag_an_incident_whose_agents_ran_with_no_transcript(tmp_path: Path) -> None:
    evidence = make_cell(tmp_path) / "operator" / "evidence"
    for path in (evidence / "incidents" / INC / "claude").rglob("*.jsonl"):
        path.unlink()
    (evidence / "incidents" / INC / "claude-turns.jsonl").unlink()
    (evidence / "incidents" / INC / "reflection-usage.jsonl").unlink()
    assert ce.copy_problems(evidence)[f"incidents/{INC}"]


def test_copy_problems_flag_a_started_investigator_that_left_no_session_but_not_an_idle_one(tmp_path: Path) -> None:
    evidence = make_cell(tmp_path) / "operator" / "evidence"
    idle = evidence / "incidents" / ("e" * 32)
    _w(idle / "incident.json", {"incident_id": "i1-inv-2"})
    assert ce.copy_problems(evidence) == {}, "a prepared investigator that never started has nothing to collect"
    started = evidence / "incidents" / ("f" * 32)
    _w(started / "incident.json", {"incident_id": "i1-inv-1"})
    _w(started / "claude-turns.jsonl", json.dumps({"event": "start", "role": "responder", "session_id": ""}) + "\n")
    assert list(ce.copy_problems(evidence)) == ["incidents/" + "f" * 32]


def test_copy_problems_cover_lifecycle_tasks_and_the_layout(tmp_path: Path) -> None:
    evidence = make_cell(tmp_path) / "operator" / "evidence"
    task = evidence / "lifecycle" / "designer-1"
    _w(task / "claude-turns.jsonl", json.dumps({"event": "complete", "role": "agent", "session_id": SID}) + "\n")
    assert list(ce.copy_problems(evidence)) == ["lifecycle/designer-1"]
    (evidence / "layout.json").unlink()
    assert "" in ce.copy_problems(evidence)
    assert ce.copy_problems(evidence / "nowhere") == {
        "": ["no sink copy: " + str(evidence / "nowhere" / "layout.json") + " does not exist"]
    }


def test_cell_problems_check_each_arms_own_transcript_layout(tmp_path: Path) -> None:
    sdo = tmp_path / "sdo"
    run = make_cell(sdo)
    cell = run / "operator"  # the sink copy sits in <cell>/evidence
    assert ce.cell_problems(cell, "sdo") == []
    (cell / "evidence" / "incidents" / INC / "claude" / "projects" / "p" / f"{SID}.jsonl").unlink()
    assert any("session" in text for text in ce.cell_problems(cell, "sdo"))
    assert ce.cell_problems(tmp_path / "empty", "sdo"), "a cell with no sink copy is incomplete"
    vanilla = tmp_path / "vanilla"
    _w(vanilla / "transcript.jsonl", json.dumps({"type": "system", "session_id": SID}) + "\n")
    assert any("missing" in text for text in ce.cell_problems(vanilla, "vanilla"))
    _w(vanilla / "session-dir" / "projects" / "p" / f"{SID}.jsonl", "{}\n")
    assert ce.cell_problems(vanilla, "vanilla") == []
    with pytest.raises(ValueError, match="unknown arm"):
        ce.cell_problems(vanilla, "codex")


def test_an_unclosed_incident_detected_before_the_run_is_carry_over_not_a_missing_outcome(tmp_path: Path) -> None:
    run = make_cell(tmp_path)
    op = run / "operator"
    _w(op / "collect.json", {"interval": ["2026-01-01T00:00:00Z", T.format(59)], "parts": {}})
    stale = op / "evidence/incidents" / ("6af1d744d2fa" + "0" * 20)
    before = int(datetime(2025, 12, 31, 15, 24, 38, tzinfo=UTC).timestamp() * 1e9)
    _w(stale / "incident.json", {"incident_id": f"hb1-{before}"})
    _w(stale / "ledger.json", {"incident_id": f"hb1-{before}", "closure": {"detected_at": "2025-12-31T15:24:38Z"}})
    ev = ce.build(run)
    assert ev.excluded == [stale.name]
    assert ev.problems == []


def _declare(inc: Path, *models: str) -> None:
    """The resolved-agent records the controller writes: one per role, naming the model it ran."""
    roles = ("responder", "reflection")
    rows = [{"role": role, "model": model, "provider": "codex"} for role, model in zip(roles, models, strict=False)]
    _w(inc / "agent-resolved.jsonl", "".join(json.dumps(r) + "\n" for r in rows))


def _without_cost_and_session(run: Path) -> Path:
    """What a scripted stand-in leaves: no cost on the responder or closing turn, no Claude session file."""
    inc = run / "operator/evidence/incidents" / INC
    ledger = json.loads((inc / "ledger.json").read_text())
    ledger["outcome_document"]["usage"] = {}
    _w(inc / "ledger.json", ledger)
    _w(
        inc / "reflection-usage.jsonl",
        json.dumps({"session_id": SID, "cost_usd": None, "duration_seconds": 1.0}) + "\n",
    )
    (inc / "claude-turns.jsonl").unlink()
    shutil.rmtree(inc / "claude")
    return inc


def test_a_declared_stub_agent_needs_no_cost_or_session_and_the_manifest_says_stub(tmp_path: Path) -> None:
    run = make_cell(tmp_path)
    _declare(_without_cost_and_session(run), "stub", "stub")
    ev = ce.write_manifest(run)
    assert ev.problems == [], ev.problems
    assert ev.stub is True
    loaded = ce.load(run)
    assert loaded.stub is True
    assert loaded.timelines[0].stub is True
    assert loaded.agents
    assert all(a.stub for a in loaded.agents)


def test_an_undeclared_agent_without_cost_or_session_still_fails(tmp_path: Path) -> None:
    run = make_cell(tmp_path)
    inc = _without_cost_and_session(run)
    for models in ((), ("gpt-real", "gpt-real"), ("stub", "gpt-real")):  # none declared, real, mixed: strict
        _declare(inc, *models)
        ev = ce.build(run)
        assert ev.stub is False
        assert any("responder usage" in p and "no total_cost_usd" in p for p in ev.problems), (models, ev.problems)
        assert any("session 11111111 missing" in p for p in ev.problems), (models, ev.problems)


def test_a_codex_session_is_found_by_its_rollout_file(tmp_path: Path) -> None:
    run = make_cell(tmp_path)
    inc = run / "operator/evidence/incidents" / INC
    shutil.rmtree(inc / "claude")
    _w(inc / f"codex/sessions/2026/01/01/rollout-2026-01-01T00-00-00-{SID}.jsonl", '{"type": "session_meta"}\n')
    ev = ce.build(run)
    assert not any("session" in p for p in ev.problems), ev.problems
    assert ev.agents[0].path is not None
