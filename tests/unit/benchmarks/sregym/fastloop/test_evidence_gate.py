"""A fastloop cell whose expected transcripts are missing is marked not gradable, never silently graded."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from benchmarks.fuzzops import sink
from benchmarks.sregym.fastloop import cli, evidence_gate, evidence_sink
from benchmarks.sregym.fastloop.records import IncidentRecord, OracleVerdict, load_records, write_records

if TYPE_CHECKING:
    from pathlib import Path

T0 = datetime(2026, 9, 27, 12, 0, 0, tzinfo=timezone.utc)
SID = "11111111-2222-3333-4444-555555555555"
AGENT = "a0123456789abcdef"


def _row(index: int, incident_id: str | None, *, agent: str = "sdo") -> IncidentRecord:
    return IncidentRecord(
        run_id="r1",
        index=index,
        agent=agent,
        problem_id="p",
        model="m",
        injection={"state": "injected", "started_at": T0, "finished_at": T0 + timedelta(seconds=6)},
        oracle=OracleVerdict(kind="sregym-mitigation-oracle", success=True, details={}),
        incident_id=incident_id,
    )


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _incident(copy: Path, incident_id: str, *, transcripts: bool = True, sidechain: bool = True) -> Path:
    directory = copy / "incidents" / sink.incident_dir_name(incident_id)
    _write(directory / "incident.json", json.dumps({"incident_id": incident_id}))
    _write(
        directory / "claude-turns.jsonl",
        json.dumps({"event": "complete", "role": "responder", "session_id": SID, "usage": {}}) + "\n",
    )
    if transcripts:
        main = directory / "claude" / "projects" / "p" / f"{SID}.jsonl"
        _write(main, json.dumps({"toolUseResult": f"agentId: {AGENT}"}) + "\n")
        if sidechain:
            _write(main.with_suffix("") / "subagents" / f"agent-{AGENT}.jsonl", '{"type": "user"}\n')
    return directory


def _results(tmp_path: Path, rows: list[IncidentRecord], *, copy: bool = True) -> Path:
    results = tmp_path / "results" / "r1"
    write_records(results / "incidents.jsonl", rows)
    if copy:
        _write(results / "evidence" / "layout.json", json.dumps({"schema": "sdo.evidence/v1"}))
        _write(results / "evidence-copy.json", "{}")
    return results


def _graded(results: Path) -> list[str]:
    return [r.grade_status for r in load_records(results / "incidents.jsonl")]


def test_a_cell_with_every_transcript_stays_gradable(tmp_path: Path) -> None:
    results = _results(tmp_path, [_row(0, "inc-1")])
    _incident(results / "evidence", "inc-1")
    assert evidence_gate.apply(results) == []
    assert _graded(results) == ["GRADED"]


def test_a_missing_sidechain_makes_only_its_own_row_not_gradable(tmp_path: Path) -> None:
    results = _results(tmp_path, [_row(0, "inc-1"), _row(1, "inc-2")])
    _incident(results / "evidence", "inc-1", sidechain=False)
    _incident(results / "evidence", "inc-2")
    reasons = evidence_gate.apply(results)
    assert [r.index for r in reasons] == [0]
    rows = load_records(results / "incidents.jsonl")
    assert [r.grade_status for r in rows] == ["NOT-GRADED", "GRADED"]
    assert rows[0].not_graded_reason is not None
    assert rows[0].not_graded_reason.startswith("evidence_incomplete: ")
    assert "subagent" in rows[0].not_graded_reason


def test_an_incident_with_no_copied_directory_is_not_gradable(tmp_path: Path) -> None:
    results = _results(tmp_path, [_row(0, "inc-1")])
    _incident(results / "evidence", "someone-else")
    evidence_gate.apply(results)
    assert _graded(results) == ["NOT-GRADED"]


def test_an_investigators_missing_transcript_makes_its_parents_row_not_gradable(tmp_path: Path) -> None:
    results = _results(tmp_path, [_row(0, "inc-1")])
    _incident(results / "evidence", "inc-1")
    directory = results / "evidence" / "incidents" / sink.incident_dir_name("inc-1-inv-1")
    _write(directory / "incident.json", json.dumps({"incident_id": "inc-1-inv-1"}))
    _write(
        directory / "claude-turns.jsonl", json.dumps({"event": "start", "role": "responder", "session_id": ""}) + "\n"
    )
    evidence_gate.apply(results)
    assert _graded(results) == ["NOT-GRADED"]


def test_a_lifecycle_transcript_missing_makes_every_row_not_gradable(tmp_path: Path) -> None:
    results = _results(tmp_path, [_row(0, "inc-1"), _row(1, "inc-2")])
    _incident(results / "evidence", "inc-1")
    _incident(results / "evidence", "inc-2")
    _write(
        results / "evidence" / "lifecycle" / "designer-1" / "claude-turns.jsonl",
        json.dumps({"event": "complete", "role": "agent", "session_id": SID}) + "\n",
    )
    evidence_gate.apply(results)
    assert _graded(results) == ["NOT-GRADED", "NOT-GRADED"]


def test_a_run_without_a_copy_or_with_a_waiver_is_not_gradable(tmp_path: Path) -> None:
    no_copy = _results(tmp_path / "a", [_row(0, "inc-1")], copy=False)
    evidence_gate.apply(no_copy)
    assert _graded(no_copy) == ["NOT-GRADED"]
    waived = _results(tmp_path / "b", [_row(0, "inc-1")])
    _incident(waived / "evidence", "inc-1")
    (waived / evidence_sink.WAIVER_FILENAME).write_text("{}")
    evidence_gate.apply(waived)
    assert _graded(waived) == ["NOT-GRADED"]


def test_the_baseline_arm_needs_no_sdo_transcripts(tmp_path: Path) -> None:
    results = _results(tmp_path, [_row(0, None, agent="codex")], copy=False)
    assert evidence_gate.apply(results) == []
    assert _graded(results) == ["GRADED"]


def test_a_row_that_never_opened_an_incident_has_no_transcript_of_its_own_to_miss(tmp_path: Path) -> None:
    results = _results(tmp_path, [_row(0, None)])
    assert evidence_gate.apply(results) == []
    assert _graded(results) == ["GRADED"]


def test_applying_the_gate_twice_gives_the_same_verdict(tmp_path: Path) -> None:
    results = _results(tmp_path, [_row(0, "inc-1")])
    _incident(results / "evidence", "inc-1", transcripts=False)
    first = evidence_gate.apply(results)
    second = evidence_gate.apply(results)
    assert [r.reason for r in first] == [r.reason for r in second]
    assert _graded(results) == ["NOT-GRADED"]


def test_the_gate_announces_the_rows_it_refused_to_grade(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    results = _results(tmp_path, [_row(0, "inc-1")])
    assert evidence_gate.announce(evidence_gate.apply(results)) is False
    assert "NOT GRADED" in capsys.readouterr().err


def _lane_with_sink(tmp_path: Path):  # type: ignore[no-untyped-def]
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    lane = FastloopEnvironment(
        run_dir=tmp_path / "lane",
        cluster="eval-w0",
        kubeconfig=tmp_path / "config",
        namespace="toy",
        application="toy",
        workspace=tmp_path / "lane" / "application_workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        evidence_root=tmp_path / "sink",
    )
    lane.workspace.mkdir(parents=True)
    return lane


def _finish(lane, results: Path, agent: str = "sdo") -> bool:  # type: ignore[no-untyped-def]
    import argparse

    return cli._finish_evidence(argparse.Namespace(agent=agent), lane, results)


def test_a_finished_run_copies_the_sink_then_grades_only_rows_with_their_transcripts(tmp_path: Path) -> None:
    lane = _lane_with_sink(tmp_path)
    results = lane.run_dir / "results" / "r1"
    write_records(results / "incidents.jsonl", [_row(0, "inc-1"), _row(1, "inc-2")])
    run = lane.evidence_root / evidence_sink.lane_evidence_run_id(lane, results)  # type: ignore[operator]
    _write(run / "layout.json", json.dumps({"schema": "sdo.evidence/v1", "run_id": run.name, "created_at": "x"}))
    _incident(run, "inc-1")
    _incident(run, "inc-2", sidechain=False)
    assert _finish(lane, results) is False
    assert _graded(results) == ["GRADED", "NOT-GRADED"]
    assert (results / "evidence-copy.json").is_file()


def test_a_run_whose_sink_cannot_be_copied_is_not_gradable_and_fails(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    lane = _lane_with_sink(tmp_path)
    results = lane.run_dir / "results" / "r1"
    write_records(results / "incidents.jsonl", [_row(0, "inc-1")])
    assert _finish(lane, results) is False
    assert _graded(results) == ["NOT-GRADED"]
    assert "evidence copy FAILED" in capsys.readouterr().err


def test_the_baseline_arm_is_not_copied_or_gated(tmp_path: Path) -> None:
    lane = _lane_with_sink(tmp_path)
    results = lane.run_dir / "results" / "r1"
    write_records(results / "incidents.jsonl", [_row(0, None, agent="codex")])
    assert _finish(lane, results, agent="codex") is True
    assert _graded(results) == ["GRADED"]
