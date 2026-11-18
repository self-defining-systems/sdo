"""Phase attribution for an SDO cell and a vanilla cell (no SDO timeline) built from synthetic records."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.analysis.__main__ import main
from benchmarks.sregym.analysis.cells import load_cell
from benchmarks.sregym.analysis.phases import attribute
from tests.unit.benchmarks.sregym.analysis.test_detection_reports import decorate, entry
from tests.unit.benchmarks.sregym.analysis.test_learning_economics import at, make_cell

if TYPE_CHECKING:
    from pathlib import Path


def tool_use(offset: float, call_id: str, command: str) -> dict:
    block = {"type": "tool_use", "id": call_id, "name": "Bash", "input": {"command": command}}
    return {"type": "assistant", "timestamp": at(offset), "message": {"content": [block]}}


def tool_result(offset: float, call_id: str) -> dict:
    block = {"type": "tool_result", "tool_use_id": call_id, "content": "ok"}
    return {"type": "user", "timestamp": at(offset), "message": {"content": [block]}}


TRAJECTORY = [
    {"type": "assistant", "timestamp": at(30), "message": {"content": [{"type": "text", "text": "looking"}]}},
    tool_use(35, "a", "kubectl get pods"),
    tool_result(37, "a"),
    tool_use(50, "b", "kubectl patch deploy x -p '{}'"),
    tool_result(55, "b"),
    {"type": "assistant", "timestamp": at(60), "message": {"content": [{"type": "text", "text": "done"}]}},
]


def write_trajectory(directory: Path) -> None:
    directory.mkdir(parents=True)
    (directory / "s.jsonl").write_text("\n".join(json.dumps(x) for x in TRAJECTORY))


def test_sdo_cell_marks_segments_and_agent_span(tmp_path: Path) -> None:
    cell = make_cell(tmp_path, "p-a-abcdef12", detected=20, verified=100, responder_cost=1.0)
    decorate(cell, first_bad=2, timeline=[entry("request-metrics", "request-metrics.errors", 20)])
    run = next((cell / "runs").iterdir())
    record = json.loads((run / "record.json").read_text())
    record["last_bad_second"] = 70
    (run / "record.json").write_text(json.dumps(record))
    write_trajectory(run / "operator" / "evidence" / "incidents" / "i1" / "claude" / "projects" / "p")
    p = attribute(load_cell(cell))
    assert p.mode == "sdo"
    assert p.marks["harm_onset"] == 2
    assert p.marks["page"] == 20
    assert p.marks["first_action"] == 35
    assert p.marks["first_change"] == 80  # the outcome record's first change (make_cell: detected + 60)
    assert p.marks["first_change_traj"] == 50
    assert p.marks["users_recovered"] == 70
    assert p.marks["closed"] == 100
    assert sum(s for *_, s in p.segments) == 98
    assert p.main["model"] > 0
    assert any("tool: kubectl" in k for k in p.main)


def test_vanilla_cell_derives_start_and_first_change_and_flags_it(tmp_path: Path) -> None:
    run = tmp_path / "v-a-abcdef12" / "runs" / "r1"
    run.mkdir(parents=True)
    (run / "result.json").write_text(json.dumps({"ok": True, "inject_at": at(0)}))
    (run / "record.json").write_text(
        json.dumps(
            {
                "recovered_in_window": True,
                "last_bad_second": 58,
                "lifecycle": {"first_bad_second": 3},
                "window": {"BadSeconds": 55},
            }
        )
    )
    write_trajectory(run / "agent")
    p = attribute(load_cell(tmp_path / "v-a-abcdef12"))
    assert p.mode == "vanilla"
    assert p.marks["start"] == 30
    assert p.marks["first_change"] == 50
    assert p.marks["page"] is None
    assert any("heuristic" in f for f in p.flags)


def test_cli_phases_report(tmp_path: Path) -> None:
    cell = make_cell(tmp_path, "p-a-abcdef12", detected=20, verified=100, responder_cost=1.0)
    out = tmp_path / "out"
    assert main(["phases", str(cell), "--out", str(out)]) == 0
    assert json.loads((out / "phases.json").read_text())[0]["cell"] == "p-a-abcdef12"
