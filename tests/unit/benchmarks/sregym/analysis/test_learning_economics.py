"""Learning economics and the analysis CLI over synthetic cells built at test time."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from benchmarks.sregym.analysis.__main__ import main
from benchmarks.sregym.analysis.cells import load_cell
from benchmarks.sregym.analysis.learning_economics import faults_from_cells

if TYPE_CHECKING:
    from pathlib import Path

T0 = 1_800_000_000.0


def at(offset: float) -> str:
    return datetime.fromtimestamp(T0 + offset, UTC).isoformat().replace("+00:00", "Z")


def make_cell(
    root: Path,
    name: str,
    *,
    detected: float,
    verified: float,
    responder_cost: float | None,
    applied: float | None = None,
    learned: list[str] | None = None,
    reflection_cost: float = 0.0,
    bad: int = 10,
    stale: bool = False,
) -> Path:
    run = root / name / "runs" / "r1"
    inc = run / "operator" / "evidence" / "incidents" / "i1"
    inc.mkdir(parents=True)
    (run / "result.json").write_text(json.dumps({"ok": True, "inject_at": at(0)}))
    (run / "record.json").write_text(
        json.dumps({"recovered_in_window": True, "window": {"BadSeconds": bad}, "tail": {"BadSeconds": 0}})
    )
    usage = (
        {"input_tokens": "1000", "output_tokens": "100", "total_cost_usd": responder_cost}
        if responder_cost is not None
        else {}
    )
    stages = []
    if applied is not None:
        receipt = {"started_at": at(applied - 1), "completed_at": at(applied)}
        stages = [
            {
                "stage": "deterministic",
                "mitigations": [{"playbook_id": "pb", "apply_receipt": receipt}],
                "recovery_wait": {"exit_reason": "EXIT_REASON_CLEARED"},
            }
        ]
    outcome = {
        "classification": "success",
        "resolution_path": "responder" if responder_cost is not None else "deterministic",
        "usage": usage,
        "prior_stages": stages,
        "agent_changes": [{"at": at(detected + 60)}] if responder_cost is not None else [],
    }
    decision = (
        {
            "learning_decision": "updated",
            "playbook_ids": learned,
            "usage": {"input_tokens": "50", "output_tokens": "5", "total_cost_usd": reflection_cost},
        }
        if learned
        else {"learning_decision": "no_change"}
    )
    ledger = {
        "incident_id": "i1",
        "closure": {"detected_at": at(detected - 1000 if stale else detected), "verified_at": at(verified)},
        "outcome_document": outcome,
        "reflection_decision": decision,
        "closure_phase_timestamps": {"reflection_dispatch": at(verified), "reflection_committed": at(verified + 90)},
    }
    (inc / "ledger.json").write_text(json.dumps(ledger))
    return root / name


def fixture(root: Path) -> list[Path]:
    return [
        make_cell(
            root, "fx-a-abcdef12", detected=20, verified=320, responder_cost=2.0, learned=["pb"], reflection_cost=0.5
        ),
        make_cell(root, "fx-b-abcdef12", detected=20, verified=120, responder_cost=None, applied=30, bad=2),
        make_cell(root, "fx-b2-abcdef12", detected=20, verified=170, responder_cost=None, applied=30, bad=4),
    ]


def test_cell_timeline_and_costs(tmp_path: Path) -> None:
    first = load_cell(fixture(tmp_path)[0])
    inc = first.first_incident
    assert inc is not None
    assert inc.page_to_first_change == 60
    assert inc.page_to_recovered == 300
    assert inc.responder.cost_usd == 2.0
    assert inc.reflection.cost_usd == 0.5
    assert inc.reflection_seconds == 90
    assert inc.learned_playbook_ids == ["pb"]


def test_stale_ledgers_from_before_inject_are_ignored(tmp_path: Path) -> None:
    cell = make_cell(tmp_path, "old-a-abcdef12", detected=20, verified=100, responder_cost=1.0, stale=True)
    assert load_cell(cell).incidents == []


def test_economics_ratios_and_break_even(tmp_path: Path) -> None:
    faults = faults_from_cells(fixture(tmp_path))
    assert len(faults) == 1
    fault = faults[0]
    assert fault.time_ratio == (100 + 150) / 2 / 300
    assert fault.cost_ratio == 0.0
    assert fault.saved_cost == 2.0
    assert fault.break_even == 0.25
    assert fault.repeats[0].incident is not None
    assert fault.repeats[0].incident.page_to_applied == 10
    assert fault.repeats[0].served_by_playbook


def test_cli_all_writes_markdown_and_json(tmp_path: Path) -> None:
    cells = fixture(tmp_path)
    out = tmp_path / "analysis"
    assert main(["all", *map(str, cells), "--out", str(out)]) == 0
    data = json.loads((out / "learning-economics.json").read_text())
    assert data["aggregate"]["all"]["median_time_ratio"] == 250 / 2 / 300
    assert "Learning economics" in (out / "learning-economics.md").read_text()
    assert (out / "sweep.md").is_file()


def test_times_and_costs_come_from_the_evidence_manifest(tmp_path: Path) -> None:
    from benchmarks.fuzzops import cellevidence

    cell = make_cell(
        tmp_path, "m-a-abcdef12", detected=20, verified=320, responder_cost=2.0, learned=["pb"], reflection_cost=0.5
    )
    run = next((cell / "runs").iterdir())
    cellevidence.write_manifest(run)
    manifest = run / "operator" / cellevidence.MANIFEST
    raw = json.loads(manifest.read_text())
    raw["timelines"][0]["responder_cost_usd"] = 9.0
    manifest.write_text(json.dumps(raw))
    inc = load_cell(cell).first_incident
    assert inc is not None
    assert inc.responder.cost_usd == 9.0
    assert inc.page_to_recovered == 300
