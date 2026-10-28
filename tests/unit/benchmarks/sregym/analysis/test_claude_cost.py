"""Phase accounting contracts, using synthetic TRIAL2 evidence shapes."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

from benchmarks.sregym.analysis.claude_cost import reconcile_claude_row
from tests.unit.libs.agent_cli.test_claude_usage import MODEL, assistant, state


def write(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(record) + "\n" for record in records))


def evidence(root: Path, incident: str, offset: int = 0) -> None:
    fragment = hashlib.sha256(incident.encode()).hexdigest()[:32]
    session = root / "export" / "sdo_runtime" / "sessions" / fragment
    events = [
        {"event": event, "role": role, "recorded_at_epoch": 1793145600 + timestamp + offset, "session_id": incident}
        for event, role, timestamp in [
            ("start", "responder", 0),
            ("start", "reflection", 2),
            ("complete", "reflection", 4),
        ]
    ]
    write(session / "trial2-turns.jsonl", events)
    records = [
        assistant("a" + incident),
        state(1, 0.00082),
        assistant("b" + incident, timestamp="2026-10-28T00:00:03Z"),
        state(2, 0.00164),
    ]
    write(session / "claude" / "projects" / "synthetic" / f"{incident}.jsonl", records)


def test_cancelled_responder_and_resumed_reflection_reconcile_export_copies(tmp_path: Path) -> None:
    evidence(tmp_path, "current")
    # Same export copied a second time must never multiply either phase.
    import shutil

    shutil.copytree(tmp_path / "export", tmp_path / "duplicate")
    row = {"provider": "claude", "model": MODEL, "incident_id": "current", "results": str(tmp_path)}
    reconcile_claude_row(row)
    assert row["cost_complete"]
    assert row["responder_usage"]["usd"] == 0.00082
    assert row["reflection_usage"]["usd"] == 0.00082
    assert row["reflection_usage"]["tokens"]["model_requests"] == 1


def test_codex_rows_unchanged_and_missing_evidence_is_unknown(tmp_path: Path) -> None:
    row = {"provider": "codex", "results": str(tmp_path), "incident_id": "x"}
    reconcile_claude_row(row)
    assert row == {"provider": "codex", "results": str(tmp_path), "incident_id": "x"}
    row["provider"] = "claude"
    reconcile_claude_row(row)
    assert not row["cost_complete"]
    assert row["responder_usage"]["usd"] is None


def test_standard_incident_report_uses_reconciled_usd_and_completeness(tmp_path: Path) -> None:
    from benchmarks.sregym.analysis.incident_cost import build_report, load_sdo_pipeline
    from tests.unit.benchmarks.sregym.analysis.test_incident_cost import PROBLEM_A, _sdo_stage

    root = tmp_path / "pipeline"
    stage = _sdo_stage(root, 0, "first", PROBLEM_A, responder={}, reflection={}, warm=False, incident_detectors=0)
    # experiment_model reads the resolved provider/model from this config.
    (stage / "experiment_config.toml").write_text(
        '[runner]\nagent = "sdo_codex"\n[agent.sdo_codex]\nprovider = "claude"\nmodel = "claude-opus-5-5"\n'
    )
    receipt_path = next(stage.rglob("sdo_production_receipt_strict.json"))
    receipt = json.loads(receipt_path.read_text())
    receipt["incident_id"] = "current"
    receipt_path.write_text(json.dumps(receipt))
    evidence(receipt_path.parent.parent, "current")
    report = build_report(load_sdo_pipeline(root), [])
    assert report.stages[0].cost_complete
    assert report.stages[0].responder_summary.usd == 0.00082
    assert report.stages[0].reflection_summary.usd == 0.00082
