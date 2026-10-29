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


def test_current_row_sums_all_episodes_once_and_excludes_prior_row_exports(tmp_path: Path) -> None:
    import shutil

    evidence(tmp_path, "current")
    evidence(tmp_path, "followup")
    evidence(tmp_path, "previous", offset=-86400)
    shutil.copytree(tmp_path / "export", tmp_path / "duplicate")
    row = {
        "provider": "claude",
        "model": MODEL,
        "incident_id": "current",
        "results": str(tmp_path),
        "injection_started_at": "2026-10-28T00:00:00Z",
    }
    reconcile_claude_row(row)
    assert row["cost_complete"]
    assert len(row["cost_episodes"]) == 2
    assert row["responder_usage"]["usd"] == 0.00164
    assert row["reflection_usage"]["usd"] == 0.00164


def test_legacy_complete_guard_rejects_cumulative_dollars(tmp_path: Path) -> None:
    fragment = hashlib.sha256(b"current").hexdigest()[:32]
    directory = tmp_path / "export" / "sdo_runtime" / "sessions" / fragment
    usage = {
        "input_tokens": 330,
        "uncached_input_tokens": 100,
        "cache_read_input_tokens": 200,
        "cache_write_input_tokens": 30,
        "cache_write_1h_input_tokens": 10,
        "output_tokens": 10,
        "total_cost_usd": 0.00082,
    }
    records = [
        {"event": "start", "role": "reflection", "recorded_at_epoch": 1},
        {"event": "complete", "role": "reflection", "recorded_at_epoch": 2, "usage": usage},
    ]
    path = directory / "trial2-turns.jsonl"
    write(path, records)
    row = {"provider": "claude", "model": MODEL, "incident_id": "current", "results": str(tmp_path)}
    reconcile_claude_row(row)
    assert row["reflection_usage"]["usd_complete"]
    assert row["reflection_usage"]["usd"] == 0.00082
    usage["total_cost_usd"] = 0.00164
    write(path, records)
    reconcile_claude_row(row)
    assert not row["reflection_usage"]["usd_complete"]
    assert row["reflection_usage"]["usd"] is None


def test_shared_session_handoff_never_bills_a_request_in_both_phases(tmp_path: Path) -> None:
    fragment = hashlib.sha256(b"current").hexdigest()[:32]
    directory = tmp_path / "export" / "sdo_runtime" / "sessions" / fragment
    records = [
        assistant("a"),
        state(1, 0.00082),
        assistant("b", timestamp="2026-10-28T00:00:03Z"),
        state(2, 0.00164),
        assistant("c", timestamp="2026-10-28T00:00:05Z"),
        state(3, 0.00246),
    ]
    write(directory / "claude" / "projects" / "synthetic" / "shared.jsonl", records)
    write(
        directory / "trial2-turns.jsonl",
        [
            {"event": event, "role": role, "recorded_at_epoch": 1793145600 + timestamp, "session_id": "shared"}
            for event, role, timestamp in [
                ("start", "responder", 0),
                ("start", "reflection", 2),
                ("complete", "responder", 4),
                ("complete", "reflection", 6),
            ]
        ],
    )
    row = {"provider": "claude", "model": MODEL, "incident_id": "current", "results": str(tmp_path)}
    reconcile_claude_row(row)
    assert row["cost_complete"]
    assert row["responder_usage"]["usd"] == 0.00164
    assert abs(row["reflection_usage"]["usd"] - 0.00082) < 1e-10
    assert row["reflection_usage"]["tokens"]["model_requests"] == 1


def receipt_evidence(
    root: Path,
    *,
    deterministic: bool = False,
    session: str | None = None,
    usage: dict | None = None,
    skipped: bool = False,
) -> None:
    incident = "current"
    value = {
        "incident_id": incident,
        "responder_job_evidence": [],
        "native_ledger": {
            "incident_id": incident,
            "closure": {
                "request": {"incident_id": incident},
                "verified_at": "2026-10-28T00:00:05Z",
                "result": {
                    "incident_id": incident,
                    "responder_session_id": session,
                    "resolution_path": "deterministic" if deterministic else "responder",
                    "usage": usage or {},
                },
            },
            "outcome_document": {"incident_id": incident},
            "reflection_skipped_reason": "no_session" if skipped else "",
        },
    }
    path = root / "000_synthetic" / "sdo_production_receipt_strict.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def test_receipt_proves_both_roles_model_free_without_session_exports(tmp_path: Path) -> None:
    receipt_evidence(tmp_path, deterministic=True, skipped=True)
    row = {"provider": "claude", "model": MODEL, "incident_id": "current", "results": str(tmp_path)}
    reconcile_claude_row(row)
    for role in ("responder", "reflection"):
        assert row[role + "_usage"]["cost_state"] == "model-free"
        assert row[role + "_usage"]["usd"] == 0
        assert row[role + "_model_dispatched"] is False
    assert row["cost_complete"]


def test_peer_billing_never_proves_absent_responder_without_receipt(tmp_path: Path) -> None:
    directory = tmp_path / "sdo_runtime" / "sessions" / hashlib.sha256(b"current").hexdigest()[:32]
    write(
        directory / "claude-turns.jsonl",
        [
            {"event": "start", "role": "reflection", "recorded_at_epoch": 1793145602},
            {"event": "complete", "role": "reflection", "recorded_at_epoch": 1793145604},
        ],
    )
    write(
        directory / "claude" / "projects" / "synthetic" / "shared.jsonl",
        [
            assistant("a", timestamp="2026-10-28T00:00:03Z"),
            state(1, 0.00082),
        ],
    )
    row = {"provider": "claude", "model": MODEL, "incident_id": "current", "results": str(tmp_path)}
    reconcile_claude_row(row)
    assert row["responder_usage"]["usd"] is None
    assert row["responder_usage"]["cost_state"] == "missing"
    assert row["reflection_usage"]["cost_state"] == "measured"
    assert row["reflection_model_dispatched"] is True
    assert row["responder_model_dispatched"] is None
    receipt_evidence(tmp_path, deterministic=True)
    reconcile_claude_row(row)
    assert row["responder_usage"]["cost_state"] == "model-free"
    assert row["reflection_usage"]["cost_state"] == "measured"
    assert row["responder_model_dispatched"] is False


def test_native_session_or_partial_usage_overrules_deterministic_zero_claim(tmp_path: Path) -> None:
    for session, usage in [("paid", {}), (None, {"model_requests": "9", "partial": True, "total_cost_usd": 0.20118})]:
        receipt_evidence(tmp_path, deterministic=True, session=session, usage=usage)
        row = {"provider": "claude", "model": MODEL, "incident_id": "current", "results": str(tmp_path)}
        reconcile_claude_row(row)
        assert row["responder_usage"]["usd"] is None
        assert row["responder_usage"]["cost_state"] == "missing"
        assert row["responder_model_dispatched"] is True


def test_native_reflection_precheck_is_worker_dispatch_without_model_dispatch(tmp_path: Path) -> None:
    receipt_evidence(tmp_path, deterministic=True)
    path = tmp_path / "000_synthetic" / "sdo_production_receipt_strict.json"
    value = json.loads(path.read_text())
    ledger = value["native_ledger"]
    ledger["reflection_dispatch_at"] = "2026-10-28T00:00:06Z"
    decision = {"learning_decision": "no_change", "no_change_reason": "native_memory_check_no_change"}
    ledger["reflection_decision"] = decision
    ledger["reflection_results"] = [decision]
    path.write_text(json.dumps(value))
    row = {"provider": "claude", "model": MODEL, "incident_id": "current", "results": str(tmp_path)}
    reconcile_claude_row(row)
    assert row["reflection_usage"]["cost_state"] == "model-free"
    assert row["reflection_model_dispatched"] is False
    # A previous paid attempt makes the whole role paid, even if its final retry
    # was completed by the deterministic native check.
    ledger["reflection_results"].insert(0, {"usage": {"model_requests": "1"}})
    path.write_text(json.dumps(value))
    reconcile_claude_row(row)
    assert row["reflection_usage"]["cost_state"] == "missing"
    assert row["reflection_model_dispatched"] is True


def test_partial_zero_or_invalid_native_usage_never_proves_no_model(tmp_path: Path) -> None:
    for usage in [{"partial": True}, {"model_requests": "invalid"}, {"input_tokens": -1}]:
        receipt_evidence(tmp_path, deterministic=True, usage=usage)
        row = {"provider": "claude", "model": MODEL, "incident_id": "current", "results": str(tmp_path)}
        reconcile_claude_row(row)
        assert row["responder_usage"]["usd"] is None
        assert row["responder_usage"]["cost_state"] == "missing"
        assert row["responder_model_dispatched"] is None


def test_model_journal_overrules_native_precheck_zero_and_missing_receipt_identity(tmp_path: Path) -> None:
    from benchmarks.sregym.analysis.stream_accounting import summarize_pairs

    receipt_evidence(tmp_path, deterministic=True, skipped=True)
    directory = tmp_path / "sdo_runtime" / "sessions" / hashlib.sha256(b"current").hexdigest()[:32]
    write(directory / "claude-turns.jsonl", [{"event": "start", "role": "responder", "recorded_at_epoch": 1}])
    row = {
        "provider": "claude",
        "model": MODEL,
        "incident_id": "current",
        "results": str(tmp_path),
        "status": "settled",
        "injection_started_at": "2026-10-28T00:00:00Z",
        "mount_sha": "synthetic",
    }
    reconcile_claude_row(row)
    assert row["responder_usage"]["cost_state"] == "missing"
    assert row["responder_model_dispatched"] is True
    # Real reconciler output flows into the real summarizer: no hand-written
    # cost states on the consumer side of this contract.
    group = summarize_pairs([{"problem_id": "mf__toy__n1__seed", "first": row, "repeat": row}])[0]
    assert group["metrics"]["responder_usd"]["ratio"]["missing"] == 1
    assert group["metrics"]["reflection_usd"]["ratio"]["zero_denominator"] == 1
    path = tmp_path / "000_synthetic" / "sdo_production_receipt_strict.json"
    value = json.loads(path.read_text())
    value["native_ledger"]["outcome_document"]["incident_id"] = "another-incident"
    path.write_text(json.dumps(value))
    reconcile_claude_row(row)
    assert row["reflection_usage"]["cost_state"] == "missing"
    assert row["reflection_model_dispatched"] is None


def test_unidentified_claude_row_cannot_keep_default_zero_counters(tmp_path: Path) -> None:
    row = {"provider": "claude", "results": str(tmp_path), "responder_usage": {"usd": 0, "usd_complete": True}}
    reconcile_claude_row(row)
    assert row["responder_usage"]["cost_state"] == "missing"
    assert row["responder_usage"]["usd"] is None
    assert row["responder_usage"]["transport_usage"]["usd"] == 0
    assert row["responder_model_dispatched"] is None
    assert not row["cost_complete"]


def test_followup_billing_cannot_hide_missing_original_incident(tmp_path: Path) -> None:
    evidence(tmp_path, "followup")
    row = {
        "provider": "claude",
        "model": MODEL,
        "incident_id": "missing-original",
        "results": str(tmp_path),
        "injection_started_at": "2026-10-28T00:00:00Z",
    }
    reconcile_claude_row(row)
    assert not row["cost_complete"]
    assert row["responder_usage"]["cost_state"] == "missing"
    assert row["responder_usage"]["usd"] is None
    assert row["reflection_usage"]["usd"] is None
    assert row["responder_model_dispatched"] is True  # The followup really ran.
    assert len(row["cost_episodes"]) == 2
