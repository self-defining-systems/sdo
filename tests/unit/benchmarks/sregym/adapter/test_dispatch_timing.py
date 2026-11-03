from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

from benchmarks.sregym.analysis.regrade import regrade_row


def test_dispatch_uses_earliest_stage_and_first_successful_completion() -> None:
    row = {
        "incident_id": "synthetic",
        "injection": {"state": "injected", "started_at": "2026-01-01T00:00:00Z"},
        "detected_at": "2026-01-01T00:00:01Z",
        "resolved_at": "2026-01-01T00:00:40Z",
    }
    receipt = {
        "incident_id": "synthetic",
        "native_ledger": {
            "outcome_document": {
                "timestamps": {"dispatched_at": "2026-01-01T00:00:20Z"},
                "prior_stages": [{"stage": "deterministic", "timing": {"started_at": "2026-01-01T00:00:10Z"}}],
                "repair_actions": [
                    {"success": False, "completed_at": "2026-01-01T00:00:21Z"},
                    {"success": True, "completed_at": "2026-01-01T00:00:30Z"},
                    {"success": True, "completed_at": "2026-01-01T00:00:25Z"},
                ],
            }
        },
    }
    result = regrade_row(row, receipt)
    assert result["dispatched_at"] == "2026-01-01T00:00:10+00:00"
    assert result["time_to_fix_seconds"] == 15
    assert result["time_to_resolution_from_dispatch_seconds"] == 30
    assert result["injection_to_dispatch_seconds"] == 10


def test_detection_is_never_a_dispatch_fallback() -> None:
    result = regrade_row({"detected_at": "2026-01-01T00:00:10Z"}, {})
    assert result["dispatched_at"] is None
    assert result["time_to_fix_seconds"] is None
    assert "dispatch" in result["timing_missing_reasons"]["time_to_fix_seconds"]


def test_playbooks_only_completion_and_missing_first_repair() -> None:
    result = regrade_row(
        {
            "ablation_arm": "playbooks-only",
            "responder_started_at": "2026-01-01T00:00:10Z",
            "responder_finished_at": "2026-01-01T00:00:30Z",
            "mitigation_applied_at": "2026-01-01T00:00:25Z",
        },
        {},
    )
    assert result["time_to_resolution_from_dispatch_seconds"] == 20
    # Legacy mitigation_applied_at was the LAST successful repair, not the first.
    assert result["time_to_fix_seconds"] is None


def test_invalid_or_reversed_timing_stays_unknown() -> None:
    result = regrade_row(
        {
            "dispatched_at": "2026-01-01T00:00:10Z",
            "first_mitigation_applied_at": "2026-01-01T00:00:05Z",
            "resolved_at": "not a timestamp",
        },
        {},
    )
    assert result["time_to_fix_seconds"] is None
    assert result["timing_missing_reasons"]["time_to_fix_seconds"] == "endpoint precedes dispatch"


def test_historical_dispatch_alias_requires_lifecycle_and_stage_start() -> None:
    row = {
        "incident_id": "synthetic",
        "detected_at": "2026-01-01T00:00:10Z",
        "responder_started_at": "2026-01-01T00:00:10Z",
    }
    receipt = {
        "incident_id": "synthetic",
        "lifecycle_events": [
            {"incident_id": "synthetic", "event": "responder_dispatched", "recorded_at": "2026-01-01T00:00:20Z"}
        ],
        "native_ledger": {
            "outcome_document": {
                "timestamps": {"detected_at": "2026-01-01T00:00:10Z", "dispatched_at": "2026-01-01T00:00:10Z"}
            }
        },
    }
    assert regrade_row(row, receipt)["dispatched_at"] == "2026-01-01T00:00:20+00:00"
    receipt["native_ledger"]["outcome_document"]["prior_stages"] = [{"stage": "deterministic"}]
    result = regrade_row(row, receipt)
    assert result["dispatched_at"] is None
    assert result["timing_missing_reasons"]["dispatched_at"] == "deterministic stage start not recorded"


def test_saved_batch_joins_incident_scoped_telemetry(tmp_path: Path) -> None:
    import csv
    import json

    from benchmarks.sregym.analysis.regrade import regrade_batch

    run = tmp_path / "lanes/lane1/results/synthetic-playbooks-only"
    run.mkdir(parents=True)
    artifacts = run / "agent"
    telemetry = artifacts / "sdo_runtime/telemetry"
    telemetry.mkdir(parents=True)
    (telemetry / "lifecycle-events.jsonl").write_text(
        "\n".join(
            json.dumps(event)
            for event in [
                {"incident_id": "other", "event": "responder_dispatched", "recorded_at": "2026-01-01T00:00:01Z"},
                {"incident_id": "synthetic", "event": "responder_dispatched", "recorded_at": "2026-01-01T00:00:10Z"},
            ]
        )
    )
    (run / "incidents.jsonl").write_text(
        json.dumps(
            {
                "incident_id": "synthetic",
                "artifacts_dir": str(artifacts),
                "injection": {"state": "injected", "started_at": "2026-01-01T00:00:00Z"},
                "first_mitigation_applied_at": "2026-01-01T00:00:20Z",
                "responder_finished_at": "2026-01-01T00:00:30Z",
            }
        )
    )
    output = tmp_path / "analysis"
    regrade_batch(tmp_path, output)
    with (output / "regraded.csv").open() as stream:
        result = next(csv.DictReader(stream))
    assert result["time_to_fix_seconds"] == "10.0"
    assert result["time_to_resolution_from_dispatch_seconds"] == "20.0"
    assert "10.0 (n=1)" in (output / "regraded-summary.md").read_text()


def test_final_verified_application_supplies_clock_after_partial_stage_handoff() -> None:
    """Final application evidence survives a stale unsuccessful repair summary."""
    from benchmarks.sregym.analysis.dispatch_timing import recover_dispatch_timing

    application = {
        "applied": True,
        "verification_completed": True,
        "verification_passed": True,
        "final_equivalence_verified": True,
        "final_verification_basis": "recovery-settled",
        "completed_at": "2026-01-01T00:00:17Z",
    }
    row = {"incident_id": "synthetic", "dispatched_at": "2026-01-01T00:00:10Z"}
    receipt = {
        "incident_id": "synthetic",
        "native_ledger": {
            "closure": {
                "result": {
                    "repair_actions": [{"completed_at": "2026-01-01T00:00:17Z"}],
                    "plan_applications": [application],
                }
            }
        },
    }
    assert recover_dispatch_timing(row, receipt).time_to_fix_seconds == 7
    for field, value in [
        ("rolled_back", True),
        ("rollback_error", "conflict"),
        ("error", "failed"),
        ("verification_passed", False),
        ("verification_completed", False),
        ("final_equivalence_verified", False),
        ("rollout_pending", {"pending": True}),
        ("settlement_rejection_reason", "SETTLEMENT_REJECTION_REASON_INTERFERENCE"),
    ]:
        previous = application.copy()
        application[field] = value
        assert recover_dispatch_timing(row, receipt).time_to_fix_seconds is None, field
        application.clear()
        application.update(previous)
