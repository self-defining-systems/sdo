"""Phase clocks must preserve scope, overlap and unknown evidence."""

import json
from pathlib import Path

import pytest

from benchmarks.sregym.analysis.encounter_timeline import build_timeline, load_evidence, main

FIXTURE = Path(__file__).parents[4] / "fixtures/benchmarks/sregym/encounter_timeline/evidence.json"


def evidence():
    data = json.loads(FIXTURE.read_text())
    data["responder_job_name"] = "responder-17"
    return data


def test_handwritten_fixture_preserves_overlapping_closure_and_responder():
    rows = {row.name: row for row in build_timeline(evidence())}
    assert rows["setup"].seconds == 270
    assert rows["injection_to_open"].seconds == 3
    assert rows["open_to_responder_start"].seconds == 5
    assert rows["responder"].seconds == 82
    assert rows["closure_verify"].seconds == 15
    assert rows["outcome_commit"].seconds == 1
    assert rows["reflection_worker"].seconds == 420
    assert rows["validator_job"].seconds == 60
    assert rows["controller_rebuild"].seconds is None
    assert rows["controller_rebuild"].status == "unknown"
    assert rows["responder"].end > rows["closure_verify"].end


def test_missing_completion_and_clock_reversal_never_become_zero():
    data = evidence()
    data["ledger"]["closure_phase_timestamps"].pop("validator_job_finished")
    rows = {row.name: row for row in build_timeline(data)}
    assert rows["validator_job"].status == "incomplete"
    assert rows["validator_job"].seconds is None
    data["ledger"]["closure_phase_timestamps"]["validator_job_finished"] = "2026-10-26T00:13:00Z"
    assert next(row for row in build_timeline(data) if row.name == "validator_job").status == "invalid"


def test_wrong_incident_ledger_is_rejected():
    data = evidence()
    data["ledger"]["incident_id"] = "other"
    with pytest.raises(ValueError, match="incident"):
        build_timeline(data)


def test_reader_deduplicates_lifecycle_and_reports_torn_tail(tmp_path):
    event = evidence()["lifecycle"][1]
    (tmp_path / "lifecycle-events.jsonl").write_text(json.dumps(event) + "\n" + json.dumps(event) + '\n{"torn":')
    loaded = load_evidence(tmp_path, "hotel-17", runtime=tmp_path)
    assert len(loaded["lifecycle"]) == 1
    assert loaded["warnings"]


def test_cli_writes_serializable_unknowns(tmp_path, capsys):
    assert main([str(tmp_path), "--incident-id", "hotel-17"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["schema_version"] == "sdo.encounter-timeline/v1"
    assert all(row["seconds"] is None for row in result["phases"])


def test_real_mcp_memory_checks_use_execution_clocks_and_reflection_scope(tmp_path):
    records = []
    for start, end in [(10_000, 15_000), (400_000, 410_000), (450_000, 460_000)]:
        records.append(
            {
                "type": "event_msg",
                "payload": {
                    "type": "item_completed",
                    "started_at_ms": start,
                    "completed_at_ms": end,
                    "item": {"type": "McpToolCall", "server": "sdo_reflection", "tool": "check_memory"},
                },
            }
        )
    path = tmp_path / "rollout.jsonl"
    path.write_text("\n".join(json.dumps(record) for record in records))
    data = load_evidence(tmp_path, "hotel-17", rollouts=[path])
    data["ledger"] = {
        "incident_id": "hotel-17",
        "closure_phase_timestamps": {
            "reflection_dispatch": "1970-01-01T00:06:00Z",
            "worker_result_written": "1970-01-01T00:08:00Z",
        },
    }
    rows = [row for row in build_timeline(data) if row.name.startswith("reflection_memory_check/")]
    assert [row.seconds for row in rows] == [10, 10]


def test_relaunch_marks_recorded_after_startup_cannot_time_rebuild():
    data = evidence()
    data["ledger"]["closure_phase_timestamps"].update(
        {"controller_restart_relaunch": "2026-10-26T00:16:00Z", "program_registry_rebuild": "2026-10-26T00:16:00Z"}
    )
    row = next(row for row in build_timeline(data) if row.name == "controller_rebuild")
    assert row.status == "unknown"


def test_stage_durations_are_elapsed_only_and_invalid_values_stay_invalid():
    data = evidence()
    data["stage_timings"] = [{"stage": "test ./...", "seconds": 9.2, "source": "validator.log"}]
    row = next(row for row in build_timeline(data) if row.name.startswith("validator_stage/"))
    assert row.seconds == 9.2
    assert row.start is None
    assert row.end is None
    data["stage_timings"][0]["seconds"] = -1
    assert next(row for row in build_timeline(data) if row.name.startswith("validator_stage/")).status == "invalid"


def test_saved_rows_keep_grading_delay_separate_and_require_recovery_completion(tmp_path):
    path = tmp_path / "incidents.jsonl"
    path.write_text(
        json.dumps(
            {
                "incident_id": "hotel-17",
                "graded_at": "2026-10-26T00:07:00Z",
                "fault_recovery_seconds": 0,
                "recovery_finished_at": None,
                "driver_phase_timings_seconds": {"lifecycle": 90},
            }
        )
        + "\n"
    )
    data = load_evidence(tmp_path, "hotel-17", rows=path)
    data.update({key: value for key, value in evidence().items() if key != "sources"})
    phases = {row.name: row for row in build_timeline(data)}
    assert phases["closure_to_grade"].seconds == 35
    assert phases["recovery"].seconds is None
    assert phases["driver/lifecycle"].seconds == 90


@pytest.mark.parametrize("prefix", ["level=INFO msg=validator_stage", "2026/10/26 00:14:20 INFO validator_stage"])
def test_machine_validator_stage_logs_are_scoped_and_keep_retries(tmp_path, prefix):
    path = tmp_path / "controller.log"
    path.write_text(f'2026-10-26T00:14:20Z {prefix} job=sdo-validator-x stage="test ./..." duration_ms=2500\n')
    data = load_evidence(tmp_path, "hotel-17", controller_log=path)
    assert data["stage_timings"][0]["seconds"] == 2.5
    assert data["stage_timings"][0]["job"] == "sdo-validator-x"


def test_warm_repeat_does_not_attribute_historical_validator_pods():
    data = evidence()
    data["audit"] = []  # Warm repeats have no install invocation clock.
    data["pods"] += [
        {"metadata": {"name": "sdo-validator-historical", "creationTimestamp": "2026-10-26T00:01:00Z"}},
        {"metadata": {"name": "sdo-validator-current", "creationTimestamp": "2026-10-26T00:14:00Z"}},
    ]
    names = [row.name for row in build_timeline(data)]
    assert not any("historical" in name for name in names)
    assert "validator_schedule/sdo-validator-current" in names
