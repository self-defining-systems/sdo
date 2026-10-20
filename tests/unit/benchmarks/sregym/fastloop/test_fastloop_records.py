from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from benchmarks.sregym.fastloop.records import (
    IncidentRecord,
    OracleVerdict,
    TokenCounts,
    append_record,
    load_records,
    summarize,
)

if TYPE_CHECKING:
    from pathlib import Path

T0 = datetime(2026, 9, 27, 12, 0, 0, tzinfo=timezone.utc)


def _record(index: int, *, agent: str = "sdo", warm: bool | None = True, passed: bool = True) -> IncidentRecord:
    return IncidentRecord(
        run_id="run-1",
        index=index,
        agent=agent,
        problem_id="missing_configmap_hotel_reservation",
        model="gpt-6-luna",
        injection_started_at=T0,
        injection_finished_at=T0 + timedelta(seconds=6),
        detected_at=T0 + timedelta(seconds=8) if agent == "sdo" else None,
        mitigation_applied_at=T0 + timedelta(seconds=40),
        resolved_at=T0 + timedelta(seconds=70),
        oracle=OracleVerdict(kind="sregym-mitigation-oracle", success=passed, details={}),
        responder_tokens=TokenCounts(input_tokens=1000, cached_input_tokens=800, output_tokens=50),
        reflection_tokens=TokenCounts(input_tokens=5000, cached_input_tokens=4000, output_tokens=100),
        warm_path=warm,
        reflection_seconds=30.0 if agent == "sdo" else None,
        incident_wall_seconds=150.0,
    )


def test_latencies_are_derived_from_injection_start() -> None:
    record = _record(0)

    assert record.injection_to_detection_seconds == pytest.approx(8.0)
    assert record.detection_to_mitigation_seconds == pytest.approx(32.0)
    assert record.injection_to_mitigation_seconds == pytest.approx(40.0)


def test_baseline_without_detector_has_no_detection_latency() -> None:
    record = _record(0, agent="codex", warm=None)

    assert record.injection_to_detection_seconds is None
    assert record.detection_to_mitigation_seconds is None
    assert record.injection_to_mitigation_seconds == pytest.approx(40.0)


def test_unknown_agent_and_naive_timestamps_are_rejected() -> None:
    with pytest.raises(ValidationError):
        IncidentRecord.model_validate(_record(0).model_dump() | {"agent": "claude"})
    with pytest.raises(ValidationError):
        IncidentRecord.model_validate(
            _record(0).model_dump() | {"injection_started_at": datetime(2026, 9, 27, 12, 0, 0)}
        )


def test_records_round_trip_through_jsonl_with_derived_fields(tmp_path: Path) -> None:
    path = tmp_path / "incidents.jsonl"
    append_record(path, _record(0))
    append_record(path, _record(1, warm=False))

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    # Derived latencies are persisted so other tools need not recompute them.
    assert json.loads(lines[0])["injection_to_mitigation_seconds"] == pytest.approx(40.0)
    assert [record.index for record in load_records(path)] == [0, 1]
    with path.with_suffix(".csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    assert rows[0]["injection_to_mitigation_seconds"] == "40.0"


def test_timing_events_and_native_usage_survive_record_round_trip(tmp_path: Path) -> None:
    record = _record(0).model_copy(
        update={
            "responder_started_at": T0 + timedelta(seconds=10),
            "responder_finished_at": T0 + timedelta(seconds=39),
            "oracle_finished_at": T0 + timedelta(seconds=80),
            "reflection_started_at": T0 + timedelta(seconds=82),
            "reflection_finished_at": T0 + timedelta(seconds=100),
            "rebuild_done_at": T0 + timedelta(seconds=105),
            "recovery_finished_at": T0 + timedelta(seconds=90),
        }
    )
    path = tmp_path / "incidents.jsonl"
    append_record(path, record)

    assert load_records(path)[0].rebuild_done_at == T0 + timedelta(seconds=105)
    assert TokenCounts.from_usage({"inputTokens": "20", "outputTokens": "3"}).total_tokens == 23


def test_summary_separates_agents_and_reports_warm_and_reflection() -> None:
    records = [_record(0, warm=False), _record(1), _record(2), _record(3, agent="codex", warm=None, passed=False)]

    summary = {row.agent: row for row in summarize(records)}

    assert summary["sdo"].incidents == 3
    assert summary["sdo"].oracle_passed == 3
    assert summary["sdo"].warm_path_fired == 2
    assert summary["sdo"].median_reflection_seconds == pytest.approx(30.0)
    assert summary["sdo"].median_responder_tokens == 1050
    assert summary["codex"].incidents == 1
    assert summary["codex"].oracle_passed == 0
    assert summary["codex"].warm_path_fired == 0
    assert summary["codex"].median_reflection_seconds is None
