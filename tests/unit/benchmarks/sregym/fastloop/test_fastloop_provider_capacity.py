"""The real capacity producer feeds timing and grading consumers."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from benchmarks.sregym.fastloop.records import IncidentRecord, summarize
from benchmarks.sregym.protocol.provider_capacity import read_provider_waits
from libs.agent_cli.provider_capacity import ProviderCapacity, RateLimitPolicy

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_recorded_capacity_waits_are_clipped_and_excluded_from_ttm(tmp_path: Path) -> None:
    seconds = [0.0]

    def sleep(duration: float) -> None:
        seconds[0] += duration

    capacity = ProviderCapacity(
        environment={"SDO_PROVIDER_WAIT_LOG": str(tmp_path / "provider-waits.jsonl"), "SDO_INCIDENT_ID": "incident-1"},
        monotonic=lambda: seconds[0],
        now=lambda: T0 + timedelta(seconds=seconds[0]),
        sleep=sleep,
    )
    capacity.backoff(0)
    with capacity.measured_wait("concurrency"):
        sleep(20)
    evidence = read_provider_waits(tmp_path, incident_id="incident-1")
    record = IncidentRecord(
        run_id="r",
        index=0,
        agent="sdo",
        model="opus",
        problem_id="p",
        injection={
            "state": "injected",
            "started_at": T0 + timedelta(seconds=10),
            "finished_at": T0 + timedelta(seconds=10),
        },
        detected_at=T0 + timedelta(seconds=15),
        mitigation_applied_at=T0 + timedelta(seconds=40),
        resolved_at=T0 + timedelta(seconds=60),
        provider_waits=evidence.intervals,
    )
    assert record.injection_to_mitigation_seconds == 0  # raw 30s minus 20s backoff and 10s admission
    assert record.detection_to_mitigation_seconds == 0
    assert record.injection_to_resolution_seconds == 10
    assert record.rate_limit_wait_seconds == 20
    assert record.concurrency_wait_seconds == 20
    assert record.raw_injection_to_mitigation_seconds == 30
    assert IncidentRecord.model_validate_json(record.model_dump_json()) == record


def test_exhausted_wait_is_never_counted_as_an_sdo_failure(tmp_path: Path) -> None:
    seconds = [0.0]
    capacity = ProviderCapacity(
        environment={"SDO_PROVIDER_WAIT_LOG": str(tmp_path / "provider-waits.jsonl")},
        policy=RateLimitPolicy(initial_delay_seconds=1, max_delay_seconds=1, max_wait_seconds=1),
        monotonic=lambda: seconds[0],
        now=lambda: T0 + timedelta(seconds=seconds[0]),
        sleep=lambda value: seconds.__setitem__(0, seconds[0] + value),
    )
    with pytest.raises(RuntimeError, match="rate_limited"):
        capacity.backoff(0)
    evidence = read_provider_waits(tmp_path)
    assert evidence.exhausted
    record = IncidentRecord(
        run_id="r",
        index=0,
        agent="sdo",
        model="opus",
        problem_id="p",
        injection={"state": "injected", "started_at": T0, "finished_at": T0},
        infra_error="rate_limited",
        provider_waits=evidence.intervals,
    )
    assert record.grade_status == "NOT-GRADED"
    assert record.not_graded_reason == "rate_limited"
    assert summarize([record])[0].incidents == 0
    assert summarize([record])[0].errors == 0
    assert json.loads(record.model_dump_json())["grade_status"] == "NOT-GRADED"


def test_cumulative_export_does_not_subtract_peer_or_reflection_waits(tmp_path: Path) -> None:
    path = tmp_path / "provider-waits.jsonl"
    capacity = ProviderCapacity(
        environment={"SDO_PROVIDER_WAIT_LOG": str(path), "SDO_INCIDENT_ID": "peer", "SDO_PHASE": "reflection"}
    )
    with capacity.measured_wait("rate_limit"):
        pass
    evidence = read_provider_waits(tmp_path, incident_id="ours")
    assert not evidence.intervals
    assert not evidence.exhausted


def test_outer_deadline_after_recorded_wait_is_infrastructure_but_other_errors_are_not() -> None:
    from benchmarks.sregym.protocol.provider_capacity import (
        ProviderWaitEvidence,
        ProviderWaitInterval,
        capacity_prevented_completion,
    )

    interval = ProviderWaitInterval(kind="rate_limit", started_at=T0, finished_at=T0 + timedelta(seconds=30))
    evidence = ProviderWaitEvidence((interval,))
    assert capacity_prevented_completion(evidence, "context deadline exceeded")
    assert not capacity_prevented_completion(evidence, "invalid agent result")
    assert not capacity_prevented_completion(ProviderWaitEvidence(), "context deadline exceeded")
