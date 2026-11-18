"""Per-phase timings: the format the lane-overhead work reads."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.eval import PHASES, TIMINGS_SCHEMA, PhaseTimings

if TYPE_CHECKING:
    from pathlib import Path


class Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def test_phases_are_the_agreed_set_in_order() -> None:
    assert PHASES == ("claim_reset", "deploy", "inject", "page", "agent", "settle", "grade", "collect")


def test_phase_records_seconds_and_a_span() -> None:
    clock = Clock()
    timings = PhaseTimings(now=clock)
    with timings.phase("inject"):
        clock.now += 4
    with timings.phase("agent"):
        clock.now += 30
    document = timings.to_dict()
    assert document["schema"] == TIMINGS_SCHEMA
    assert document["phases"]["inject"] == 4
    assert document["phases"]["agent"] == 30
    assert document["spans"] == [
        {"phase": "inject", "start": 100.0, "end": 104.0, "seconds": 4.0},
        {"phase": "agent", "start": 104.0, "end": 134.0, "seconds": 30.0},
    ]
    assert document["total_seconds"] == 34
    assert document["overhead_seconds"] == 4  # everything outside the agent's own work


def test_a_repeated_phase_accumulates() -> None:
    clock = Clock()
    timings = PhaseTimings(now=clock)
    for _ in range(2):
        with timings.phase("settle"):
            clock.now += 2
    assert timings.to_dict()["phases"]["settle"] == 4


def test_an_unknown_phase_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown phase"):
        PhaseTimings().phase("warm_up")


def test_a_failing_phase_is_still_recorded() -> None:
    clock = Clock()
    timings = PhaseTimings(now=clock)

    def failing() -> None:
        with timings.phase("deploy"):
            clock.now += 5
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        failing()
    assert timings.to_dict()["phases"]["deploy"] == 5
    assert timings.to_dict()["spans"][0]["failed"] is True


def test_unrun_phases_are_present_as_null_so_overhead_is_not_misread() -> None:
    phases = PhaseTimings().to_dict()["phases"]
    assert list(phases) == list(PHASES)
    assert all(value is None for value in phases.values())


def test_write_is_atomic_json(tmp_path: Path) -> None:
    timings = PhaseTimings(now=Clock())
    timings.write(tmp_path / "timings.json")
    assert json.loads((tmp_path / "timings.json").read_text())["schema"] == TIMINGS_SCHEMA
    assert not list(tmp_path.glob("*.tmp"))


def test_seconds_measured_inside_an_arm_are_added_and_marked() -> None:
    timings = PhaseTimings(now=Clock())
    timings.add("claim_reset", 12.5)
    with pytest.raises(ValueError, match="negative"):
        timings.add("deploy", -1)
    document = timings.to_dict()
    assert document["phases"]["claim_reset"] == 12.5
    assert document["spans"][0]["measured_by"] == "arm"
    assert document["overhead_seconds"] == 12.5
