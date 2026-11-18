"""The independent prober and the retroactive hold rule that define time to mitigate."""

from __future__ import annotations

import json
import threading
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.eval import HealthAssessment, Prober, ProbeResult, ProbeSample, assess_health

if TYPE_CHECKING:
    from pathlib import Path


def samples(*pairs: tuple[float, bool]) -> list[ProbeSample]:
    return [ProbeSample(at=at, healthy=healthy) for at, healthy in pairs]


def test_healthy_since_is_the_start_of_the_last_unbroken_run_that_lasts_the_hold() -> None:
    run = samples(
        (0, False), (3, False), (6, True), (9, True), (12, False), (15, True), (18, True), (21, True), (24, True)
    )
    result = assess_health(run, hold_seconds=9, max_gap_seconds=5)
    assert result.healthy_at == 15
    assert result.held_seconds == 9
    assert result.first_unhealthy_at == 0
    assert result.flaps == 1  # one healthy run broke before the final one


def test_a_run_shorter_than_the_hold_is_not_healthy() -> None:
    run = samples((0, False), (3, True), (6, True))
    result = assess_health(run, hold_seconds=60, max_gap_seconds=5)
    assert result.healthy_at is None
    assert result.held_seconds == 3


def test_a_late_failure_moves_the_time_back_retroactively() -> None:
    held = samples((0, True), (3, True), (6, True), (9, True))
    assert assess_health(held, hold_seconds=6, max_gap_seconds=5).healthy_at == 0
    broke = [*held, *samples((12, False), (15, True), (18, True), (21, True), (24, True))]
    assert assess_health(broke, hold_seconds=6, max_gap_seconds=5).healthy_at == 15


def test_a_gap_in_sampling_breaks_the_run_because_unknown_is_not_healthy() -> None:
    run = samples((0, True), (3, True), (40, True), (43, True), (46, True))
    result = assess_health(run, hold_seconds=6, max_gap_seconds=5)
    assert result.healthy_at == 40
    assert result.gaps == 1


def test_no_samples_gives_no_time() -> None:
    assert assess_health([], hold_seconds=5, max_gap_seconds=5) == HealthAssessment(
        healthy_at=None, held_seconds=0.0, first_unhealthy_at=None, unhealthy_seconds=0.0, flaps=0, gaps=0, samples=0
    )


def test_unhealthy_seconds_count_each_failing_sample_for_the_interval_to_the_next() -> None:
    run = samples((0, False), (3, False), (6, True), (9, True))
    assert assess_health(run, hold_seconds=3, max_gap_seconds=5).unhealthy_seconds == 6


def test_samples_are_ordered_by_time_before_assessment() -> None:
    run = samples((6, True), (0, False), (3, True))
    assert assess_health(run, hold_seconds=3, max_gap_seconds=5).healthy_at == 3


def test_hold_and_gap_must_be_positive() -> None:
    with pytest.raises(ValueError, match="hold_seconds"):
        assess_health([], hold_seconds=0, max_gap_seconds=5)
    with pytest.raises(ValueError, match="max_gap_seconds"):
        assess_health([], hold_seconds=5, max_gap_seconds=0)


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


def test_prober_samples_until_stopped_and_writes_every_sample(tmp_path: Path) -> None:
    clock = Clock()
    results = iter([ProbeResult(False, "503"), ProbeResult(True), ProbeResult(True)])
    stop = threading.Event()

    def probe() -> ProbeResult:
        try:
            return next(results)
        except StopIteration:
            stop.set()
            return ProbeResult(True)

    prober = Prober(probe, tmp_path / "probes.jsonl", interval_seconds=3, now=clock.time, sleep=clock.sleep)
    prober.run(stop)
    lines = [json.loads(line) for line in (tmp_path / "probes.jsonl").read_text().splitlines()]
    assert [(line["at"], line["healthy"]) for line in lines] == [
        (1000.0, False),
        (1003.0, True),
        (1006.0, True),
        (1009.0, True),
    ]
    assert lines[0]["detail"] == "503"
    assert prober.samples()[-1].healthy
    assert clock.slept == [3, 3, 3]


def test_a_probe_that_raises_is_an_unhealthy_sample_not_a_crash(tmp_path: Path) -> None:
    clock = Clock()
    stop = threading.Event()
    calls = 0

    def probe() -> ProbeResult:
        nonlocal calls
        calls += 1
        if calls == 2:
            stop.set()
        raise TimeoutError("no answer")

    prober = Prober(probe, tmp_path / "p.jsonl", interval_seconds=2, now=clock.time, sleep=clock.sleep)
    prober.run(stop)
    assert [s.healthy for s in prober.samples()] == [False, False]
    assert "TimeoutError: no answer" in prober.samples()[0].detail


def test_probe_time_does_not_stretch_the_cadence(tmp_path: Path) -> None:
    clock = Clock()
    stop = threading.Event()
    calls = 0

    def probe() -> ProbeResult:
        nonlocal calls
        calls += 1
        clock.now += 1.5  # the probe itself takes time
        if calls == 3:
            stop.set()
        return ProbeResult(True)

    Prober(probe, tmp_path / "p.jsonl", interval_seconds=3, now=clock.time, sleep=clock.sleep).run(stop)
    assert clock.slept == [1.5, 1.5]


def test_interval_is_bounded_to_the_agreed_two_to_five_seconds(tmp_path: Path) -> None:
    for bad in (1.0, 6.0):
        with pytest.raises(ValueError, match="interval_seconds"):
            Prober(lambda: ProbeResult(True), tmp_path / "p.jsonl", interval_seconds=bad)
