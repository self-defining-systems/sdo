from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop.loop import AgentOutcome, InjectionWindow, LoopConfig, run_incidents
from benchmarks.sregym.fastloop.records import OracleVerdict, TokenCounts, load_records

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

T0 = datetime(2026, 9, 27, 12, 0, 0, tzinfo=timezone.utc)


@dataclass
class FakeClock:
    now: float = 1000.0

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@dataclass
class FakeDriver:
    clock: FakeClock
    events: list[str] = field(default_factory=list)
    oracle_success: bool = True
    recover_fails: bool = False
    recovered: bool = False

    def inject(self, problem_id: str) -> InjectionWindow:
        self.recovered = False
        self.events.append(f"inject:{problem_id}")
        self.clock.advance(5)
        return InjectionWindow(started_at=T0, finished_at=T0 + timedelta(seconds=5))

    def oracle(self) -> OracleVerdict:
        self.events.append("oracle")
        return OracleVerdict(kind="sregym-mitigation-oracle", success=self.oracle_success or self.recovered)

    def recover(self) -> float:
        self.events.append("recover")
        if self.recover_fails:
            raise RuntimeError("cluster gone")
        self.clock.advance(10)
        self.recovered = True
        return 10.0


@dataclass
class FakeAgent:
    clock: FakeClock
    name: str = "sdo"
    model: str = "gpt-test"
    events: list[str] = field(default_factory=list)
    fail_on: set[int] = field(default_factory=set)
    resolution: str | None = None

    def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
        self.events.append(f"resolve:{index}")
        self.clock.advance(2)  # baseline gate before injection
        window = inject()
        if index in self.fail_on:
            raise RuntimeError("responder crashed")
        self.clock.advance(60)
        return AgentOutcome(
            injection=window,
            detected_at=T0 + timedelta(seconds=8),
            mitigation_applied_at=T0 + timedelta(seconds=40),
            resolved_at=T0 + timedelta(seconds=65),
            diagnosis="mongo-geo-script ConfigMap missing",
            responder_tokens=TokenCounts(input_tokens=100, output_tokens=10),
            warm_path=index > 0,
            baseline_gate_seconds=2.0,
            incident_id=f"incident-{index}",
        )

    def learn(self, outcome: AgentOutcome) -> AgentOutcome:
        self.events.append(f"learn:{outcome.incident_id}")
        self.clock.advance(30)
        return outcome.with_learning(
            reflection_seconds=30.0,
            reflection_tokens=TokenCounts(input_tokens=1000, output_tokens=20),
            reflection_attempts=1,
            sdo_resolution=self.resolution,
        )

    def close(self) -> None:
        self.events.append("close")


def _config(tmp_path: Path, count: int, problems: tuple[str, ...] = ("p1",)) -> LoopConfig:
    return LoopConfig(run_id="run-1", problems=problems, incidents=count, results_path=tmp_path / "incidents.jsonl")


def test_each_incident_is_graded_before_recovery_and_learning_follows_recovery(tmp_path: Path) -> None:
    clock = FakeClock()
    driver, agent = FakeDriver(clock), FakeAgent(clock)

    records = run_incidents(agent, driver, _config(tmp_path, 2, ("p1", "p2")), monotonic=clock.monotonic)

    assert driver.events == ["inject:p1", "oracle", "recover", "oracle", "inject:p2", "oracle", "recover", "oracle"]
    assert agent.events == ["resolve:0", "learn:incident-0", "resolve:1", "learn:incident-1", "close"]
    assert [record.problem_id for record in records] == ["p1", "p2"]
    first = records[0]
    assert first.oracle is not None
    assert first.oracle.success
    assert first.injection_to_detection_seconds == pytest.approx(8.0)
    assert first.reflection_seconds == pytest.approx(30.0)
    assert first.fault_recovery_seconds == pytest.approx(10.0)
    # gate 2 + inject 5 + response 60 + recovery 10 + learning 30
    assert first.incident_wall_seconds == pytest.approx(107.0)
    assert (records[0].warm_path, records[1].warm_path) == (False, True)
    assert [record.index for record in load_records(tmp_path / "incidents.jsonl")] == [0, 1]


def test_the_run_record_says_when_sdo_did_not_mitigate_the_incident(tmp_path: Path) -> None:
    clock = FakeClock()
    driver = FakeDriver(clock)
    agent = FakeAgent(clock, resolution="cleared_without_sdo_action")

    records = run_incidents(agent, driver, _config(tmp_path, 1), monotonic=clock.monotonic)

    assert records[0].sdo_resolution == "cleared_without_sdo_action"
    assert load_records(tmp_path / "incidents.jsonl")[0].sdo_resolution == "cleared_without_sdo_action"


def test_a_failed_incident_is_recorded_recovered_and_the_loop_continues(tmp_path: Path) -> None:
    clock = FakeClock()
    driver, agent = FakeDriver(clock), FakeAgent(clock, fail_on={0})

    records = run_incidents(agent, driver, _config(tmp_path, 2), monotonic=clock.monotonic)

    assert records[0].error == "RuntimeError: responder crashed"
    assert records[0].oracle is not None  # the cluster state is still graded
    assert records[1].error is None
    assert driver.events.count("recover") == 2


def test_the_loop_stops_when_the_fault_cannot_be_recovered(tmp_path: Path) -> None:
    clock = FakeClock()
    driver, agent = FakeDriver(clock, recover_fails=True), FakeAgent(clock)

    records = run_incidents(agent, driver, _config(tmp_path, 3), monotonic=clock.monotonic)

    assert len(records) == 1
    assert records[0].error == "fault recovery failed: RuntimeError: cluster gone"
    assert agent.events[-1] == "close"


def test_config_rejects_empty_problem_lists_and_non_positive_counts(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="problem"):
        LoopConfig(run_id="r", problems=(), incidents=1, results_path=tmp_path / "x.jsonl")
    with pytest.raises(ValueError, match="incidents"):
        LoopConfig(run_id="r", problems=("p",), incidents=0, results_path=tmp_path / "x.jsonl")


def test_next_injection_waits_for_learning_and_failed_learning_stops_stream(tmp_path: Path) -> None:
    clock = FakeClock()
    events: list[str] = []

    class GatedDriver(FakeDriver):
        def inject(self, problem_id: str) -> InjectionWindow:
            events.append(f"inject:{problem_id}")
            return super().inject(problem_id)

    class GatedAgent(FakeAgent):
        def learn(self, outcome: AgentOutcome) -> AgentOutcome:
            events.append("reflection-and-rebuild")
            if outcome.incident_id == "incident-0":
                raise RuntimeError("rollout was not verified")
            return super().learn(outcome)

    records = run_incidents(
        GatedAgent(clock), GatedDriver(clock), _config(tmp_path, 2, ("p1", "p2")), monotonic=clock.monotonic
    )

    assert events == ["inject:p1", "reflection-and-rebuild"]
    assert len(records) == 1
    assert "rollout was not verified" in str(records[0].error)


def test_oracle_failure_is_recorded_and_recovery_finishes_before_next_injection(tmp_path: Path) -> None:
    clock = FakeClock()
    records = run_incidents(
        FakeAgent(clock), FakeDriver(clock, oracle_success=False), _config(tmp_path, 2), monotonic=clock.monotonic
    )

    assert len(records) == 2
    assert all(record.error == "oracle reported failure" for record in records)
    assert all(record.oracle_finished_at is not None and record.recovery_finished_at is not None for record in records)


def test_recovery_residue_fails_the_record_and_stops_before_next_injection(tmp_path: Path) -> None:
    clock = FakeClock()

    class ResidualDriver(FakeDriver):
        def oracle(self) -> OracleVerdict:
            self.events.append("oracle")
            return OracleVerdict(kind="sregym-mitigation-oracle", success=not self.recovered)

    driver = ResidualDriver(clock)
    records = run_incidents(FakeAgent(clock), driver, _config(tmp_path, 2), monotonic=clock.monotonic)

    assert len(records) == 1
    assert "recovery residue" in str(records[0].error)


def test_failed_injection_stops_before_another_fault_can_be_applied(tmp_path: Path) -> None:
    clock = FakeClock()

    class FailedDriver(FakeDriver):
        def inject(self, problem_id: str) -> InjectionWindow:
            raise RuntimeError("injector did not return a window")

    records = run_incidents(FakeAgent(clock), FailedDriver(clock), _config(tmp_path, 2), monotonic=clock.monotonic)

    assert len(records) == 1
    assert "injector did not return a window" in str(records[0].error)


def test_composed_stream_injects_together_and_grades_each_fault(tmp_path: Path) -> None:
    clock = FakeClock()

    class CompositeDriver(FakeDriver):
        def inject_composite(self, ids: tuple[str, ...]) -> object:
            self.events.append("compose:" + "+".join(ids))
            return type("Injection", (), {"window": InjectionWindow(T0, T0 + timedelta(seconds=5))})()

        def oracle(self, *, fault: int | None = None) -> OracleVerdict:
            self.events.append(f"oracle:{fault}")
            return OracleVerdict(kind="sregym-mitigation-oracle", success=fault == 0)

    driver = CompositeDriver(clock)
    config = LoopConfig(
        run_id="run-1",
        problems=("a",),
        incidents=1,
        results_path=tmp_path / "incidents.jsonl",
        stream=(("a", "b"),),
    )

    records = run_incidents(FakeAgent(clock), driver, config, monotonic=clock.monotonic)

    assert driver.events[:3] == ["compose:a+b", "oracle:0", "oracle:1"]
    assert records[0].problem_id == "a+b"
    assert records[0].oracle is not None
    assert records[0].oracle.success is False
    assert "oracle reported failure" in str(records[0].error)
