from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop.baseline_recovery import RecoveryCleanup
from benchmarks.sregym.fastloop.loop import AgentOutcome, InjectionWindow, LoopConfig, run_incidents
from benchmarks.sregym.fastloop.records import DiagnosisGrade, OracleVerdict, TokenCounts, load_records

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
    metadata = json.loads((tmp_path / "run_metadata.json").read_text())
    assert metadata["token_accounting_version"] == "responder-drift/v2"
    assert metadata["drift_reasoning_charged_to"] == "responder_tokens"
    assert all(record.drift_tokens.total_tokens == 0 for record in records)


def test_run_prelude_phases_are_recorded_once_before_the_first_incident(tmp_path: Path) -> None:
    clock = FakeClock()
    config = LoopConfig(
        run_id="run-1",
        problems=("p1",),
        incidents=2,
        results_path=tmp_path / "incidents.jsonl",
        run_prelude_phase_seconds={"fault_description": 4.0, "worker_deploy": 12.0},
    )

    records = run_incidents(FakeAgent(clock), FakeDriver(clock), config, monotonic=clock.monotonic)

    assert records[0].run_prelude_phase_seconds == {"fault_description": 4.0, "worker_deploy": 12.0}
    assert records[1].run_prelude_phase_seconds == {}
    assert load_records(config.results_path)[0].run_prelude_phase_seconds == records[0].run_prelude_phase_seconds


def test_incident_row_records_recovery_cleanup(tmp_path: Path) -> None:
    clock = FakeClock()

    class Driver(FakeDriver):
        last_cleanup = RecoveryCleanup(deleted_counts={"Job": 1}, restored=["Deployment/mongodb-geo"])

    record = run_incidents(FakeAgent(clock), Driver(clock), _config(tmp_path, 1), monotonic=clock.monotonic)[0]
    saved = load_records(tmp_path / "incidents.jsonl")[0]

    assert record.cleanup_deleted_counts == saved.cleanup_deleted_counts == {"Job": 1}
    assert record.cleanup_restored == saved.cleanup_restored == ["Deployment/mongodb-geo"]


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


def test_interrupt_after_injection_recovers_and_checks_the_oracle_before_exiting(tmp_path: Path) -> None:
    clock = FakeClock()
    driver = FakeDriver(clock)

    class InterruptedAgent(FakeAgent):
        def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
            inject()
            raise KeyboardInterrupt

    agent = InterruptedAgent(clock)
    with pytest.raises(KeyboardInterrupt):
        run_incidents(agent, driver, _config(tmp_path, 2), monotonic=clock.monotonic)

    assert driver.events == ["inject:p1", "recover", "oracle"]
    assert agent.events == ["close"]


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


def test_continue_on_error_records_failed_learning_and_runs_next_incident(tmp_path: Path) -> None:
    clock = FakeClock()

    class Agent(FakeAgent):
        def learn(self, outcome: AgentOutcome) -> AgentOutcome:
            if outcome.incident_id == "incident-0":
                raise RuntimeError("reflection failed")
            return super().learn(outcome)

    driver = FakeDriver(clock)
    reset_calls: list[str] = []
    records = run_incidents(
        Agent(clock),
        driver,
        LoopConfig(
            run_id="run-1",
            problems=("p1", "p2"),
            incidents=2,
            results_path=tmp_path / "incidents.jsonl",
            continue_on_error=True,
        ),
        monotonic=clock.monotonic,
        on_failed_incident=lambda: reset_calls.append("reset"),
    )

    assert len(records) == 2
    assert "reflection failed" in str(records[0].error)
    assert records[1].error is None
    assert driver.events == ["inject:p1", "oracle", "recover", "oracle", "inject:p2", "oracle", "recover", "oracle"]
    assert reset_calls == ["reset"]


def test_continue_on_error_still_stops_when_baseline_recovery_fails(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    clock = FakeClock()
    driver = FakeDriver(clock, recover_fails=True)
    config = LoopConfig(
        run_id="run-1", problems=("p1",), incidents=2, results_path=tmp_path / "incidents.jsonl", continue_on_error=True
    )

    with caplog.at_level("INFO"):
        assert len(run_incidents(FakeAgent(clock), driver, config, monotonic=clock.monotonic)) == 1
    assert "run_end" in caplog.text
    assert "completed=1 requested=2" in caplog.text
    assert "reason=incident 0 did not clear the recovery and learning gate" in caplog.text


def test_recovery_failure_keeps_grade_and_still_learns_from_the_same_sdo_outcome(tmp_path: Path) -> None:
    clock = FakeClock()
    resolved: list[AgentOutcome] = []
    learned: list[AgentOutcome] = []

    class Agent(FakeAgent):
        def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
            outcome = super().resolve(index, problem_id, inject)
            resolved.append(outcome)
            return outcome

        def learn(self, outcome: AgentOutcome) -> AgentOutcome:
            learned.append(outcome)
            return super().learn(outcome)

    driver = FakeDriver(clock, oracle_success=False, recover_fails=True)

    records = run_incidents(Agent(clock), driver, _config(tmp_path, 2), monotonic=clock.monotonic)

    assert len(learned) == 1
    assert learned[0] is resolved[0]
    assert learned[0].responder_tokens == TokenCounts(input_tokens=100, output_tokens=10)
    assert len(records) == 1  # Learning does not authorize injection into a broken baseline.
    assert records[0].oracle is not None
    assert records[0].oracle.success is False
    assert records[0].recovery_oracle is None
    assert "fault recovery failed: RuntimeError: cluster gone" in str(records[0].error)
    assert records[0].reflection_tokens == TokenCounts(input_tokens=1000, output_tokens=20)


def test_cleanup_exception_is_logged_with_a_run_end_reason(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    class Agent(FakeAgent):
        def close(self) -> None:
            raise RuntimeError("cleanup unavailable")

    clock = FakeClock()
    with caplog.at_level("INFO"), pytest.raises(RuntimeError, match="cleanup unavailable"):
        run_incidents(Agent(clock), FakeDriver(clock), _config(tmp_path, 1), monotonic=clock.monotonic)

    assert "run_end" in caplog.text
    assert "reason=cleanup failed: RuntimeError: cleanup unavailable" in caplog.text


def test_oracle_failure_is_recorded_and_recovery_finishes_before_next_injection(tmp_path: Path) -> None:
    clock = FakeClock()
    records = run_incidents(
        FakeAgent(clock), FakeDriver(clock, oracle_success=False), _config(tmp_path, 2), monotonic=clock.monotonic
    )

    assert len(records) == 2
    assert all(record.error == "oracle reported failure" for record in records)
    assert all(record.oracle_finished_at is not None and record.recovery_finished_at is not None for record in records)


def test_deferred_diagnosis_grading_does_not_gate_next_injection_and_merges_results(tmp_path: Path) -> None:
    import threading

    grading_started = threading.Event()
    next_injected = threading.Event()

    class Driver(FakeDriver):
        def inject(self, problem_id: str) -> InjectionWindow:
            if problem_id == "p2":
                next_injected.set()
            return super().inject(problem_id)

    class Judge:
        def grade(self, problem_ids: tuple[str, ...], diagnosis: str) -> DiagnosisGrade:
            if problem_ids == ("p1",):
                grading_started.set()
                assert next_injected.wait(2)
            return DiagnosisGrade(
                success=True, accuracy=100.0, judge_tokens=TokenCounts(input_tokens=3), latency_seconds=0.1
            )

    clock = FakeClock()
    records = run_incidents(
        FakeAgent(clock),
        Driver(clock),
        _config(tmp_path, 2, ("p1", "p2")),
        monotonic=clock.monotonic,
        grader=Judge(),
        diagnosis_grading="deferred",
    )

    assert grading_started.is_set()
    assert all(record.diagnosis_grade and record.diagnosis_grade.success for record in records)
    assert [
        record.diagnosis_grade.judge_tokens.input_tokens for record in load_records(tmp_path / "incidents.jsonl")
    ] == [3, 3]
    with (tmp_path / "incidents.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert [json.loads(row["diagnosis_grade"])["judge_tokens"]["input_tokens"] for row in rows] == [3, 3]


def test_inline_diagnosis_grading_finishes_before_recovery(tmp_path: Path) -> None:
    clock = FakeClock()
    driver = FakeDriver(clock)

    class Judge:
        def grade(self, problem_ids: tuple[str, ...], diagnosis: str) -> DiagnosisGrade:
            driver.events.append("diagnosis-grade")
            return DiagnosisGrade(success=False, accuracy=0.0, latency_seconds=0.2)

    records = run_incidents(
        FakeAgent(clock),
        driver,
        _config(tmp_path, 1),
        monotonic=clock.monotonic,
        grader=Judge(),
        diagnosis_grading="inline",
    )
    assert driver.events == ["inject:p1", "diagnosis-grade", "oracle", "recover", "oracle"]
    assert records[0].diagnosis_grade is not None
    assert records[0].diagnosis_grade.success is False


def test_missing_diagnosis_skips_deferred_grading(tmp_path: Path) -> None:
    clock = FakeClock()

    class EmptyAgent(FakeAgent):
        def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
            return AgentOutcome(injection=inject(), error="closure failed")

    class Judge:
        def grade(self, problem_ids: tuple[str, ...], diagnosis: str) -> DiagnosisGrade:
            pytest.fail("empty diagnosis must not be graded")

    records = run_incidents(
        EmptyAgent(clock),
        FakeDriver(clock),
        _config(tmp_path, 1),
        monotonic=clock.monotonic,
        grader=Judge(),
        diagnosis_grading="deferred",
    )
    assert records[0].diagnosis_grade is not None
    assert records[0].diagnosis_grade.skipped is True
    assert records[0].diagnosis_grade.skip_reason == "no diagnosis text"


def test_terminal_closure_error_records_evidence_and_stops_stream(tmp_path: Path) -> None:
    clock = FakeClock()

    class FailedAgent(FakeAgent):
        def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
            return AgentOutcome(
                injection=inject(),
                detected_at=T0 + timedelta(seconds=6),
                responder_started_at=T0 + timedelta(seconds=7),
                responder_finished_at=T0 + timedelta(seconds=9),
                mitigation_applied_at=T0 + timedelta(seconds=8),
                resolved_at=T0 + timedelta(seconds=10),
                diagnosis="bad image",
                responder_tokens=TokenCounts(input_tokens=500, output_tokens=50),
                program_diagnoses=2,
                program_verdicts={"RESULT_MATCH": 1, "RESULT_NO_MATCH": 1},
                program_tool_calls=1,
                tool_commands={"verify": 1},
                response_loops_used=1,
                error="closure failed permanently",
                terminal_error=True,
            )

    agent = FailedAgent(clock)
    records = run_incidents(agent, FakeDriver(clock), _config(tmp_path, 2), monotonic=clock.monotonic)
    assert len(records) == 1
    record = load_records(tmp_path / "incidents.jsonl")[0]
    assert record.detected_at == T0 + timedelta(seconds=6)
    assert record.responder_tokens.input_tokens == 500
    assert record.program_diagnoses == 2
    assert record.tool_commands == {"verify": 1}
    assert record.response_loops_used == 1
    assert "closure failed permanently" in (record.error or "")
    assert agent.events == ["close"]


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


def test_delivery_fallback_survives_failed_and_successful_incident_rows(tmp_path):
    from dataclasses import replace

    clock = FakeClock()
    config = replace(
        _config(tmp_path, 2),
        continue_on_error=True,
        image_delivery="kind-load-fallback",
        image_delivery_reason="registry offline",
    )
    records = run_incidents(FakeAgent(clock, fail_on={0}), FakeDriver(clock), config, monotonic=clock.monotonic)
    assert len(records) == 2
    for record in load_records(config.results_path):
        assert record.image_delivery == "kind-load-fallback"
        assert record.image_delivery_reason == "registry offline"


@pytest.mark.parametrize("failed", [False, True])
def test_incident_row_rereads_delivery_after_source_deployment(tmp_path, failed):
    from dataclasses import replace

    from benchmarks.sregym.fastloop.image_delivery import save_delivery

    clock = FakeClock()
    delivery_path = tmp_path / "image_delivery.jsonl"
    save_delivery(delivery_path, {"path": "registry", "reason": "", "seconds": 1, "images": ["app:v1"]})
    config = replace(_config(tmp_path, 1), image_delivery="registry", delivery_record_path=delivery_path)

    class DeployingAgent(FakeAgent):
        def resolve(self, index, problem_id, inject):
            save_delivery(
                delivery_path,
                {
                    "path": "kind-load-fallback",
                    "reason": "mid-run registry failure",
                    "seconds": 2,
                    "images": ["app:v2"],
                },
            )
            return super().resolve(index, problem_id, inject)

    records = run_incidents(
        DeployingAgent(clock, fail_on={0} if failed else set()),
        FakeDriver(clock),
        config,
        monotonic=clock.monotonic,
    )
    persisted = load_records(config.results_path)
    assert records[0].image_delivery == persisted[0].image_delivery == "kind-load-fallback"
    assert records[0].image_delivery_reason == "mid-run registry failure"
