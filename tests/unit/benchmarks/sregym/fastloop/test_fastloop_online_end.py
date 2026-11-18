"""The row's online end: graded, outcome committed, published before the closing turn starts."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter.persistent import Clock, wait_for_outcome_commit
from benchmarks.sregym.fastloop.loop import AgentOutcome, LoopConfig, run_incidents
from benchmarks.sregym.fastloop.records import IncidentRecord, online_checkpoint_path
from tests.unit.benchmarks.sregym.fastloop.test_fastloop_loop import FakeAgent, FakeClock, FakeDriver

if TYPE_CHECKING:
    from pathlib import Path

T0 = datetime(2026, 9, 27, 12, 0, 0, tzinfo=timezone.utc)


def _config(tmp_path: Path, **overrides: object) -> LoopConfig:
    return LoopConfig(
        run_id="run-1", problems=("p1",), incidents=1, results_path=tmp_path / "incidents.jsonl", **overrides
    )  # type: ignore[arg-type]


class CommittingAgent(FakeAgent):
    """An agent whose outcome is committed at ``T0 + 70 s`` (the hook the SDO agent implements)."""

    def await_outcome_committed(self, outcome: AgentOutcome) -> datetime | None:
        self.events.append("await_committed")
        self.clock.advance(5)
        return T0 + timedelta(seconds=70)


def test_the_online_checkpoint_is_published_after_grading_and_before_learning(tmp_path: Path) -> None:
    clock = FakeClock()
    path = online_checkpoint_path(tmp_path / "incidents.jsonl", 0)
    seen: dict[str, object] = {}

    class Agent(CommittingAgent):
        def learn(self, outcome: AgentOutcome) -> AgentOutcome:
            seen["online_exists_at_learn"] = path.is_file()
            seen["events_at_learn"] = list(self.events)
            return super().learn(outcome)

    agent = Agent(clock)
    run_incidents(agent, FakeDriver(clock), _config(tmp_path), monotonic=clock.monotonic)
    assert seen["online_exists_at_learn"] is True
    assert seen["events_at_learn"] == ["resolve:0", "await_committed"]
    online = IncidentRecord.model_validate_json(path.read_text())
    assert online.oracle is not None
    assert online.oracle.success is True
    assert online.incident_id == "incident-0"
    assert online.responder_tokens.input_tokens == 100  # the outcome's online fields are present
    assert online.reflection_tokens.input_tokens == 0  # the closing turn has not happened
    assert online.outcome_committed_at == T0 + timedelta(seconds=70)
    assert online.online_ended_at is not None


def test_the_final_record_carries_the_commit_time_and_the_online_end(tmp_path: Path) -> None:
    clock = FakeClock()
    records = run_incidents(CommittingAgent(clock), FakeDriver(clock), _config(tmp_path), monotonic=clock.monotonic)
    online = IncidentRecord.model_validate_json(online_checkpoint_path(tmp_path / "incidents.jsonl", 0).read_text())
    assert records[0].outcome_committed_at == T0 + timedelta(seconds=70)
    assert records[0].online_ended_at == online.online_ended_at


def test_an_agent_without_a_commit_hook_still_publishes_the_online_end(tmp_path: Path) -> None:
    clock = FakeClock()
    run_incidents(FakeAgent(clock), FakeDriver(clock), _config(tmp_path), monotonic=clock.monotonic)
    online = IncidentRecord.model_validate_json(online_checkpoint_path(tmp_path / "incidents.jsonl", 0).read_text())
    assert online.outcome_committed_at is None
    assert online.oracle is not None


def test_a_failing_commit_wait_never_fails_the_row(tmp_path: Path) -> None:
    clock = FakeClock()

    class Agent(FakeAgent):
        def await_outcome_committed(self, outcome: AgentOutcome) -> datetime | None:
            raise RuntimeError("controller state unreadable")

    records = run_incidents(Agent(clock), FakeDriver(clock), _config(tmp_path), monotonic=clock.monotonic)
    assert records[0].oracle is not None
    assert records[0].outcome_committed_at is None
    assert online_checkpoint_path(tmp_path / "incidents.jsonl", 0).is_file()


def test_an_uninjected_row_publishes_no_online_checkpoint(tmp_path: Path) -> None:
    clock = FakeClock()
    run_incidents(
        FakeAgent(clock, fail_on={0}),
        FakeDriver(clock),
        _config(tmp_path, continue_on_error=True),
        monotonic=clock.monotonic,
    )
    # The injection happened before the crash, but there is no outcome and no grade to publish.
    path = online_checkpoint_path(tmp_path / "incidents.jsonl", 0)
    if path.is_file():
        assert json.loads(path.read_text())["incident_id"] is None


@dataclass
class StateOps:
    """Controller states returned one per read; the last repeats."""

    states: list[dict[str, object]]
    reads: int = 0
    clock: FakeClock = field(default_factory=FakeClock)

    def runtime_state(self, control: str) -> dict[str, object]:
        state = self.states[min(self.reads, len(self.states) - 1)]
        self.reads += 1
        return state


def _clock(fake: FakeClock) -> Clock:
    base = datetime(2026, 9, 27, 12, 0, 0, tzinfo=timezone.utc)
    return Clock(monotonic=fake.monotonic, sleep=fake.advance, now=lambda: base + timedelta(seconds=fake.now - 1000.0))


def test_the_commit_wait_returns_when_the_incident_is_acknowledged() -> None:
    fake = FakeClock()
    ops = StateOps([{}, {"last_acknowledged_incident_id": "other"}, {"last_acknowledged_incident_id": "i-1"}])
    clock = _clock(fake)
    committed = wait_for_outcome_commit(ops, "ctl", "i-1", clock.now() + timedelta(seconds=600), clock)  # type: ignore[arg-type]
    assert committed is not None
    assert ops.reads == 3


def test_the_commit_wait_gives_up_quietly_at_the_deadline() -> None:
    fake = FakeClock()
    ops = StateOps([{"last_acknowledged_incident_id": "other"}])
    clock = _clock(fake)
    assert wait_for_outcome_commit(ops, "ctl", "i-1", clock.now() + timedelta(seconds=20), clock) is None  # type: ignore[arg-type]


def test_the_commit_wait_survives_an_unreadable_state_read() -> None:
    fake = FakeClock()

    class Flaky(StateOps):
        def runtime_state(self, control: str) -> dict[str, object]:
            if self.reads == 0:
                self.reads += 1
                raise RuntimeError("apiserver blip")
            return super().runtime_state(control)

    ops = Flaky([{"last_acknowledged_incident_id": "i-1"}])
    clock = _clock(fake)
    assert wait_for_outcome_commit(ops, "ctl", "i-1", clock.now() + timedelta(seconds=60), clock) is not None  # type: ignore[arg-type]
