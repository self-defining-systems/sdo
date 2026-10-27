"""Grading is an eval-only closure effect, independent of reflection."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.adapter.persistent import Clock
    from tests.unit.benchmarks.sregym.adapter.test_persistent import FakeOps

import pytest

from benchmarks.sregym.adapter.grading import ClosureGrader


def ledger() -> dict:
    return {
        "incident_id": "incident-1",
        "closure": {
            "request": {"incident_id": "incident-1"},
            "dispatched_at": "2026-10-26T00:00:00Z",
            "responder_completed_at": "2026-10-26T00:00:30Z",
            "verified_at": "2026-10-26T00:00:35Z",
            "result": {
                "confirmed_root_causes": [{"summary": "selector changed"}],
                "repair_actions": [{"summary": "restored selector"}],
            },
        },
        "reflection": {"learning_decision": "running"},
    }


def test_grade_precedes_terminal_receipt_and_survives_failed_reflection(tmp_path: Path) -> None:
    calls = []

    def submit(result: dict, api: str) -> dict:
        assert not (tmp_path / "sdo_production_receipt_strict.json").exists()
        assert result["native_ledger"]["reflection"]["learning_decision"] == "running"
        calls.append(api)
        return {"mitigation": {"success": True}}

    grader = ClosureGrader(
        tmp_path, "http://conductor", submit=submit, now=lambda: datetime(2026, 10, 26, 0, 0, 40, tzinfo=timezone.utc)
    )
    evidence = ledger()
    grader.observe(evidence)
    evidence["reflection"] = {"learning_decision": "failed"}
    grader.observe(evidence)
    # A restarted adapter also retains the frozen grade, never resubmitting.
    ClosureGrader(tmp_path, "http://conductor", submit=submit).observe(evidence)
    grade = json.loads((tmp_path / "sdo_incident_grade.json").read_text())
    assert calls == ["http://conductor"]
    assert grade["grade"] == {"mitigation": {"success": True}}
    assert grade["closure_to_grade_seconds"] == 5
    assert grade["responder_runtime_seconds"] == 30
    assert grade["graded_at"] == "2026-10-26T00:00:40+00:00"


@pytest.mark.parametrize("result", [None, "invalid"])
def test_closure_without_result_submits_nothing(tmp_path: Path, result: object) -> None:
    evidence = ledger()
    evidence["closure"]["result"] = result
    grader = ClosureGrader(tmp_path, "http://conductor", submit=lambda *_: pytest.fail("no result"))
    grader.observe(evidence)
    assert not (tmp_path / "sdo_incident_grade.json").exists()


def test_submission_failure_is_recorded_without_retrying_ambiguous_post(tmp_path: Path) -> None:
    calls = []

    def submit(*_args: object) -> dict:
        calls.append(1)
        raise RuntimeError("conductor disconnected after POST")

    grader = ClosureGrader(tmp_path, "http://conductor", submit=submit)
    grader.observe(ledger())
    grader.observe(ledger())
    assert calls == [1]
    grade = json.loads((tmp_path / "sdo_incident_grade.json").read_text())
    assert grade["status"] == "failed"
    assert "disconnected" in grade["error"]


def test_closure_monitor_grades_while_lifecycle_waits_for_reflection(tmp_path: Path) -> None:
    from threading import Event

    from benchmarks.sregym.adapter.grading import ClosureGradingMonitor

    submitted = Event()
    terminal = Event()

    def submit(_result: dict, _api: str) -> dict:
        assert not terminal.is_set()
        submitted.set()
        return {"done": {"status": "done"}}

    grader = ClosureGrader(tmp_path, "http://conductor", submit=submit)
    with ClosureGradingMonitor(grader, read=lambda: ledger(), poll_seconds=0.01):
        assert submitted.wait(2), "submission must not wait for reflection/receipt"
        terminal.set()
    assert json.loads(grader.path.read_text())["status"] == "graded"


def test_monitor_final_sample_catches_fast_closure(tmp_path: Path) -> None:
    from threading import Event

    from benchmarks.sregym.adapter.grading import ClosureGradingMonitor

    sampled = Event()
    closed = Event()
    calls = []

    def read() -> dict | None:
        sampled.set()
        return ledger() if closed.is_set() else None

    grader = ClosureGrader(tmp_path, "http://conductor", submit=lambda *_: calls.append(1) or {})
    with ClosureGradingMonitor(grader, read=read, poll_seconds=30):
        assert sampled.wait(2)
        closed.set()
    assert calls == [1]


@pytest.mark.parametrize("reflection_failed", [False, True])
def test_native_monitor_uses_correlated_ledger_and_skips_previous_incident(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reflection_failed: bool
) -> None:
    import subprocess
    from threading import Event

    from benchmarks.sregym.adapter import driver
    from benchmarks.sregym.adapter.native_operator import NativeInstallConfig

    closed = Event()
    submitted = Event()
    snapshots = iter(
        [
            {"last_acknowledged_incident_id": "previous"},
            {
                **(
                    {"failed_closures": [{"closure": {"request": {"incident_id": "incident-1"}}}]}
                    if reflection_failed
                    else {"pending_closure": {"request": {"incident_id": "incident-1"}}}
                ),
                "last_acknowledged_incident_id": "previous",
            },
        ]
    )
    latest = {}

    def kubectl(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess:
        nonlocal latest
        latest = next(snapshots, latest)
        return subprocess.CompletedProcess([], 0, json.dumps({"data": {"runtime-state.json": json.dumps(latest)}}))

    def evidence(_config: object, incident_id: str) -> dict:
        assert incident_id == "incident-1"
        assert not closed.is_set(), "reflection has not finished"
        return {"ledger": ledger()}

    def submit(_receipt: dict, _base: str) -> dict:
        assert not closed.is_set()
        submitted.set()
        return {"mitigation": {"success": True}}

    monkeypatch.setattr(driver, "kubectl", kubectl)
    monkeypatch.setattr(driver, "read_incident_evidence", evidence)
    monkeypatch.setattr(driver, "_submit_recorded_result", submit)
    native = NativeInstallConfig(
        repository=tmp_path,
        namespace="hotel",
        control_namespace="hotel-sdo",
        application="hotel",
        controller_image="controller:test",
        responder_image="responder:test",
        validator_image="validator:test",
        repository_pvc="repo",
        credentials_secret="credentials",
        model="test",
        kubeconfig=tmp_path / "config",
    )
    with driver._closure_grading_monitor(native, "http://conductor", tmp_path):
        assert submitted.wait(2)
        closed.set()
    assert json.loads((tmp_path / "sdo_incident_grade.json").read_text())["incident_id"] == "incident-1"


def test_uncorrelated_closure_cannot_reach_benchmark_transport(tmp_path: Path) -> None:
    evidence = ledger()
    evidence["closure"]["request"]["incident_id"] = "other-incident"
    grader = ClosureGrader(tmp_path, "http://conductor", submit=lambda *_: pytest.fail("uncorrelated result"))
    with pytest.raises(ValueError, match="incident-correlated"):
        grader.observe(evidence)
    assert not grader.path.exists()


def test_recovery_pause_rejects_unclosed_incident() -> None:
    from benchmarks.sregym.adapter.persistent import PersistentControllerError, pause_for_recovery
    from tests.unit.benchmarks.sregym.adapter.test_persistent import FakeOps

    ops = FakeOps(states={"hotel-sdo": {"busy_until": _busy_until(600)}})
    with pytest.raises(PersistentControllerError, match="recovery.*closed"):
        pause_for_recovery(ops, "hotel-sdo", incident_id="incident-1")
    assert not ops.events


def test_recovery_pause_requires_ack_then_rechecks_for_racing_incident() -> None:
    from benchmarks.sregym.adapter.persistent import PersistentControllerError, pause_for_recovery
    from tests.unit.benchmarks.sregym.adapter.test_persistent import FakeOps

    class RacingOps(FakeOps):
        def controller_logs(self, control: str) -> str:
            self.states[control]["busy_until"] = _busy_until(600)
            return super().controller_logs(control)

    ops = RacingOps(states={"hotel-sdo": {"last_acknowledged_incident_id": "incident-1"}}, logs={"hotel-sdo": []})
    with pytest.raises(PersistentControllerError, match="recovery.*closed"):
        pause_for_recovery(ops, "hotel-sdo", incident_id="incident-1")
    assert ops.events == [("paused", "hotel-sdo")]


def _followup_ops(open_polls: int | None) -> FakeOps:
    """Controller whose incident-2 opened after graded incident-1 closed; it closes after ``open_polls`` reads."""
    from tests.unit.benchmarks.sregym.adapter.test_persistent import FakeOps

    reads = [0]

    class FollowupOps(FakeOps):
        def runtime_state(self, control_namespace: str) -> dict[str, Any]:
            reads[0] += 1
            if open_polls is None or reads[0] <= open_polls:
                return {
                    "last_acknowledged_incident_id": "incident-1",
                    "incident_request": {"incident_id": "incident-2"},
                    "busy_until": _busy_until(FOLLOWUP_BUSY_SECONDS),
                }
            return {"last_acknowledged_incident_id": "incident-2"}

    return FollowupOps(logs={"hotel-sdo": []})


def _fake_clock() -> tuple[list[float], Clock]:
    from benchmarks.sregym.adapter.persistent import Clock

    elapsed = [0.0]
    return elapsed, Clock(
        monotonic=lambda: elapsed[0],
        sleep=lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds),
        now=lambda: CLOCK_ORIGIN + timedelta(seconds=elapsed[0]),
    )


#: The fake clock starts here, so ``busy_until`` values are seconds after the start of the wait.
CLOCK_ORIGIN = datetime(2026, 10, 26, 12, 0, tzinfo=timezone.utc)
#: The controller's own bound on the follow-up incident; the wait adds one closeout margin.
FOLLOWUP_BUSY_SECONDS = 480.0
CLOSEOUT_MARGIN = 120.0


def _busy_until(seconds: float) -> str:
    return (CLOCK_ORIGIN + timedelta(seconds=seconds)).isoformat()


def test_recovery_waits_for_an_incident_opened_after_the_graded_closure() -> None:
    from benchmarks.sregym.adapter.persistent import FollowupClosureWait, pause_for_recovery

    elapsed, clock = _fake_clock()
    ops = _followup_ops(open_polls=3)

    wait = pause_for_recovery(ops, "hotel-sdo", incident_id="incident-1", clock=clock)

    assert wait == FollowupClosureWait(
        graded_incident_id="incident-1",
        followup_incident_ids=("incident-2",),
        waited_seconds=3.0,
        bound_seconds=FOLLOWUP_BUSY_SECONDS + CLOSEOUT_MARGIN,
        closed=True,
    )
    assert ops.events == [("paused", "hotel-sdo")]


def test_recovery_stops_when_the_followup_incident_outlives_the_bound() -> None:
    from benchmarks.sregym.adapter.persistent import FollowupClosureTimeout, pause_for_recovery

    elapsed, clock = _fake_clock()
    ops = _followup_ops(open_polls=None)

    with pytest.raises(FollowupClosureTimeout, match="incident-2.*incident-1") as raised:
        pause_for_recovery(ops, "hotel-sdo", incident_id="incident-1", clock=clock)

    assert raised.value.wait.followup_incident_ids == ("incident-2",)
    assert raised.value.wait.closed is False
    assert raised.value.wait.waited_seconds == elapsed[0] == FOLLOWUP_BUSY_SECONDS + CLOSEOUT_MARGIN
    assert not ops.events


def _queued_reflection_ops(queued: list[str], open_polls: int | None, busy_seconds: float) -> FakeOps:
    """Controller whose graded incident-1 closed (failed attempt) and whose reflections are queued behind its ACK."""
    from tests.unit.benchmarks.sregym.adapter.test_persistent import FakeOps

    reads = [0]

    class QueuedOps(FakeOps):
        def runtime_state(self, control_namespace: str) -> dict[str, Any]:
            reads[0] += 1
            if open_polls is None or reads[0] <= open_polls:
                return {
                    "last_acknowledged_incident_id": "incident-1",
                    "queued_reflection_incident_ids": queued,
                    "busy_until": _busy_until(busy_seconds),
                }
            return {"last_acknowledged_incident_id": "incident-1"}

    return QueuedOps(logs={"hotel-sdo": []})


def test_recovery_waits_for_the_graded_incidents_queued_reflection_up_to_its_own_deadline() -> None:
    """SDO may run a reflection for its whole response timeout; the controller's busy_until says so."""
    from benchmarks.sregym.adapter.persistent import pause_for_recovery

    elapsed, clock = _fake_clock()
    ops = _queued_reflection_ops(["incident-1"], open_polls=1800, busy_seconds=2102.0)

    wait = pause_for_recovery(ops, "hotel-sdo", incident_id="incident-1", clock=clock)

    assert wait is not None
    assert wait.closed is True
    assert wait.waited_seconds == 1800.0 > FOLLOWUP_BUSY_SECONDS + CLOSEOUT_MARGIN
    assert wait.followup_incident_ids == ("incident-1",)
    assert ops.events == [("paused", "hotel-sdo")]


def test_recovery_bounds_the_queued_reflection_wait_by_the_queue_length() -> None:
    from benchmarks.sregym.adapter.persistent import FollowupClosureTimeout, pause_for_recovery

    elapsed, clock = _fake_clock()
    ops = _queued_reflection_ops(["incident-0", "incident-1"], open_polls=None, busy_seconds=2 * 2102.0)

    with pytest.raises(FollowupClosureTimeout) as raised:
        pause_for_recovery(ops, "hotel-sdo", incident_id="incident-1", clock=clock)

    assert raised.value.wait.bound_seconds == 2 * 2102.0 + CLOSEOUT_MARGIN
    assert raised.value.wait.waited_seconds == elapsed[0] == raised.value.wait.bound_seconds
    assert raised.value.wait.followup_incident_ids == ("incident-0", "incident-1")
    assert not ops.events


def test_recovery_without_a_graded_incident_does_not_wait_for_open_incidents() -> None:
    from benchmarks.sregym.adapter.persistent import PersistentControllerError, pause_for_recovery

    elapsed, clock = _fake_clock()
    with pytest.raises(PersistentControllerError, match="recovery.*closed"):
        pause_for_recovery(_followup_ops(open_polls=None), "hotel-sdo", clock=clock)
    assert elapsed[0] == 0


def test_recovery_pause_acknowledges_closed_incident_before_return() -> None:
    from benchmarks.sregym.adapter.persistent import pause_for_recovery
    from tests.unit.benchmarks.sregym.adapter.test_persistent import FakeOps

    ops = FakeOps(states={"hotel-sdo": {"last_acknowledged_incident_id": "incident-1"}}, logs={"hotel-sdo": []})
    pause_for_recovery(ops, "hotel-sdo", incident_id="incident-1")
    assert ops.events == [("paused", "hotel-sdo")]
    assert json.loads(ops.logs["hotel-sdo"][-1])["controller_maintenance"] == "paused"


def test_persistent_conductor_cleanup_waits_for_the_same_recovery_fence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from benchmarks.sregym.adapter import driver

    monkeypatch.setenv("SREGYM_DEFER_CLEANUP", "1")
    monkeypatch.setattr(driver, "_application_repository", lambda: tmp_path)
    monkeypatch.setattr(driver, "_run_persistent", lambda *_: {"namespace": "hotel", "incident_id": "incident-1"})

    def blocked(*_args: object) -> None:
        raise RuntimeError("incident is still open")

    monkeypatch.setattr(driver, "_quiesce_persistent_cleanup", blocked, raising=False)
    monkeypatch.setattr(driver, "signal_cleanup", lambda *_: pytest.fail("cleanup before terminal pause"))
    assert driver._main_persistent(driver._parse_args([]), "http://conductor") == 1


def test_persistent_cleanup_drains_before_pausing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from benchmarks.sregym.adapter import driver

    events = []
    record = SimpleNamespace(
        pending=SimpleNamespace(incident_id="incident-1", repository=tmp_path), control_namespace="hotel-sdo"
    )
    state = SimpleNamespace(controllers={"hotel": record}, save=lambda *_: events.append("saved"))
    monkeypatch.setenv(driver.PERSISTENT_STATE_ENV, str(tmp_path / "state.json"))
    monkeypatch.setattr(driver.PersistentState, "load", lambda *_: state)
    monkeypatch.setattr(driver, "_cluster_ops", lambda *_: object())
    monkeypatch.setattr(driver, "drain_pending_incident", lambda *_args, **_kwargs: events.append("drained"))

    def pause(*_args: object, **_kwargs: object) -> None:
        assert record.pending is None
        events.append("paused")

    monkeypatch.setattr(driver, "pause_for_recovery", pause)
    driver._quiesce_persistent_cleanup(driver._parse_args([]), {"namespace": "hotel", "incident_id": "incident-1"})
    assert events == ["drained", "saved", "paused"]
