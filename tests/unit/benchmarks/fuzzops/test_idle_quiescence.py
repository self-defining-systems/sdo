"""The ``idle`` verb answers idle only when SDO itself is quiescent, and ``reset`` lets work in progress finish."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from benchmarks.fuzzops import hooks

OWN = "2026-11-20T16:51:58.384124803Z"
OTHER = "2026-11-20T17:00:37.422866601Z"


def _incident_state(detected: str = OTHER, **extra: Any) -> dict[str, Any]:
    """The shape of ``sdo-controller-state`` while an incident runs (a second incident of a run's lane)."""
    return {
        "version": "sdo.controller/v1",
        "dispatch_state": "DISPATCH_STATE_PENDING",
        "incident_open": True,
        "incident_detected_at": detected,
        "incident_request": {"incident_id": "fg1-1795194037422866601", "detected_at": detected},
        **extra,
    }


def _kubectl(state: dict[str, Any]) -> Any:
    fresh = (datetime.now(UTC) - timedelta(seconds=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    configmap = {"metadata": {"managedFields": [{"time": fresh}]}, "data": {"runtime-state.json": json.dumps(state)}}
    pods = {"items": [{"metadata": {"name": "p"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}]}

    def run(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        for key, value in {"configmap": configmap, "pods": pods}.items():
            if key in cmd:
                return subprocess.CompletedProcess(cmd, 0, json.dumps(value), "")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    return run


def _env(tmp_path: Path, **extra: str) -> dict[str, str]:
    lane = tmp_path / "lane"
    lane.mkdir(exist_ok=True)
    return {
        "FUZZOPS_NAMESPACE": "fg1",
        "FUZZOPS_KUBE_CONTEXT": "kind-c",
        "KUBECONFIG": "/k",
        "BENCH_LANE_DIR": str(lane),
        **extra,
    }


def test_an_open_incident_other_than_the_cells_own_is_not_idle(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The settle after a run named its own incident and the hook then answered idle for a second one that was still
    running; the lane was collected and released under it (no outcome, ``closure pending`` at the reset)."""
    monkeypatch.setattr(hooks.subprocess, "run", _kubectl(_incident_state()))
    lines: list[str] = []
    assert hooks.idle(_env(tmp_path, BENCH_OWN_DETECTED_AT=OWN), out=lines.append) == 1
    assert lines == ["alarm: incident open fg1-1795194037422866601"]


@pytest.mark.parametrize(
    ("state", "why"),
    [
        ({"dispatch_state": "DISPATCH_STATE_PENDING"}, "responder dispatch"),
        ({"dispatch_state": "DISPATCH_STATE_RUNNING"}, "responder dispatch"),
        ({"dispatch_state": "DISPATCH_STATE_WORKSPACE_PENDING"}, "responder dispatch"),
        ({"dispatch_state": "DISPATCH_STATE_WORKSPACE_RUNNING"}, "responder dispatch"),
        (
            {
                "dispatch_state": "DISPATCH_STATE_IDLE",
                "closure_state": "CLOSURE_STATE_COMMITTED",
                "pending_closure": {"request": {"incident_id": "i1"}},
            },
            "closure pending",
        ),
        ({"dispatch_state": "DISPATCH_STATE_IDLE", "queued_reflection_incident_ids": ["i1"]}, "reflection queued"),
    ],
)
def test_work_in_progress_is_not_idle(state: dict[str, Any], why: str) -> None:
    assert why in hooks.idle_reason({"version": "sdo.controller/v1", **state})


def test_a_quiet_controller_is_idle() -> None:
    state = {
        "version": "sdo.controller/v1",
        "dispatch_state": "DISPATCH_STATE_IDLE",
        "closure_state": "CLOSURE_STATE_COMMITTED",
    }
    assert hooks.idle_reason(state) == ""
    assert hooks.idle_reason({**state, "dispatch_state": "DISPATCH_STATE_COMPLETED"}) == ""


def _steps(events: list[str], busy: list[str]) -> hooks.ResetSteps:
    """A controller that is finishing a closure: ``busy`` holds the reasons it gives, one per idle poll."""

    def idle() -> str:
        events.append("idle")
        return busy.pop(0) if busy else ""

    return hooks.ResetSteps(
        resume_controller=lambda: events.append("resume"),
        reset_incidents=lambda: events.append("incidents"),
        retire_standby=lambda: events.append("retire"),
        stop_controller=lambda: events.append("stop"),
        restore_repository=lambda head: events.append("repo"),
        app_reset=lambda: (events.append("app") or {"match": True}),
        start_controller=lambda: events.append("start"),
        wait_standby=lambda: events.append("standby"),
        repository_state=lambda: {"head": "h" * 40, "status": "", "outcomes": ""},
        controller_idle=idle,
    )


def _lane(tmp_path: Path) -> dict[str, str]:
    env = _env(tmp_path)
    record = {"revision": "r" * 40, "memory": "m" * 40, "state": {"head": "h" * 40, "status": "", "outcomes": ""}}
    (Path(env["BENCH_LANE_DIR"]) / hooks.PREPARED_RECORD).write_text(json.dumps(record))
    return env


def test_a_closure_in_progress_finishes_before_the_controller_is_stopped_and_the_repository_restored(
    tmp_path: Path,
) -> None:
    """Stopping the controller mid-closure and moving the repository back strands the closure (``busy: closure
    pending`` after every reset); the run's outcome commit is expected and the reset restores the head after it."""
    events: list[str] = []
    hooks.reset_lane(_lane(tmp_path), _steps(events, ["busy: closure pending", "busy: reflection queued"]))
    assert events.index("stop") > events.index("idle")
    assert events.index("stop") > max(i for i, e in enumerate(events[: events.index("stop")]) if e == "idle")
    assert events.index("idle") < events.index("stop") < events.index("repo")
    assert events[: events.index("stop")].count("idle") == 3  # two busy answers, then idle


def test_a_controller_that_never_finishes_its_closure_is_not_stopped(tmp_path: Path) -> None:
    events: list[str] = []
    with pytest.raises(hooks.HookError, match="before the reset.*closure pending"):
        hooks.reset_lane(_lane(tmp_path), _steps(events, ["busy: closure pending"] * 10))
    assert "stop" not in events
    assert "repo" not in events
    assert "app" not in events
