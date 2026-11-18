"""The ``reset`` and ``snapshot`` hook verbs: SDO's state goes back in place and the app returns to its golden copy
while the controller is paused or stopped."""

from __future__ import annotations

import json
from pathlib import Path  # noqa: TC003
from typing import Any

import pytest

from benchmarks.fuzzops import hooks
from benchmarks.fuzzops.hooks import HookError


class Recorder:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, name: str, *args: object) -> None:
        self.calls.append(name)


def lane(tmp_path: Path, state: bool = True) -> dict[str, str]:
    lane_dir = tmp_path / "lane"
    lane_dir.mkdir()
    record: dict[str, object] = {"revision": "r" * 40, "memory": "m" * 40}
    if state:
        record["state"] = {"head": "h" * 40, "status": "", "outcomes": ""}
    (lane_dir / hooks.PREPARED_RECORD).write_text(json.dumps(record))
    return {"BENCH_LANE_DIR": str(lane_dir), "BENCH_PACK": str(tmp_path / "pack"), "FUZZOPS_NAMESPACE": "fg1"}


def test_reset_pauses_the_controller_around_the_app_and_repository_reset(tmp_path: Path) -> None:
    rec = Recorder()
    steps = hooks.ResetSteps(
        resume_controller=lambda: None,
        reset_incidents=lambda: rec("incidents"),
        retire_standby=lambda: rec("retire"),
        stop_controller=lambda: rec("stop"),
        restore_repository=lambda head: rec(f"repo {head[:1]}"),
        app_reset=lambda: (rec("app") or {"match": True, "total_s": 3.0}),
        start_controller=lambda: rec("start"),
        wait_standby=lambda: rec("standby"),
        repository_state=lambda: {"head": "h" * 40, "status": "", "outcomes": ""},
        controller_idle=lambda: "",
    )
    receipt = hooks.reset_lane(lane(tmp_path), steps)
    assert rec.calls == ["incidents", "retire", "stop", "repo h", "app", "start", "standby"]
    assert receipt["memory"] == "m" * 40
    assert receipt["app"]["match"] is True


def test_reset_fails_when_the_app_is_not_the_golden_copy(tmp_path: Path) -> None:
    def broken() -> dict[str, object]:
        raise hooks.HookError("fingerprint mismatch")

    steps = hooks.ResetSteps(
        resume_controller=lambda: None,
        reset_incidents=lambda: None,
        retire_standby=lambda: None,
        stop_controller=lambda: None,
        restore_repository=lambda head: None,
        app_reset=broken,
        start_controller=lambda: None,
        wait_standby=lambda: None,
        repository_state=dict,
        controller_idle=lambda: "",
    )
    with pytest.raises(hooks.HookError, match="fingerprint mismatch"):
        hooks.reset_lane(lane(tmp_path), steps)


def test_reset_fails_when_the_repository_is_not_the_prepared_one(tmp_path: Path) -> None:
    steps = hooks.ResetSteps(
        resume_controller=lambda: None,
        reset_incidents=lambda: None,
        retire_standby=lambda: None,
        stop_controller=lambda: None,
        restore_repository=lambda head: None,
        app_reset=lambda: {"match": True},
        start_controller=lambda: None,
        wait_standby=lambda: None,
        repository_state=lambda: {"head": "x" * 40, "status": " M a", "outcomes": ""},
        controller_idle=lambda: "",
    )
    with pytest.raises(hooks.HookError, match="not the prepared"):
        hooks.reset_lane(lane(tmp_path), steps)


def test_reset_needs_the_prepared_state_record(tmp_path: Path) -> None:
    with pytest.raises(hooks.HookError, match="no recorded prepared state"):
        hooks.reset_lane(lane(tmp_path, state=False), hooks.ResetSteps.unused())


def test_reset_needs_idle_after_the_resume(tmp_path: Path) -> None:
    steps = hooks.ResetSteps(
        resume_controller=lambda: None,
        reset_incidents=lambda: None,
        retire_standby=lambda: None,
        stop_controller=lambda: None,
        restore_repository=lambda head: None,
        app_reset=lambda: {"match": True},
        start_controller=lambda: None,
        wait_standby=lambda: None,
        repository_state=lambda: {"head": "h" * 40, "status": "", "outcomes": ""},
        controller_idle=lambda: "incident open",
    )
    with pytest.raises(hooks.HookError, match="incident open"):
        hooks.reset_lane(lane(tmp_path), steps)


def _steps(rec: Recorder, state: dict[str, str]) -> hooks.ResetSteps:
    return hooks.ResetSteps(
        resume_controller=lambda: None,
        reset_incidents=lambda: rec("incidents"),
        retire_standby=lambda: rec("retire"),
        stop_controller=lambda: rec("stop"),
        restore_repository=lambda head: rec(f"repo {head[:1]}"),
        app_reset=lambda: (rec("app") or {"match": True}),
        start_controller=lambda: rec("start"),
        wait_standby=lambda: rec("standby"),
        repository_state=lambda: {"head": "h" * 40, "status": "", "outcomes": ""},
        controller_idle=lambda: "",
    )


def test_a_moved_idle_lane_repository_is_restored(tmp_path: Path) -> None:
    rec = Recorder()
    moved = {"head": "x" * 40, "status": "", "outcomes": "o"}
    assert hooks.restore_moved_repository(lane(tmp_path), _steps(rec, moved), moved, lambda _m: None) is True
    assert rec.calls == ["incidents", "retire", "stop", "repo h", "app", "start", "standby"]


def test_an_unmoved_idle_lane_repository_is_left_alone(tmp_path: Path) -> None:
    rec = Recorder()
    same = {"head": "h" * 40, "status": "", "outcomes": ""}
    assert hooks.restore_moved_repository(lane(tmp_path), _steps(rec, same), same, lambda _m: None) is False
    assert rec.calls == []


def test_a_claim_whose_stores_match_the_golden_copy_restores_nothing() -> None:
    restored: list[str] = []
    hooks.prove_golden(lambda: None, lambda: restored.append("restore"), lambda _m: None)
    assert restored == []


def test_a_claim_whose_stores_differ_forces_a_full_restore_then_proves_it() -> None:
    log: list[str] = []
    state = {"clean": False}

    def verify() -> None:
        log.append("verify")
        if not state["clean"]:
            raise hooks.HookError("pg/migrations differs")

    def restore() -> None:
        log.append("restore")
        state["clean"] = True

    hooks.prove_golden(verify, restore, log.append)
    assert [e for e in log if e in ("verify", "restore")] == ["verify", "restore", "verify"]


def test_a_claim_whose_stores_still_differ_after_the_restore_fails_loudly() -> None:
    def verify() -> None:
        raise hooks.HookError("pg/migrations differs")

    with pytest.raises(hooks.HookError, match="after a full restore: pg/migrations differs"):
        hooks.prove_golden(verify, lambda: None, lambda _m: None)


def test_a_failed_snapshot_leaves_no_golden_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env = lane(tmp_path)
    golden = hooks.golden_directory(env)
    golden.mkdir()
    (golden / "meta.json").write_text("{}")
    monkeypatch.setattr(hooks, "set_maintenance", lambda *_a, **_k: None)
    monkeypatch.setattr(hooks, "_kube", lambda _e: None)
    monkeypatch.setattr(hooks, "control_namespace", lambda _e: "ns")
    monkeypatch.setattr(hooks, "fuzzops_app", lambda _e, _v: {"match": True})
    monkeypatch.setattr(hooks, "wait_app_ready", lambda *_a, **_k: None)
    monkeypatch.setattr(hooks, "wait_idle_stable", lambda *_a, **_k: "idle for less than the quiet period")
    with pytest.raises(hooks.HookError, match="not idle after the snapshot"):
        hooks.snapshot(env, lambda _m: None)
    assert not golden.exists()


def test_the_snapshot_is_refused_for_an_app_that_differs_from_the_fresh_deploy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[list[str]] = []

    class Done:
        returncode = 0
        stdout = '{"match": true}\n'
        stderr = ""

    monkeypatch.setattr(hooks.subprocess, "run", lambda cmd, **_k: seen.append(cmd) or Done())
    env = {**lane(tmp_path), "FUZZOPS_BIN": "/bin/fuzzops", hooks.FRESH_FINGERPRINT_ENV: "/lane/fresh.json"}
    hooks.fuzzops_app(env, "snapshot")
    assert seen[0][-2:] == ["--fresh", "/lane/fresh.json"]


def _recording_steps(rec: Recorder, **overrides: Any) -> hooks.ResetSteps:
    fields: dict[str, Any] = {
        "resume_controller": lambda: rec("resume"),
        "reset_incidents": lambda: rec("incidents"),
        "retire_standby": lambda: rec("retire"),
        "stop_controller": lambda: rec("stop"),
        "restore_repository": lambda head: rec("repo"),
        "app_reset": lambda: (rec("app") or {"match": True}),
        "start_controller": lambda: rec("start"),
        "wait_standby": lambda: rec("standby"),
        "repository_state": lambda: {"head": "h" * 40, "status": "", "outcomes": ""},
        "controller_idle": lambda: "",
    }
    return hooks.ResetSteps(**{**fields, **overrides})


def test_a_reset_first_makes_sure_the_controller_runs(tmp_path: Path) -> None:
    """A failed swap or reset leaves the controller Job suspended; nothing acknowledges the incident reset then."""
    rec = Recorder()
    hooks.reset_lane(lane(tmp_path), _recording_steps(rec))
    assert rec.calls[:2] == ["resume", "incidents"]


@pytest.mark.parametrize("failing", ["restore_repository", "app_reset"])
def test_a_failed_reset_does_not_leave_the_controller_stopped(tmp_path: Path, failing: str) -> None:
    rec = Recorder()

    def broken(*_: object) -> Any:
        rec("broken")
        raise hooks.HookError("the data stores are not the golden copy's")

    with pytest.raises(hooks.HookError, match="golden copy"):
        hooks.reset_lane(lane(tmp_path), _recording_steps(rec, **{failing: broken}))
    assert rec.calls.index("stop") < rec.calls.index("broken")
    assert rec.calls[-1] == "start"


def test_a_failed_reset_surfaces_the_error_even_when_the_restart_fails_too(tmp_path: Path) -> None:
    def broken() -> dict[str, Any]:
        raise hooks.HookError("fingerprint mismatch")

    def no_restart() -> None:
        raise hooks.HookError("the job could not be patched")

    steps = _recording_steps(Recorder(), app_reset=broken, start_controller=no_restart)
    with pytest.raises(hooks.HookError, match="fingerprint mismatch"):
        hooks.reset_lane(lane(tmp_path), steps)


def test_a_failed_reset_is_tried_again_and_the_lane_is_recycled(tmp_path: Path) -> None:
    rec = Recorder()
    attempts = [0]

    def flaky() -> dict[str, Any]:
        attempts[0] += 1
        if attempts[0] == 1:
            raise hooks.HookError("volume restore failed")
        return {"match": True}

    messages: list[str] = []
    receipt = hooks.reset_lane_again(lane(tmp_path), _recording_steps(rec, app_reset=flaky), messages.append)
    assert attempts[0] == 2
    assert receipt["app"]["match"] is True
    assert any("volume restore failed" in m and "trying again" in m for m in messages)
    # the second pass starts from a running controller
    assert rec.calls.count("resume") == 2


def test_a_reset_that_fails_twice_is_reported(tmp_path: Path) -> None:
    def broken() -> dict[str, Any]:
        raise hooks.HookError("volume restore failed")

    with pytest.raises(hooks.HookError, match="volume restore failed"):
        hooks.reset_lane_again(lane(tmp_path), _recording_steps(Recorder(), app_reset=broken), lambda _m: None)


class _JobKube:
    """A lane cluster whose controller Job reads as ``suspended`` (None: the read fails)."""

    def __init__(self, suspended: bool | None) -> None:
        self.suspended = suspended

    def controller_suspended(self, control: str) -> bool | None:
        return self.suspended


def test_the_resume_step_patches_the_job_even_when_it_cannot_be_read(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed ``kubectl get job`` must not skip the resume: the wait would then run against a suspended Job."""
    patched: list[bool] = []
    monkeypatch.setattr(hooks, "set_controller_suspended", lambda _k, _c, suspended, **_: patched.append(suspended))
    assert hooks.resume_stopped_controller(_JobKube(None), "ns") is True  # type: ignore[arg-type]
    assert patched == [False]
    patched.clear()
    assert hooks.resume_stopped_controller(_JobKube(True), "ns") is True  # type: ignore[arg-type]
    assert hooks.resume_stopped_controller(_JobKube(False), "ns") is False  # type: ignore[arg-type]
    assert patched == [False, False]


def test_an_unreadable_job_is_unknown_not_running(monkeypatch: pytest.MonkeyPatch) -> None:
    kube = hooks.Kube(context="")
    monkeypatch.setattr(kube, "_json", lambda *_a: None)
    assert kube.controller_suspended("ns") is None
    monkeypatch.setattr(kube, "_json", lambda *_a: {"spec": {"suspend": True}})
    assert kube.controller_suspended("ns") is True
    monkeypatch.setattr(kube, "_json", lambda *_a: {"spec": {}})
    assert kube.controller_suspended("ns") is False


def _wired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, suspended: bool | None
) -> tuple[hooks.ResetSteps, list[float]]:
    patiences: list[float] = []
    monkeypatch.setattr(hooks, "_kube", lambda _e: _JobKube(suspended))
    monkeypatch.setattr(hooks, "control_namespace", lambda _e: "ns")
    monkeypatch.setattr(hooks, "_binary", lambda _e: tmp_path / "sdo-operator")
    monkeypatch.setattr(hooks, "set_controller_suspended", lambda *_a, **_k: None)
    monkeypatch.setattr(
        hooks,
        "reset_when_running",
        lambda _b, _c, _k, patience, **_: patiences.append(patience) or {"acknowledged": True},
    )
    return hooks.reset_steps({}, lambda: {"match": True}), patiences


def test_the_wired_reset_waits_for_a_controller_it_just_resumed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    steps, patiences = _wired(tmp_path, monkeypatch, suspended=True)
    steps.resume_controller()
    steps.reset_incidents()
    assert patiences == [hooks.RESET_START_PATIENCE_S]
    assert patiences[0] > hooks.RESET_PATIENCE_S


def test_the_wired_reset_of_a_running_controller_fails_soon_when_it_is_dead(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dead controller must fail the reset well inside a rebuild's 19 minutes, not after the start-up bound."""
    steps, patiences = _wired(tmp_path, monkeypatch, suspended=False)
    steps.resume_controller()
    steps.reset_incidents()
    assert patiences == [hooks.RESET_PATIENCE_S]
    assert hooks.RESET_PATIENCE_S < 10 * 60


def test_the_wired_incident_reset_is_retried_not_a_single_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``reset_steps`` once asked for the reset a single time (one 2-minute acknowledgement): it retries until the
    controller answers."""
    answers = [HookError("no ack"), HookError("no ack"), {"acknowledged": True}]

    def ask(*_a: object, **_k: object) -> dict[str, object]:
        answer = answers.pop(0)
        if isinstance(answer, hooks.HookError):
            raise answer
        return answer

    monkeypatch.setattr(hooks, "_kube", lambda _e: _JobKube(False))
    monkeypatch.setattr(hooks, "control_namespace", lambda _e: "ns")
    monkeypatch.setattr(hooks, "_binary", lambda _e: tmp_path / "sdo-operator")
    monkeypatch.setattr(hooks, "reset_incidents", ask)
    monkeypatch.setattr(hooks.time, "sleep", lambda _s: None)
    steps = hooks.reset_steps({}, lambda: {"match": True})
    steps.reset_incidents()
    assert answers == []


def test_a_controller_stopped_block_always_resumes_the_job(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[bool] = []
    monkeypatch.setattr(hooks, "set_controller_suspended", lambda _k, _c, suspended, **_: calls.append(suspended))
    with hooks.controller_stopped(None, "ns"):  # type: ignore[arg-type]
        pass
    assert calls == [True, False]
    calls.clear()
    with pytest.raises(hooks.HookError, match="config overlay"), hooks.controller_stopped(None, "ns"):  # type: ignore[arg-type]
        raise hooks.HookError("config overlay failed")
    assert calls == [True, False]


def test_a_stop_that_times_out_still_resumes_the_job(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[bool] = []

    def suspend(_k: object, _c: str, suspended: bool, **_: object) -> None:
        calls.append(suspended)
        if suspended:
            raise hooks.HookError("the controller pod did not end within 300s")

    monkeypatch.setattr(hooks, "set_controller_suspended", suspend)
    with pytest.raises(hooks.HookError, match="did not end"), hooks.controller_stopped(None, "ns"):  # type: ignore[arg-type]
        pytest.fail("the block must not run")
    assert calls == [True, False]


def test_the_reset_needs_at_least_one_attempt(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        hooks.reset_lane_again(lane(tmp_path), _recording_steps(Recorder()), lambda _m: None, attempts=0)


def test_a_restarted_controller_gets_time_to_start_before_the_swap_gives_up() -> None:
    assert hooks.SWAP_STANDBY_WAIT_S >= 600
