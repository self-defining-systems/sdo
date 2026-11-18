"""The ``reset`` and ``snapshot`` hook verbs: SDO's state goes back in place and the app returns to its golden copy
while the controller is paused or stopped."""

from __future__ import annotations

import json
from pathlib import Path  # noqa: TC003

import pytest

from benchmarks.fuzzops import hooks


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

    with pytest.raises(hooks.HookError, match="after 2 full restores: pg/migrations differs"):
        hooks.prove_golden(verify, lambda: None, lambda _m: None)


def test_a_restore_that_itself_fails_the_gate_is_tried_once_more_before_the_claim_fails() -> None:
    # the pool lane pc5 failed its claim because the restore's own verify raced the app (a key or the schema was
    # still moving); the second restore, run on a settled app, proves the copy
    log: list[str] = []
    state = {"restores": 0}

    def verify() -> None:
        log.append("verify")
        if state["restores"] == 0:
            raise hooks.HookError("redis/cache:x:set has other content")

    def restore() -> None:
        state["restores"] += 1
        log.append("restore")
        if state["restores"] == 1:
            raise hooks.HookError(
                "app reset failed: reset: the data stores are not the golden copy's: postgres/columns differs"
            )
        log.append("restored")

    hooks.prove_golden(verify, restore, log.append)
    assert [e for e in log if e in ("verify", "restore", "restored")] == [
        "verify",
        "restore",
        "restore",
        "restored",
        "verify",
    ]


def test_a_claim_fails_naming_the_last_difference_when_every_restore_attempt_fails() -> None:
    def verify() -> None:
        raise hooks.HookError("first")

    def restore() -> None:
        raise hooks.HookError("app reset failed: postgres/columns differs")

    with pytest.raises(hooks.HookError, match="after 2 full restores: app reset failed: postgres/columns differs"):
        hooks.prove_golden(verify, restore, lambda _m: None)


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
