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
