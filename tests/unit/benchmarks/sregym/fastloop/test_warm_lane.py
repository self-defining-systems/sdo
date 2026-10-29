from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.adapter import controller_proof
from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, reconcile_application
from benchmarks.sregym.fastloop.warm_lane import LaneIdentity, WarmLaneError, prepare_lane

if TYPE_CHECKING:
    from pathlib import Path

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
REVISION = "a" * 40


class Lane:
    """Resource store consumed by the real reconciler and real controller proof."""

    def __init__(self) -> None:
        self.identity = LaneIdentity("controller-1", "standby-1", REVISION)
        self.objects = [self.service(99), self.service(80, "peer")]
        self.baseline = ApplicationBaseline("toy", [self.service(80), self.service(80, "peer")])
        self.writes: list[str] = []
        self.events: list[str] = []
        self.evaluated_at = NOW + timedelta(seconds=1)
        self.changed_at: str | None = None
        self.failed_at: str | None = None
        self.fallbacks = 0

    @staticmethod
    def service(port: int, name: str = "app") -> dict[str, Any]:
        return {"kind": "Service", "metadata": {"name": name, "namespace": "toy"}, "spec": {"ports": [{"port": port}]}}

    def list_objects(self, namespace: str) -> list[dict[str, Any]]:
        return self.objects

    def apply(self, namespace: str, value: dict[str, Any]) -> None:
        self.writes.append(value["metadata"]["name"])
        self.objects = [obj for obj in self.objects if obj["metadata"]["name"] != value["metadata"]["name"]] + [value]

    def delete(self, namespace: str, kind: str, name: str) -> None:
        self.objects = [obj for obj in self.objects if obj["metadata"]["name"] != name]

    def create_namespace(self, namespace: str) -> None:
        raise AssertionError("toy namespace exists")

    def step(self, name: str) -> None:
        self.events.append(name)
        if self.failed_at == name:
            raise RuntimeError("synthetic failure")
        if self.changed_at == name:
            self.identity = LaneIdentity("replacement", "standby-1", REVISION)

    def inspect(self) -> LaneIdentity:
        self.step("inspect")
        return self.identity

    def pause(self) -> None:
        self.step("pause")

    def reset_changed(self) -> None:
        self.step("reset")
        reconcile_application(self, self.baseline, "toy")

    def adopt(self) -> str:
        self.step("adopt")
        return self.identity.accepted_revision

    def prove(self, not_before: datetime, revision: str) -> dict[str, Any]:
        self.step("prove")
        return controller_proof(
            {"history": [{"detector_id": "health", "status": "clear", "evaluated_at": self.evaluated_at.isoformat()}]},
            {"health"},
            not_before.isoformat(),
        )

    def full_reset(self) -> None:
        self.fallbacks += 1
        self.step("full_reset")


def test_changed_only_reset_then_adopt_then_fresh_proof(tmp_path: Path) -> None:
    lane = Lane()
    receipt = prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)
    assert receipt.reused
    assert lane.writes == ["app"]
    assert lane.events.index("reset") < lane.events.index("adopt") < lane.events.index("prove")
    assert not lane.fallbacks
    # Real restoration producer -> real resource consumer, rather than a supplied clean flag.
    assert {obj["metadata"]["name"]: obj["spec"]["ports"][0]["port"] for obj in lane.objects} == {"app": 80, "peer": 80}


@pytest.mark.parametrize("step", ["inspect", "pause", "reset", "adopt", "prove"])
def test_each_uncertain_boundary_falls_back(tmp_path: Path, step: str) -> None:
    lane = Lane()
    lane.failed_at = step
    receipt = prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)
    assert not receipt.reused
    assert receipt.fallback_reason
    assert lane.fallbacks == 1


@pytest.mark.parametrize("step", ["pause", "reset", "adopt", "prove"])
def test_process_replacement_invalidates_transaction(tmp_path: Path, step: str) -> None:
    lane = Lane()
    lane.changed_at = step
    assert not prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW).reused
    assert lane.fallbacks == 1


def test_old_clear_sample_cannot_admit_warm_lane(tmp_path: Path) -> None:
    lane = Lane()
    lane.evaluated_at = NOW - timedelta(seconds=1)
    assert not prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW).reused
    assert lane.fallbacks == 1


def test_fallback_failure_propagates_and_never_publishes_ready(tmp_path: Path) -> None:
    lane = Lane()
    lane.evaluated_at = NOW - timedelta(seconds=1)
    lane.failed_at = "full_reset"
    with pytest.raises(WarmLaneError, match="full reset"):
        prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)


def test_busy_lane_is_never_wiped(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.warm_lane import WarmLaneBusy

    class BusyLane(Lane):
        def inspect(self) -> LaneIdentity:
            raise WarmLaneBusy("prior incident still owns the lane")

    lane = BusyLane()
    with pytest.raises(WarmLaneBusy):
        prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)
    assert lane.fallbacks == 0


def test_warm_option_is_explicit(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.cli import build_parser

    args = build_parser().parse_args(["run", "--run-dir", str(tmp_path), "--agent", "sdo"])
    assert not args.warm_lane
    args = build_parser().parse_args(["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--warm-lane"])
    assert args.warm_lane


@pytest.mark.parametrize("field", ["accepted_revision", "standby_uid"])
@pytest.mark.parametrize("step", ["pause", "reset", "adopt", "prove"])
def test_every_identity_component_is_bound_throughout_transaction(tmp_path: Path, field: str, step: str) -> None:
    from dataclasses import replace

    class ChangingLane(Lane):
        def step(self, name: str) -> None:
            super().step(name)
            if name == step:
                replacement = "b" * 40 if field == "accepted_revision" else "standby-2"
                self.identity = replace(self.identity, **{field: replacement})

    lane = ChangingLane()
    assert not prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW).reused
    assert lane.fallbacks == 1


def test_same_lane_cannot_have_two_warm_owners(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.warm_lane import WarmLaneBusy, own_lane

    with own_lane(tmp_path, "toy-lane"):
        with pytest.raises(WarmLaneBusy):
            with own_lane(tmp_path, "toy-lane"):
                pytest.fail("second owner entered the lane")
        # A distinct lane never shares a lock.
        with own_lane(tmp_path, "peer-lane"):
            pass
    # Kernel ownership is released on exit; the lock inode must remain.
    with own_lane(tmp_path, "toy-lane"):
        pass
