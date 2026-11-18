"""A compiled swap restarts the controller once, however many later steps restart it, and says what it did."""

from __future__ import annotations

import subprocess
from collections.abc import Iterator  # noqa: TC003
from contextlib import contextmanager
from pathlib import Path  # noqa: TC003

import pytest

from benchmarks.devswap import controller, devtree, swap, tree
from benchmarks.devswap.kube import SwapError


class FakeController:
    """The controller pods a lane has: a restart replaces the running one with a new one."""

    def __init__(self) -> None:
        self.running = {"pod-1"}
        self.restarts = 0

    def pod_ids(self) -> set[str]:
        return set(self.running)

    def restart(self) -> None:
        self.restarts += 1
        self.running = {f"pod-{self.restarts + 1}"}


def test_a_binary_placed_under_a_running_controller_needs_one_restart() -> None:
    controller = FakeController()
    tracker = swap.ControllerBinary(controller.pod_ids)
    tracker.placed()
    assert tracker.restart_if_stale(controller.restart)
    assert controller.restarts == 1


def test_a_restart_after_the_binary_was_placed_is_the_restart() -> None:
    controller = FakeController()
    tracker = swap.ControllerBinary(controller.pod_ids)
    tracker.placed()
    controller.restart()  # a seed import, a tree import or a lane reset
    assert not tracker.restart_if_stale(controller.restart)
    assert controller.restarts == 1


def test_a_swap_without_a_new_binary_restarts_nothing() -> None:
    controller = FakeController()
    tracker = swap.ControllerBinary(controller.pod_ids)
    assert not tracker.restart_if_stale(controller.restart)
    assert controller.restarts == 0


def test_a_second_check_does_not_restart_again() -> None:
    controller = FakeController()
    tracker = swap.ControllerBinary(controller.pod_ids)
    tracker.placed()
    assert tracker.restart_if_stale(controller.restart)
    assert not tracker.restart_if_stale(controller.restart)
    assert controller.restarts == 1


def test_only_controller_pods_that_run_or_are_about_to_count_as_the_controller() -> None:
    class Kube:
        def read_controller_pods(self, control: str) -> list[dict[str, object]]:
            return [
                {"metadata": {"uid": "running"}, "status": {"phase": "Running"}},
                {"metadata": {"uid": "starting"}, "status": {"phase": "Pending"}},
                {"metadata": {"uid": "ended"}, "status": {"phase": "Failed"}},
                {"metadata": {"uid": "done"}, "status": {"phase": "Succeeded"}},
                {"metadata": {"uid": "going", "deletionTimestamp": "t"}, "status": {"phase": "Running"}},
            ]

    assert controller.controller_pod_ids(Kube(), "ns") == {"running", "starting"}  # type: ignore[arg-type]


def test_a_controller_pod_list_kubectl_could_not_read_is_an_error_not_an_empty_list() -> None:
    """An empty answer would read as "no controller to restart" and the swap would report success on the old binary."""

    class Kube:
        def read_controller_pods(self, control: str) -> None:
            return None

    with pytest.raises(SwapError, match="could not read the controller pods"):
        controller.controller_pod_ids(Kube(), "ns")  # type: ignore[arg-type]
    tracker = swap.ControllerBinary(lambda: controller.controller_pod_ids(Kube(), "ns"))  # type: ignore[arg-type]
    with pytest.raises(SwapError):
        tracker.placed()


def test_phases_carry_the_seconds_of_each_named_step() -> None:
    phases = swap.Phases()
    with phases("project"):
        pass
    with phases("restart"):
        pass
    assert list(phases.seconds) == ["project", "restart"]
    assert all(isinstance(seconds, float) for seconds in phases.seconds.values())


def test_a_phase_that_fails_is_still_timed() -> None:
    phases = swap.Phases()
    with pytest.raises(RuntimeError), phases("tree"):
        raise RuntimeError("boom")
    assert "tree" in phases.seconds


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t", *args], capture_output=True, check=True
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    for rel in ("sdo/operator/go.mod", "sdo/operator/Dockerfile.runtime", "uv.lock"):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n")
    _git(tmp_path, "init", "-q", str(root))
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "base")
    return root


def test_an_image_change_is_refused_with_its_cause_and_the_way_out(repo: Path) -> None:
    lane = devtree.fingerprint(repo)
    (repo / "sdo/operator/Dockerfile.runtime").write_text("y\n")
    _git(repo, "commit", "-qam", "image")
    why = devtree.swap_problem(repo, lane, rebuild_binary=True)
    assert "Dockerfile.runtime" in why
    assert "base images" in why
    assert "full build" in why


def test_a_dependency_change_is_refused_with_its_cause(repo: Path) -> None:
    lane = devtree.fingerprint(repo)
    (repo / "uv.lock").write_text("y\n")
    _git(repo, "commit", "-qam", "deps")
    why = devtree.swap_problem(repo, lane, rebuild_binary=True)
    assert "uv.lock" in why
    assert "base images" in why


def _import_steps(calls: list[str], restore_fails: bool = False) -> tree.ImportSteps:
    @contextmanager
    def stopped() -> Iterator[None]:
        calls.append("stop")
        try:
            yield
        finally:
            calls.append("start")

    def restore() -> None:
        calls.append("restore")
        if restore_fails:
            raise SwapError("repository restore failed")

    return tree.ImportSteps(
        controller_stopped=stopped,
        retire_standby=lambda: calls.append("retire"),
        restore=restore,
        wait_standby=lambda: calls.append("wait"),
    )


def test_a_tree_import_stops_the_controller_before_it_retires_the_standby() -> None:
    """A running controller recreates a retired standby at once; that stale standby ends the wait for the restarted
    controller's own, and the next read fails with no running controller pod."""
    calls: list[str] = []
    tree.import_prepared_head(_import_steps(calls))
    assert calls == ["stop", "retire", "restore", "start", "wait"]


def test_a_tree_import_that_fails_after_the_stop_starts_the_controller_again() -> None:
    calls: list[str] = []
    with pytest.raises(SwapError, match="restore failed"):
        tree.import_prepared_head(_import_steps(calls, restore_fails=True))
    assert calls == ["stop", "retire", "restore", "start"]


def _pod(name: str, ready: bool = True, deleting: bool = False) -> dict[str, object]:
    meta: dict[str, object] = {"name": name}
    if deleting:
        meta["deletionTimestamp"] = "2026-11-18T00:00:00Z"
    return {"metadata": meta, "status": {"conditions": [{"type": "Ready", "status": "True" if ready else "False"}]}}


def test_a_retired_standby_that_is_still_ready_is_not_the_controllers_standby() -> None:
    assert controller.standby_ready([_pod("sdo-standby-a-x")])
    assert not controller.standby_ready([_pod("sdo-standby-a-x", deleting=True)])
    assert not controller.standby_ready([_pod("sdo-standby-a-x", ready=False)])
    assert not controller.standby_ready([_pod("sdo-controller-run-x")])
