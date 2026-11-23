"""Warm restoration must repair stale clients without treating Pod readiness as HTTP health."""

import json
from pathlib import Path
from typing import Any

import pytest

from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.reset import reset_environment
from tests.unit.benchmarks.sregym.fastloop.accepted_source_fixture import commit_accepted_source


class Lane:
    def __init__(self, *, refresh_succeeds: bool = True) -> None:
        self.objects = [
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "frontend", "namespace": "hotel-reservation"},
                "spec": {
                    "replicas": 1,
                    "template": {"spec": {"containers": [{"name": "app", "image": "example.invalid/app:1"}]}},
                },
            }
        ]
        self.client_registered = False
        self.refresh_succeeds = refresh_succeeds
        self.http_checks = 0
        self.refreshes = 0
        self.accepted = "accepted-memory"

    def list_objects(self, namespace: str) -> list[dict[str, Any]]:
        return self.objects

    def apply(self, namespace: str, value: dict[str, Any]) -> None:
        self.objects = [value]

    def delete(self, namespace: str, kind: str, name: str) -> None:
        raise AssertionError("No object deletion needed")

    def reset_controller(self, seed_commit: str, *, preserve_repository: bool = False) -> None:
        assert preserve_repository

    def request(self, op: str, **kwargs: Any) -> dict[str, Any]:
        if op == "clean_start":
            return {"success": True}
        if op == "health":
            return {"healthy": True}
        if op == "baseline_oracle":
            if kwargs.get("verify_traffic"):
                self.http_checks += 1
                return {
                    "success": self.client_registered,
                    "details": {"traffic_detail": "18/18" if self.client_registered else "12/18"},
                }
            return {"success": True}
        if op == "refresh_baseline_clients":
            self.refreshes += 1
            self.client_registered = self.refresh_succeeds
            return {"success": self.refresh_succeeds}
        raise AssertionError(op)


def environment(tmp_path: Path) -> FastloopEnvironment:
    env = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="synthetic",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel-reservation",
        application="hotel-reservation",
        workspace=tmp_path / "app",
        sregym_dir=tmp_path / "harness",
        private_tmp=None,
        seed_commit="a" * 40,
    )
    commit_accepted_source(env.workspace, Lane().objects, namespace="hotel-reservation", kustomize_root=True)
    baseline = ApplicationBaseline("hotel-reservation", Lane().objects)
    env.baseline_path.write_text(json.dumps({"fastloop_application": {"namespaces": [baseline.__dict__]}}))
    return env


def test_reset_refreshes_stale_clients_then_rechecks_real_http(tmp_path: Path) -> None:
    lane = Lane()
    reset_environment(environment(tmp_path), lane, lane, preserve_repository=True)
    assert lane.http_checks == 2
    assert lane.refreshes == 1
    assert lane.client_registered
    assert lane.accepted == "accepted-memory"


def test_reset_refuses_lane_when_client_refresh_cannot_restore_http(tmp_path: Path) -> None:
    lane = Lane(refresh_succeeds=False)
    with pytest.raises(RuntimeError, match="client refresh|traffic"):
        reset_environment(environment(tmp_path), lane, lane, preserve_repository=True)
    assert lane.refreshes == 1
    assert lane.accepted == "accepted-memory"


def test_refresh_success_does_not_replace_independent_http_verification(tmp_path: Path) -> None:
    class TrafficStillBroken(Lane):
        def request(self, op: str, **kwargs: Any) -> dict[str, Any]:
            reply = super().request(op, **kwargs)
            if op == "baseline_oracle" and kwargs.get("verify_traffic"):
                return {"success": False, "details": {"traffic_detail": "12/18"}}
            return reply

    lane = TrafficStillBroken()
    with pytest.raises(RuntimeError, match="independent traffic"):
        reset_environment(environment(tmp_path), lane, lane, preserve_repository=True)
    assert lane.http_checks == 2
    assert lane.refreshes == 1


def test_restored_is_signalled_once_the_writes_are_done_and_before_the_oracle_only_observes(tmp_path: Path) -> None:
    """The warm transaction starts the new controller on this signal: every write of the restore (objects, clients)
    precedes it, and only the baseline oracle's observation follows it."""
    from benchmarks.sregym.fastloop.reset import restore_application

    ops: list[str] = []

    class Recording(Lane):
        def __init__(self) -> None:
            super().__init__()
            self.client_registered = True

        def apply(self, namespace: str, value: dict[str, Any]) -> None:
            ops.append("apply")
            super().apply(namespace, value)

        def request(self, op: str, **kwargs: Any) -> dict[str, Any]:
            ops.append(op)
            return super().request(op, **kwargs)

    lane = Recording()
    lane.objects = [{**lane.objects[0], "spec": {**lane.objects[0]["spec"], "replicas": 3}}]
    restore_application(environment(tmp_path), lane, lane, restored=lambda: ops.append("restored"))
    assert ops.index("clean_start") < ops.index("apply") < ops.index("health") < ops.index("restored")
    assert ops[ops.index("restored") + 1 :] == ["baseline_oracle"]


def test_a_caller_that_takes_the_sustained_window_gets_it_after_the_oracle_and_it_fails_the_restore(
    tmp_path: Path,
) -> None:
    """The oracle's pod-health window is a minute of Kubernetes history; a re-rolled pod can only fill it by waiting.
    The warm transaction runs it beside the reseed and the proof, so the restore hands it over instead of blocking."""
    from benchmarks.sregym.fastloop.reset import restore_application

    requests: list[tuple[str, dict[str, Any]]] = []
    windows: list[Any] = []

    class Recording(Lane):
        def __init__(self) -> None:
            super().__init__()
            self.client_registered = True
            self.sustained = {"success": True, "problems": []}

        def request(self, op: str, **kwargs: Any) -> dict[str, Any]:
            requests.append((op, kwargs))
            if op == "sustained_health":
                return self.sustained
            return super().request(op, **kwargs)

    lane = Recording()
    restore_application(environment(tmp_path), lane, lane, sustained=windows.append)
    oracle = [kwargs for op, kwargs in requests if op == "baseline_oracle"]
    assert oracle
    assert all(kwargs["sustain"] is False for kwargs in oracle)
    assert "sustained_health" not in [op for op, _ in requests]
    assert len(windows) == 1
    windows[0]()
    assert requests[-1][0] == "sustained_health"
    lane.sustained = {"success": False, "problems": ["pod a container c restarted (0 -> 1)"]}
    with pytest.raises(RuntimeError, match="restarted"):
        windows[0]()


def test_without_a_caller_for_the_window_the_oracle_takes_it_inline(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.reset import restore_application

    seen: list[dict[str, Any]] = []

    class Recording(Lane):
        def __init__(self) -> None:
            super().__init__()
            self.client_registered = True

        def request(self, op: str, **kwargs: Any) -> dict[str, Any]:
            if op == "baseline_oracle":
                seen.append(kwargs)
            return super().request(op, **kwargs)

    lane = Recording()
    restore_application(environment(tmp_path), lane, lane)
    assert seen
    assert all("sustain" not in kwargs for kwargs in seen)
