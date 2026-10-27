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
                "kind": "Deployment",
                "metadata": {"name": "frontend", "namespace": "hotel-reservation"},
                "spec": {"replicas": 1},
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
    commit_accepted_source(env.workspace, namespace="hotel-reservation")
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
