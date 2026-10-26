"""A supported teardown leaves no open incident for the next run to inherit."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.adapter import closure_pending, persistent

if TYPE_CHECKING:
    from pathlib import Path

CONTROL = "hotel-sdo"
PENDING = {"pending_closure": {"request": {"incident_id": "unclosable"}}, "closure_state": "CLOSURE_STATE_PENDING"}


class FakeApi:
    """ConfigMaps in the control namespace, served through the kubectl seam."""

    def __init__(self) -> None:
        self.configmaps: dict[str, dict[str, Any]] = {
            # The storage edge identifies its checkpoint by its CAS annotation and typed key.
            "checkpoint": {
                "metadata": {"name": "checkpoint", "annotations": {"sdo.dev/state-revision": "7"}},
                "data": {"runtime-state.json": json.dumps(PENDING)},
            },
            "onboarding": {
                "metadata": {"name": "onboarding", "labels": {"app.kubernetes.io/managed-by": "sdo"}},
                "data": {"onboarding.json": "{}"},
            },
            "unrelated": {"metadata": {"name": "unrelated"}, "data": {"runtime-state.json": "{}"}},
        }
        self.stopped = False

    def kubectl(self, args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if kwargs.get("namespace") not in (CONTROL, None):
            raise AssertionError(f"teardown left the control namespace: {args}")
        if args[:2] == ["get", "pods"]:
            return subprocess.CompletedProcess(args, 0, json.dumps({"items": []}), "")
        if args[:2] == ["get", "configmaps"]:
            items = list(self.configmaps.values())
            return subprocess.CompletedProcess(args, 0, json.dumps({"items": items}), "")
        if args[:1] == ["get"] and args[1].startswith("configmap/"):
            name = args[1].removeprefix("configmap/")
            if name not in self.configmaps:
                return subprocess.CompletedProcess(args, 1, "", "(NotFound)")
            return subprocess.CompletedProcess(args, 0, json.dumps(self.configmaps[name]), "")
        if args[:2] == ["delete", "configmap"]:
            assert self.stopped, "state retired while the controller could still write it"
            self.configmaps.pop(args[2], None)
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[:1] == ["delete"]:
            return subprocess.CompletedProcess(args, 0, "", "")
        raise AssertionError(f"unexpected kubectl call: {args}")

    def runtime_state(self) -> dict[str, Any]:
        for configmap in self.configmaps.values():
            if "sdo.dev/state-revision" in configmap["metadata"].get("annotations", {}):
                return json.loads(configmap["data"]["runtime-state.json"])
        return {}


@pytest.fixture
def api(monkeypatch: pytest.MonkeyPatch) -> FakeApi:
    from benchmarks.sregym.adapter import native_operator

    fake = FakeApi()

    def stop(*_args: Any, **_kwargs: Any) -> dict[str, list[str]]:
        fake.stopped = True
        return {"removed": ["job/sdo-controller-run"], "leftovers": []}

    monkeypatch.setattr(native_operator, "stop_runtime", stop)
    monkeypatch.setattr(persistent, "kubectl", fake.kubectl)
    return fake


def _seed(path: Path, *, pending: bool) -> None:
    record = persistent.ControllerRecord(
        application="hotel",
        namespace="hotel",
        control_namespace=CONTROL,
        lifecycle_fingerprint="f",
        controller_pod_uid="gone",
        controller_pod_name="gone",
        installed_by_stage="s0",
        installed_at=datetime.now(timezone.utc),
        runtime_config={},
        pending=(
            persistent.PendingIncident(
                incident_id="unclosable",
                stage_label="s0",
                receipt_dir=path.parent / "s0",
                repository=path.parent,
                verified_at=None,
                stage_receipt={},
            )
            if pending
            else None
        ),
    )
    persistent.PersistentState(controllers={"hotel": record}).save(path)


@pytest.mark.parametrize("pending", [True, False])
def test_teardown_retires_open_closure_and_keeps_namespace_authority(
    tmp_path: Path, api: FakeApi, pending: bool
) -> None:
    path = tmp_path / "sdo_persistent_controller.json"
    _seed(path, pending=pending)

    assert persistent.teardown(path, ops=persistent.KubectlClusterOps()) == []

    assert not closure_pending(api.runtime_state())
    assert set(api.configmaps) == {"onboarding", "unrelated"}
    archived = json.loads(
        (tmp_path / "sdo_persistent_controller.json.teardown-state" / CONTROL / "checkpoint.json").read_text()
    )
    assert json.loads(archived["data"]["runtime-state.json"]) == PENDING
    assert persistent.PersistentState.load(path).controllers == {}


def test_abort_retires_open_closure_only_on_a_quiet_lane(tmp_path: Path, api: FakeApi, monkeypatch) -> None:
    from benchmarks.sregym.adapter import processes

    monkeypatch.setattr(processes, "terminate_run", lambda _path: [])
    path = tmp_path / "sdo_persistent_controller.json"
    _seed(path, pending=True)
    ops = persistent.KubectlClusterOps()
    leftovers = ["pod/stuck"]
    monkeypatch.setattr(ops, "protect_incident_work", lambda *_args: None)
    monkeypatch.setattr(ops, "abort_leftovers", lambda _target: leftovers)

    assert persistent.abort(path, ops=ops) == ["hotel: leftover pod/stuck"]
    assert closure_pending(api.runtime_state()), "a lane with live leftovers keeps its evidence"

    leftovers.clear()
    assert persistent.abort(path, ops=ops) == []
    assert not closure_pending(api.runtime_state())
    assert "onboarding" in api.configmaps
