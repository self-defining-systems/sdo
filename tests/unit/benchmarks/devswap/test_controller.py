"""The controller primitives of a lane: the restore pod that puts a memory on the accepted repository."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from benchmarks.devswap import controller
from benchmarks.devswap.kube import Kubectl, SwapError


def test_the_restore_pod_receives_the_bundle_in_its_own_writable_volume(monkeypatch: pytest.MonkeyPatch) -> None:
    """The controller image runs with a read-only root: the bundle goes to an emptyDir, never to /tmp."""
    head = "a" * 40
    job = {
        "spec": {
            "template": {
                "spec": {
                    "volumes": [{"name": "repository", "persistentVolumeClaim": {"claimName": "repo"}}],
                    "containers": [
                        {
                            "name": controller.CONTROLLER_CONTAINER,
                            "image": "img",
                            "securityContext": {"readOnlyRootFilesystem": True},
                            "volumeMounts": [{"name": "repository", "mountPath": "/repo"}],
                        }
                    ],
                }
            }
        }
    }
    applied: list[dict[str, Any]] = []

    def fake_run(cmd: list[str], what: str, timeout: float, **kw: Any) -> Any:
        if kw.get("input") is not None:
            applied.append(json.loads(kw["input"]))
        return SimpleNamespace(stdout=head, stderr="")

    monkeypatch.setattr(controller, "run", fake_run)
    monkeypatch.setattr(controller, "send_bundle", lambda *a, **k: None)
    monkeypatch.setattr(
        controller.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout="Succeeded", stderr="", returncode=0)
    )
    kube = SimpleNamespace(get_json=lambda *a: job, cmd=lambda *a: list(a))
    controller.restore_repository(kube, "ctl", head, bundle=Path("b"))  # type: ignore[arg-type]
    pod = applied[0]["spec"]
    container = pod["containers"][0]
    assert {"name": "seed", "mountPath": controller.BUNDLE_DIR} in container["volumeMounts"]
    assert {"name": "seed", "emptyDir": {}} in pod["volumes"]
    assert "/tmp/" not in container["command"][2]
    assert f"{controller.BUNDLE_DIR}/memory.bundle" in container["command"][2]
    # a memory with another history leaves no broker proposals or ledger describing the old one
    assert "rm -rf /repo/worktrees/*" in container["command"][2]


def test_an_unreadable_job_is_unknown_not_running(monkeypatch: pytest.MonkeyPatch) -> None:
    kube = Kubectl(context="")
    monkeypatch.setattr(kube, "get_json", lambda *_a: None)
    assert kube.controller_suspended("ns") is None
    monkeypatch.setattr(kube, "get_json", lambda *_a: {"spec": {"suspend": True}})
    assert kube.controller_suspended("ns") is True
    monkeypatch.setattr(kube, "get_json", lambda *_a: {"spec": {}})
    assert kube.controller_suspended("ns") is False


def test_a_controller_stopped_block_always_resumes_the_job(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[bool] = []
    monkeypatch.setattr(controller, "set_controller_suspended", lambda _k, _c, suspended, **_: calls.append(suspended))
    with controller.controller_stopped(None, "ns"):  # type: ignore[arg-type]
        pass
    assert calls == [True, False]
    calls.clear()
    with pytest.raises(SwapError, match="config overlay"), controller.controller_stopped(None, "ns"):  # type: ignore[arg-type]
        raise SwapError("config overlay failed")
    assert calls == [True, False]


def test_a_stop_that_times_out_still_resumes_the_job(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[bool] = []

    def suspend(_k: object, _c: str, suspended: bool, **_: object) -> None:
        calls.append(suspended)
        if suspended:
            raise SwapError("the controller pod did not end within 300s")

    monkeypatch.setattr(controller, "set_controller_suspended", suspend)
    with pytest.raises(SwapError, match="did not end"), controller.controller_stopped(None, "ns"):  # type: ignore[arg-type]
        pytest.fail("the block must not run")
    assert calls == [True, False]


class LeaseLane:
    """A cluster's controller Job and leader Lease as ``kubectl`` calls see them: the process killed by a stop never
    releases the Lease, so it names a dead holder until its 60 s run out. ``reads`` is what each pod-list read returns
    (None: kubectl could not read it); the last one repeats."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, reads: list[list[dict[str, Any]] | None]) -> None:
        self.calls: list[list[str]] = []
        self.reads = list(reads)
        self.read_count = 0
        self.lease_held = True
        self.lease_deleted_after_reads = -1
        self.delete_fails = False
        monkeypatch.setattr(controller, "run", self.run)
        monkeypatch.setattr(controller.time, "sleep", lambda _s: None)

    def run(self, args: list[str], what: str, timeout: float = 600, **_: Any) -> Any:
        self.calls.append(args)
        if "lease" in args and "delete" in args:
            if self.delete_fails:
                raise SwapError("delete lease failed (1): forbidden")
            self.lease_held = False
            self.lease_deleted_after_reads = self.read_count
        return SimpleNamespace(stdout="", stderr="", returncode=0)

    def kube(self) -> Kubectl:
        kube = Kubectl(context="c")

        def read(_control: str) -> list[dict[str, Any]] | None:
            self.read_count += 1
            return self.reads.pop(0) if len(self.reads) > 1 else self.reads[0]

        kube.read_controller_pods = read  # type: ignore[method-assign]
        return kube


def test_stopping_the_controller_frees_its_leader_lease_for_the_next_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """The stopped controller process is killed before it can release the Lease, and the controller that starts next
    would wait out the 60 s the dead one still holds (measured: 41-60 s of every restart)."""
    lane = LeaseLane(monkeypatch, [[{}, {}], [{}], []])
    controller.set_controller_suspended(lane.kube(), "ns", True)
    assert not lane.lease_held
    assert lane.lease_deleted_after_reads == 3  # only once a read showed no controller pod
    [delete] = [call for call in lane.calls if "lease" in call]
    assert delete[-5:] == ["delete", "lease", controller.CONTROLLER_LEASE, "--ignore-not-found", "--wait=false"]


def test_an_unreadable_pod_list_is_not_a_stopped_controller(monkeypatch: pytest.MonkeyPatch) -> None:
    """A kubectl read that fails says nothing about the pod: the Lease of a controller that may still run (and renew it)
    must not be deleted, or a second controller could take it over."""
    lane = LeaseLane(monkeypatch, [None, None, [{}], None, []])
    controller.set_controller_suspended(lane.kube(), "ns", True)
    assert lane.lease_deleted_after_reads == 5  # not after the unreadable reads, nor the read that still showed a pod
    assert not lane.lease_held


def test_a_pod_list_that_stays_unreadable_never_deletes_the_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    lane = LeaseLane(monkeypatch, [None])
    clock = iter([0.0, 1.0, 400.0])
    monkeypatch.setattr(controller.time, "monotonic", lambda: next(clock))
    with pytest.raises(SwapError, match="did not end"):
        controller.set_controller_suspended(lane.kube(), "ns", True)
    assert lane.lease_held


def test_a_lease_that_cannot_be_deleted_does_not_fail_the_stop(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Freeing the Lease only saves time: it expires by itself."""
    lane = LeaseLane(monkeypatch, [[]])
    lane.delete_fails = True
    controller.set_controller_suspended(lane.kube(), "ns", True)
    assert "expires by itself" in capsys.readouterr().err


def test_resuming_the_controller_leaves_the_lease_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    lane = LeaseLane(monkeypatch, [[]])
    controller.set_controller_suspended(lane.kube(), "ns", False)
    assert lane.lease_held
