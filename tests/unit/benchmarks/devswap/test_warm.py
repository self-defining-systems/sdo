"""The Go build cache warm: the pod built from the controller Job, and a step that never fails the swap."""

from __future__ import annotations

import json
import subprocess
from typing import Any

import pytest

from benchmarks.devswap import warm
from benchmarks.devswap.kube import APPLICATION_ROOT, Kubectl, SwapError


def _job(**overrides: Any) -> dict[str, Any]:
    controller: dict[str, Any] = {
        "name": "controller",
        "image": "sdo-controller:dev",
        "imagePullPolicy": "IfNotPresent",
        "command": ["sdo-operator", "run"],
        "envFrom": [{"secretRef": {"name": "credentials"}}],
        "env": [
            {"name": "GOCACHE", "value": "/workspace/.sdo-runtime/build/go-cache"},
            {"name": "TMPDIR", "value": "/workspace/.sdo-runtime/build/tmp"},
            {"name": "SDO_DEV_MOUNT", "value": "1"},
            {"name": "SDO_CONTROLLER_POD_UID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}},
        ],
        "resources": {"limits": {"cpu": "4", "memory": "4Gi"}},
        "securityContext": {"runAsNonRoot": True},
        "volumeMounts": [
            {"name": "repository", "mountPath": "/workspace"},
            {"name": "scratch", "mountPath": "/tmp"},
            {"name": "credentials", "mountPath": "/sdo/credentials", "readOnly": True},
            {"name": "dev-sdo", "mountPath": "/opt/sdo/sdo", "readOnly": True},
            {"name": "dev-operator", "mountPath": "/usr/local/bin/sdo-operator", "readOnly": True},
        ],
    }
    volumes = [
        {"name": "repository", "persistentVolumeClaim": {"claimName": "repo"}},
        {"name": "scratch", "emptyDir": {}},
        {"name": "credentials", "secret": {"secretName": "credentials"}},
        {"name": "dev-sdo", "hostPath": {"path": "/mnt/dev/sdo", "type": "Directory"}},
        {"name": "dev-operator", "hostPath": {"path": "/mnt/dev/.fastloop/sdo-operator", "type": "File"}},
    ]
    spec: dict[str, Any] = {
        "containers": [{"name": "sidecar", "image": "x"}, controller],
        "volumes": volumes,
        "securityContext": {"fsGroup": 65532},
        **overrides,
    }
    return {"spec": {"template": {"spec": spec}}}


def test_the_warm_pod_runs_the_controllers_environment_on_its_cache_and_the_dev_mount() -> None:
    pod = warm.warm_pod(_job(), "control")
    assert pod["metadata"] == {"name": warm.WARM_POD, "namespace": "control"}
    spec = pod["spec"]
    assert spec["restartPolicy"] == "Never"
    assert spec["securityContext"] == {"fsGroup": 65532}
    (container,) = spec["containers"]
    assert container["image"] == "sdo-controller:dev"
    assert container["command"] == [
        "/usr/local/bin/sdo-operator",
        "warm-memory-build",
        "--repository",
        APPLICATION_ROOT,
    ]
    # the same Go environment (cache on the repository volume), nothing that needs the pod's own identity
    assert {e["name"]: e["value"] for e in container["env"]} == {
        "GOCACHE": "/workspace/.sdo-runtime/build/go-cache",
        "TMPDIR": "/workspace/.sdo-runtime/build/tmp",
        "SDO_DEV_MOUNT": "1",
    }
    assert "envFrom" not in container
    assert container["resources"] == {"limits": {"cpu": "4", "memory": "4Gi"}}
    assert container["securityContext"] == {"runAsNonRoot": True}
    # the repository (cache and accepted memory), scratch and the projected tree; no credentials
    assert [m["name"] for m in container["volumeMounts"]] == ["repository", "scratch", "dev-sdo", "dev-operator"]
    assert [v["name"] for v in spec["volumes"]] == ["repository", "scratch", "dev-sdo", "dev-operator"]


def test_a_job_without_the_repository_volume_cannot_be_warmed() -> None:
    job = _job()
    spec = job["spec"]["template"]["spec"]
    spec["volumes"] = [v for v in spec["volumes"] if v["name"] != "repository"]
    with pytest.raises(SwapError, match="repository"):
        warm.warm_pod(job, "control")


class FakeKubectl(Kubectl):
    """The lane's pod phase, as kubectl reports it."""

    def __init__(self, job: dict[str, Any] | None) -> None:
        super().__init__("ctx")
        self.job = job

    def get_json(self, *args: str) -> dict[str, Any] | None:
        return self.job


class Cluster:
    def __init__(self, monkeypatch: pytest.MonkeyPatch, phases: list[str], log: str = '{"seconds": 31.5}') -> None:
        self.verbs: list[str] = []
        self.phases = list(phases)
        self.log = log
        self.apply_fails = False
        monkeypatch.setattr(warm.subprocess, "run", self.run)
        monkeypatch.setattr(warm.time, "sleep", lambda _s: None)

    def run(self, args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        verb = next(a for a in args if a in {"apply", "delete", "logs", "get"})
        self.verbs.append(verb)
        code, out = 0, ""
        if verb == "apply":
            code = 1 if self.apply_fails else 0
            assert json.loads(kwargs["input"])["metadata"]["name"] == warm.WARM_POD
        elif verb == "get":
            out = self.phases.pop(0) if len(self.phases) > 1 else self.phases[0]
        elif verb == "logs":
            out = self.log
        return subprocess.CompletedProcess(args, code, out, "boom" if code else "")


def _warm(kube: Kubectl, messages: list[str], timeout: float = 60.0) -> warm.GoCacheWarm:
    return warm.GoCacheWarm(kube, "control", messages.append, timeout)


def test_a_warm_that_succeeds_waits_for_the_pod_and_removes_it(monkeypatch: pytest.MonkeyPatch) -> None:
    cluster = Cluster(monkeypatch, ["Pending", "Running", "Succeeded"])
    messages: list[str] = []
    step = _warm(FakeKubectl(_job()), messages)
    step.start()
    assert cluster.verbs == ["delete", "apply"]  # a stale pod of an earlier swap goes first
    step.finish()
    assert cluster.verbs[-2:] == ["logs", "delete"]
    assert any("31.5" in m for m in messages)
    step.abort()  # nothing is left to remove
    assert cluster.verbs.count("delete") == 2


@pytest.mark.parametrize("phase", ["Failed", "Running"])
def test_a_warm_that_fails_or_overruns_is_reported_and_never_raises(
    monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    cluster = Cluster(monkeypatch, [phase], log="go: build failed")
    messages: list[str] = []
    clock = iter(range(0, 1000, 40))
    monkeypatch.setattr(warm.time, "monotonic", lambda: float(next(clock)))
    step = _warm(FakeKubectl(_job()), messages, timeout=60)
    step.start()
    step.finish()
    assert any("skipped" in m for m in messages), messages
    assert cluster.verbs[-1] == "delete"


def test_a_pod_that_cannot_be_created_is_reported_and_finish_has_nothing_to_do(monkeypatch: pytest.MonkeyPatch) -> None:
    cluster = Cluster(monkeypatch, ["Succeeded"])
    cluster.apply_fails = True
    messages: list[str] = []
    step = _warm(FakeKubectl(_job()), messages)
    step.start()
    step.finish()
    assert any("skipped" in m for m in messages), messages
    assert "get" not in cluster.verbs


def test_a_controller_job_that_cannot_be_read_skips_the_warm(monkeypatch: pytest.MonkeyPatch) -> None:
    cluster = Cluster(monkeypatch, ["Succeeded"])
    messages: list[str] = []
    step = _warm(FakeKubectl(None), messages)
    step.start()
    step.finish()
    assert any("skipped" in m for m in messages), messages
    assert "apply" not in cluster.verbs


def test_aborting_removes_the_pod_without_waiting(monkeypatch: pytest.MonkeyPatch) -> None:
    cluster = Cluster(monkeypatch, ["Running"])
    step = _warm(FakeKubectl(_job()), [])
    step.start()
    step.abort()
    assert cluster.verbs == ["delete", "apply", "delete"]
