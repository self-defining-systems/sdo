"""Warm a lane's Go build cache for the operator source tree a swap is about to run, while the old controller serves.

The controller compiles its accepted memory at every start (``builder.RunOnce``: the staged workspace, ``go mod tidy``,
the test compile of every package, the detector and generated gates, the controller binary, the memory host). That build
imports the operator packages from the projected source tree (the dev mount), so after a swap to a new tree their cache
entries do not exist and the first restart recompiles them: measured on a pool lane, a restart that took 19-21 s with a
warm cache took about 45 s on a new tree and 130 s with an empty cache. The cache lives on the controller's repository
volume and survives the restart.

``GoCacheWarm`` runs the new binary's ``warm-build`` (the same compiles, the executables discarded, the controller's own
arguments) in a short pod made from the controller Job's own pod template: the same image, Go environment, cache volume,
accepted repository, mounted settings and projected tree. The pod needs no credentials and no cluster API. A target tree
whose binary has no ``warm-build`` fails the pod: reported, and the restart compiles as before. Nothing here may fail a swap (the restart only
compiles what is missing, as before): every failure is reported to ``out`` and swallowed.
"""

from __future__ import annotations

import json
import subprocess
import time
from typing import TYPE_CHECKING, Any

from benchmarks.devswap.kube import CONTROLLER_CONTAINER, CONTROLLER_JOB, Kubectl, SwapError, run

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

WARM_POD = "sdo-warm-go-cache"
#: The warm of each lane that has one running, by ``lane_key``: ``controller.set_controller_suspended`` joins it before
#: any stop of that lane's controller, whichever step stops it.
pending: dict[tuple[str, str, str], GoCacheWarm] = {}
#: A bound, not a delay: the pod ends when the build does (measured 25-45 s on a new tree, 130 s on an empty cache).
WARM_TIMEOUT_S = 420.0
OPERATOR_BINARY = "/usr/local/bin/sdo-operator"
#: The controller's mounts the build needs: the repository volume (accepted memory, ``GOCACHE``, scratch directories),
#: scratch, the mounted settings (the build reads the replay policy) and every projected-tree mount (``devmount.Apply``
#: names them ``dev-*``).
_KEPT_MOUNTS = ("repository", "scratch", "sdo-config")
_KEPT_PREFIX = "dev-"


def lane_key(kube: Kubectl, control: str) -> tuple[str, str, str]:
    return (kube.kubeconfig, kube.context, control)


def join_pending(kube: Kubectl, control: str) -> None:
    """Wait (bounded) for the lane's running warm, if any: called before the controller is stopped."""
    step = pending.get(lane_key(kube, control))
    if step is not None:
        step.finish()


def warm_pod(job: Mapping[str, Any], control: str) -> dict[str, Any]:
    """The warm pod, from the controller Job object ``job``: its controller container's image, plain environment,
    resources and security context, with only the mounts the build needs, running ``warm-build`` with the controller's
    arguments. Raises ``SwapError`` when the Job has no controller container, arguments or repository volume."""
    spec = ((job.get("spec") or {}).get("template") or {}).get("spec") or {}
    container = next((c for c in spec.get("containers", []) if c.get("name") == CONTROLLER_CONTAINER), None)
    if container is None:
        raise SwapError(f"no controller container in job {CONTROLLER_JOB}")
    mounts = [
        m
        for m in container.get("volumeMounts", [])
        if m.get("name") in _KEPT_MOUNTS or str(m.get("name", "")).startswith(_KEPT_PREFIX)
    ]
    args = list(container.get("args") or [])
    if args[:1] != ["controller"]:
        raise SwapError(f"the {CONTROLLER_CONTAINER} container of job {CONTROLLER_JOB} does not run the controller")
    names = {m["name"] for m in mounts}
    volumes = [v for v in spec.get("volumes", []) if v.get("name") in names]
    if "repository" not in names or "repository" not in {v["name"] for v in volumes}:
        raise SwapError("the controller job has no repository volume")
    warm: dict[str, Any] = {
        "name": "warm",
        "image": container["image"],
        "imagePullPolicy": "IfNotPresent",
        "command": [OPERATOR_BINARY, "warm-build", *args],
        # plain values only: an entry that reads the pod's own identity is the controller's (leader election)
        "env": [e for e in container.get("env", []) if "value" in e],
        "resources": container.get("resources") or {},
        "securityContext": container.get("securityContext") or {},
        "volumeMounts": mounts,
    }
    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "securityContext": spec.get("securityContext") or {},
        "containers": [warm],
        "volumes": volumes,
    }
    for key in ("nodeSelector", "tolerations", "affinity"):
        if spec.get(key):
            pod_spec[key] = spec[key]
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": WARM_POD, "namespace": control}, "spec": pod_spec}


class GoCacheWarm:
    """One warm: ``start`` creates the pod and returns at once, ``finish`` waits for it (bounded), ``abort`` removes it.
    None of them raises."""

    def __init__(
        self, kube: Kubectl, control: str, out: Callable[[str], object] = print, timeout: float = WARM_TIMEOUT_S
    ) -> None:
        self._kube = kube
        self._control = control
        self._out = out
        self._timeout = timeout
        self._started = 0.0
        self._created = False
        #: Seconds ``finish`` waited for the pod (the part of the warm that was not overlapped with other swap steps).
        self.waited = 0.0

    def _delete(self, wait: bool) -> None:
        subprocess.run(
            self._kube.cmd(
                "-n", self._control, "delete", "pod", WARM_POD, "--ignore-not-found", f"--wait={str(wait).lower()}"
            ),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )

    def start(self) -> None:
        try:
            job = self._kube.get_json("-n", self._control, "get", "job", CONTROLLER_JOB)
            if job is None:
                raise SwapError(f"the controller job {CONTROLLER_JOB} cannot be read")
            pod = warm_pod(job, self._control)
            self._delete(wait=True)  # an earlier swap's pod (same name) must be gone first
            self._started = time.monotonic()
            self._created = True
            run(self._kube.cmd("apply", "-f", "-"), "create the Go cache warm pod", 60, input=json.dumps(pod))
            pending[lane_key(self._kube, self._control)] = self
        except (SwapError, OSError, subprocess.SubprocessError, ValueError, KeyError) as exc:
            self._created = False
            pending.pop(lane_key(self._kube, self._control), None)
            self._out(f"Go build cache warm skipped (the restart compiles what is missing): {exc}")

    def _phase(self) -> str:
        proc = subprocess.run(
            self._kube.cmd("-n", self._control, "get", "pod", WARM_POD, "-o", "jsonpath={.status.phase}"),
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        return proc.stdout.strip()

    def _logs(self) -> str:
        proc = subprocess.run(
            self._kube.cmd("-n", self._control, "logs", WARM_POD),
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        return (proc.stdout or proc.stderr).strip()

    def finish(self) -> None:
        if not self._created:
            return
        waiting = time.monotonic()
        try:
            while True:
                phase = self._phase()
                if phase == "Succeeded":
                    last = (self._logs().splitlines() or [""])[-1]
                    self._out(f"Go build cache warmed in {time.monotonic() - self._started:.0f}s: {last}")
                    return
                if phase == "Failed":
                    raise SwapError(f"the warm build failed: {self._logs()[-300:]}")
                if time.monotonic() - self._started > self._timeout:
                    raise SwapError(f"the warm build did not end within {self._timeout:.0f}s")
                time.sleep(2)
        except (SwapError, OSError, subprocess.SubprocessError) as exc:
            self._out(f"Go build cache warm skipped (the restart compiles what is missing): {exc}")
        finally:
            self.waited = time.monotonic() - waiting
            self.abort()

    def abort(self) -> None:
        """Remove the pod (a build still running is ended); a pod that is not there is not an error."""
        if not self._created:
            return
        self._created = False
        pending.pop(lane_key(self._kube, self._control), None)
        try:
            self._delete(wait=False)
        except (OSError, subprocess.SubprocessError) as exc:
            self._out(f"could not remove the Go cache warm pod: {exc}")


def start_go_cache_warm(kube: Kubectl, control: str, out: Callable[[str], object] = print) -> GoCacheWarm:
    step = GoCacheWarm(kube, control, out)
    step.start()
    return step
