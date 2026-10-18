"""Record the healthy application namespace for the opt-in detector healthy-baseline gate.

The commit broker (``--healthy-baseline-source``) replays every proposed incident detector on
snapshots of the healthy application and rejects one that fires on them. Nothing in the production runtime records those
snapshots, so the benchmark harness does: just before it injects a fault, while the application is
deployed and no fault exists, it dumps the namespace in the ``sdktest.Snapshot`` JSON shape and
writes the files onto the controller's repository volume, outside the application repository. This
module is harness-only; ``sdo/operator/runtime`` is untouched.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    import subprocess

    from benchmarks.sregym.adapter import ClusterOps

# Where the controller pod's broker reads the snapshots (the repository volume root, not the repository).
BASELINE_DIRECTORY = "/workspace/.sdo-baseline/healthy"
CONTROLLER_JOB = "job/sdo-controller-run"
DEFAULT_SNAPSHOT_COUNT = 3
DEFAULT_SNAPSHOT_INTERVAL_SECONDS = 2.0

# Kubernetes kind -> sdktest.Snapshot JSON key.
_SNAPSHOT_KEYS: dict[str, str] = {
    "ConfigMap": "configMaps",
    "Service": "services",
    "Pod": "pods",
    "Deployment": "deployments",
    "ReplicaSet": "replicaSets",
    "Endpoints": "endpoints",
    "EndpointSlice": "endpointSlices",
    "NetworkPolicy": "networkPolicies",
    "Event": "events",
}
_RESOURCES = "configmaps,services,pods,deployments,replicasets,endpoints,endpointslices,networkpolicies,events"
_FILE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*\.json$")


class HealthyBaselineCaptureError(RuntimeError):
    """The healthy baseline could not be captured or written; the gated run must not continue ungated."""


class KubectlRunner(Protocol):
    def __call__(
        self, args: list[str], *, namespace: str | None, input_text: str | None = None, check: bool = True
    ) -> subprocess.CompletedProcess[str]: ...


def snapshot_document(namespace: str, listing: dict[str, Any]) -> dict[str, Any]:
    """Convert a ``kubectl get ... -o json`` list into one ``sdktest.Snapshot`` document."""

    document: dict[str, Any] = {"namespace": namespace, **{key: [] for key in _SNAPSHOT_KEYS.values()}}
    for item in listing.get("items", []):
        key = _SNAPSHOT_KEYS.get(str(item.get("kind", "")))
        if key is None:
            continue
        metadata = item.get("metadata")
        if isinstance(metadata, dict):
            metadata.pop("managedFields", None)
        document[key].append(item)
    return document


def capture_snapshots(
    namespace: str,
    *,
    kubectl_runner: KubectlRunner,
    count: int = DEFAULT_SNAPSHOT_COUNT,
    interval_seconds: float = DEFAULT_SNAPSHOT_INTERVAL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, dict[str, Any]]:
    """Take ``count`` snapshots of ``namespace`` spaced ``interval_seconds`` apart, keyed by file name."""

    if count < 1:
        raise ValueError("healthy baseline snapshot count must be at least 1")
    snapshots: dict[str, dict[str, Any]] = {}
    for index in range(count):
        if index:
            sleep(interval_seconds)
        completed = kubectl_runner(["get", _RESOURCES, "-o", "json"], namespace=namespace, check=False)
        if completed.returncode != 0:
            details = completed.stderr.strip() or completed.stdout.strip()
            raise HealthyBaselineCaptureError(
                f"cannot list namespace {namespace!r} for the healthy baseline: {details}"
            )
        try:
            listing = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise HealthyBaselineCaptureError(
                f"kubectl returned invalid JSON for namespace {namespace!r}: {exc}"
            ) from exc
        snapshots[f"healthy-{index}.json"] = snapshot_document(namespace, listing)
    return snapshots


def publish_snapshots(
    snapshots: dict[str, dict[str, Any]],
    *,
    control_namespace: str,
    kubectl_runner: KubectlRunner,
    directory: str = BASELINE_DIRECTORY,
) -> None:
    """Replace the snapshot files on the controller's repository volume."""

    if directory != BASELINE_DIRECTORY:
        raise ValueError("Go baseline writer uses the fixed healthy baseline directory")
    for name in snapshots:
        if not _FILE_NAME.fullmatch(name):
            raise ValueError(f"unsafe healthy baseline file name: {name!r}")
    completed = kubectl_runner(
        ["exec", "-i", CONTROLLER_JOB, "-c", "controller", "--", "/usr/local/bin/sdo-operator", "baseline-write"],
        namespace=control_namespace,
        input_text=json.dumps(snapshots),
        check=False,
    )
    if completed.returncode != 0:
        details = completed.stderr.strip() or completed.stdout.strip()
        raise HealthyBaselineCaptureError(f"cannot write the healthy baseline into the controller pod: {details}")


Capture = Callable[[], dict[str, dict[str, Any]]]
Publish = Callable[[dict[str, dict[str, Any]], str], None]


class HealthyBaselineCaptureOps:
    """A :class:`ClusterOps` that records the healthy namespace right before the fault is injected.

    It wraps the ``inject`` callback, so it composes with the default gate (the controller has just
    reported an all-clear evaluation) and with ``InjectBeforeResumeOps`` (the controller is paused,
    the application is deployed, no fault has landed). A failed capture aborts before injection.
    """

    def __init__(self, inner: ClusterOps, *, namespace: str, capture: Capture, publish: Publish) -> None:
        self._inner = inner
        self._namespace = namespace
        self._capture = capture
        self._publish = publish

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]:
        capture_seconds = 0.0

        def capture_then_inject() -> None:
            nonlocal capture_seconds
            started = time.monotonic()
            self._publish(self._capture(), control_namespace)
            capture_seconds = time.monotonic() - started
            inject()

        timings = dict(self._inner.inject_after_resume(control_namespace, generation, capture_then_inject))
        timings["healthy_baseline_capture"] = capture_seconds
        return timings


def kubectl_capture_ops(
    inner: ClusterOps, *, namespace: str, kubectl_runner: KubectlRunner
) -> HealthyBaselineCaptureOps:
    """Production wiring: capture with kubectl, publish into the controller pod."""

    return HealthyBaselineCaptureOps(
        inner,
        namespace=namespace,
        capture=lambda: capture_snapshots(namespace, kubectl_runner=kubectl_runner),
        publish=lambda snapshots, control: publish_snapshots(
            snapshots, control_namespace=control, kubectl_runner=kubectl_runner
        ),
    )
