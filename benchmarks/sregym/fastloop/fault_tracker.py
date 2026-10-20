"""Per-fault resolution tracking for composite incidents.

A composite injects several independent faults at once. The conductor's mitigation
oracle answers only "all fixed"; to say which fault was fixed when, a read-only
probe per fault is polled while the agent works. Probes only read Kubernetes objects
(no helper pods), so they never disturb the agent or the controller. The problem's own
oracle still grades the end state.

Resolution of a fault is the start of its final unbroken green run, provided that run
lasted at least ``stable_seconds``: a fault that is fixed and then broken again by a later
change is not counted as resolved early.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

NETWORK_POLICY_LABEL_KEY = "io.kompose.service"


@dataclass(frozen=True)
class FaultSpec:
    kind: str
    component: str
    policy_name: str | None = None
    pod_label_key: str = NETWORK_POLICY_LABEL_KEY

    @property
    def name(self) -> str:
        return f"{self.kind}:{self.component}"


#: Hand-registered composites (third_party/sregym ``COMPOSITE_SPECS``), in injection order.
COMPOSITE_FAULTS: dict[str, tuple[FaultSpec, ...]] = {
    "composite3_hotel_geo_rate_recommendation": (
        FaultSpec("readiness", "geo"),
        FaultSpec("configmap", "mongodb-rate"),
        FaultSpec("network_policy", "recommendation"),
    ),
    "composite3b_hotel_profile_mongodb_geo_recommendation": (
        FaultSpec("readiness", "profile"),
        FaultSpec("configmap", "mongodb-geo"),
        FaultSpec("network_policy", "recommendation"),
    ),
    "composite3c_hotel_rate_mongodb_geo_user": (
        FaultSpec("readiness", "rate"),
        FaultSpec("configmap", "mongodb-geo"),
        FaultSpec("network_policy", "user"),
    ),
    "composite4_hotel_profile_rate_recommendation_frontend": (
        FaultSpec("readiness", "profile"),
        FaultSpec("configmap", "mongodb-rate"),
        FaultSpec("network_policy", "recommendation"),
        FaultSpec("wrong_selector", "frontend"),
    ),
    "composite5_hotel_geo_rate_recommendation_frontend_user": (
        FaultSpec("readiness", "geo"),
        FaultSpec("configmap", "mongodb-rate"),
        FaultSpec("network_policy", "recommendation"),
        FaultSpec("wrong_selector", "frontend"),
        FaultSpec("resource_request", "user"),
    ),
}


def composite_faults(problem_id: str) -> tuple[FaultSpec, ...]:
    return COMPOSITE_FAULTS.get(problem_id, ())


def describe_sregym_faults(problem_ids: tuple[str, ...], *, sregym_dir: Path) -> dict[str, tuple[FaultSpec, ...]]:
    """Resolve probe targets from SREGym's registered problem instances without cluster calls."""

    if not problem_ids:
        return {}
    root = Path(__file__).resolve().parents[3]
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(filter(None, (str(root), os.environ.get("PYTHONPATH", "")))),
    }
    completed = subprocess.run(
        ["uv", "run", "--directory", str(sregym_dir), "python", "-m", "benchmarks.sregym.fastloop.fault_spec_worker"],
        input=json.dumps(problem_ids),
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )
    if completed.returncode != 0:
        raise ValueError(f"SREGym fault description failed: {(completed.stderr or completed.stdout)[-2000:]}")
    described = json.loads(completed.stdout)
    if set(described) != set(problem_ids):
        raise ValueError("SREGym returned incomplete fault descriptions")
    return {problem_id: tuple(FaultSpec(**item) for item in items) for problem_id, items in described.items()}


def stream_faults(
    problem_ids: tuple[str, ...], *, catalog: Mapping[str, tuple[FaultSpec, ...]] | None = None
) -> tuple[FaultSpec, ...]:
    """Build probes in the worker's injection order from resolved SREGym problems."""

    catalog = (
        catalog
        if catalog is not None
        else describe_sregym_faults(
            problem_ids, sregym_dir=Path(__file__).resolve().parents[3] / "third_party" / "sregym"
        )
    )
    faults: list[FaultSpec] = []
    for problem_id in problem_ids:
        resolved = catalog.get(problem_id)
        if not resolved:
            raise ValueError(f"no fastloop probe for {problem_id!r}")
        faults.extend(resolved)
    return tuple(faults)


def _deployment_available(read: Callable[[str, str], dict[str, Any] | None], name: str) -> bool:
    deployment = read("deployment", name)
    if not deployment:
        return False
    spec = deployment.get("spec") or {}
    status = deployment.get("status") or {}
    desired = spec.get("replicas")
    desired = 1 if desired is None else int(desired)
    generation = (deployment.get("metadata") or {}).get("generation") or 0
    return (
        desired >= 1
        and int(status.get("observedGeneration") or 0) >= int(generation)
        and int(status.get("readyReplicas") or 0) == desired
        and int(status.get("updatedReplicas") or 0) == desired
        and int(status.get("replicas") or 0) == desired
    )


def _network_policy_open(
    read: Callable[[str, str], dict[str, Any] | None], component: str, name: str, label_key: str
) -> bool:
    policy = read("networkpolicy", name)
    if not policy:
        return True
    spec = policy.get("spec") or {}
    selector = ((spec.get("podSelector") or {}).get("matchLabels")) or {}
    targets_component = selector.get(label_key) == component
    still_denies = not spec.get("ingress") and not spec.get("egress")
    return not (targets_component and still_denies)


def _service_has_endpoints(read: Callable[[str, str], dict[str, Any] | None], name: str) -> bool:
    endpoints = read("endpoints", name)
    if not endpoints:
        return False
    return any(subset.get("addresses") for subset in endpoints.get("subsets") or [])


def _target_port_matches(read: Callable[[str, str], dict[str, Any] | None], component: str) -> bool:
    service = read("service", component)
    deployment = read("deployment", component)
    if not service or not deployment:
        return False
    ports = [
        port
        for container in (((deployment.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers")
        or []
        for port in container.get("ports") or []
    ]
    targets = {port.get("containerPort") for port in ports} | {port.get("name") for port in ports}
    return all(
        port.get("targetPort", port.get("port")) in targets for port in (service.get("spec") or {}).get("ports") or []
    )


def probes_for(
    faults: Sequence[FaultSpec], read: Callable[[str, str], dict[str, Any] | None]
) -> dict[str, Callable[[], bool]]:
    probes: dict[str, Callable[[], bool]] = {}
    for fault in faults:
        component = fault.component
        if fault.kind in {"readiness", "configmap", "resource_request", "incorrect_image"}:
            probes[fault.name] = lambda component=component: _deployment_available(read, component)
        elif fault.kind == "target_port":
            probes[fault.name] = lambda component=component: _target_port_matches(read, component)
        elif fault.kind == "network_policy":
            name = fault.policy_name or f"deny-all-{component}"
            label_key = fault.pod_label_key
            probes[fault.name] = lambda component=component, name=name, label_key=label_key: (
                _network_policy_open(read, component, name, label_key) and (_deployment_available(read, component))
            )
        elif fault.kind == "wrong_selector":
            probes[fault.name] = lambda component=component: _service_has_endpoints(read, component)
        else:
            raise ValueError(f"no probe for fault kind {fault.kind!r}")
    return probes


@dataclass(frozen=True)
class Sample:
    t: float
    states: dict[str, bool]
    errors: dict[str, str] = field(default_factory=dict)


class FaultTracker:
    def __init__(
        self,
        probes: Mapping[str, Callable[[], bool]],
        *,
        stable_seconds: float = 20.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not probes:
            raise ValueError("at least one probe is required")
        if stable_seconds < 0:
            raise ValueError("stable_seconds must not be negative")
        self._probes = dict(probes)
        self._stable = stable_seconds
        self._clock = clock
        self._origin = clock()
        self._lock = threading.Lock()
        self.samples: list[Sample] = []

    def restart(self) -> None:
        """Make now time zero (the end of injection) and drop earlier samples."""

        with self._lock:
            self._origin = self._clock()
            self.samples = []

    def poll(self) -> Sample:
        states: dict[str, bool] = {}
        errors: dict[str, str] = {}
        for name, probe in self._probes.items():
            try:
                states[name] = bool(probe())
            except Exception as exc:  # a probe that cannot read counts as unresolved
                states[name] = False
                errors[name] = f"{type(exc).__name__}: {exc}"
        sample = Sample(t=self._clock() - self._origin, states=states, errors=errors)
        with self._lock:
            self.samples.append(sample)
        return sample

    def all_green_now(self) -> bool:
        with self._lock:
            return bool(self.samples) and all(self.samples[-1].states.values())

    def ever_red(self) -> dict[str, bool]:
        """False for a fault whose probe was never red: the injection had no effect on the live app."""

        with self._lock:
            samples = list(self.samples)
        return {name: any(not sample.states[name] for sample in samples) for name in self._probes}

    def first_green(self) -> dict[str, float | None]:
        with self._lock:
            samples = list(self.samples)
        return {name: next((sample.t for sample in samples if sample.states[name]), None) for name in self._probes}

    def resolution_times(self) -> dict[str, float | None]:
        with self._lock:
            samples = list(self.samples)
        times: dict[str, float | None] = {}
        for name in self._probes:
            start: float | None = None
            for sample in samples:
                if sample.states[name]:
                    start = sample.t if start is None else start
                else:
                    start = None
            last = samples[-1].t if samples else 0.0
            times[name] = start if start is not None and last - start >= self._stable else None
        return times

    def all_resolved_at(self) -> float | None:
        times = self.resolution_times().values()
        return None if any(value is None for value in times) else max(times, default=None)  # type: ignore[type-var]

    def timeline(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                {
                    "t": round(sample.t, 1),
                    "states": sample.states,
                    **({"errors": sample.errors} if sample.errors else {}),
                }
                for sample in self.samples
            ]


class KubectlReader:
    """Reads one namespaced object as JSON; ``None`` when it does not exist."""

    def __init__(self, *, namespace: str, kubeconfig: Path | str | None = None, timeout: float = 20.0) -> None:
        self._namespace = namespace
        self._kubeconfig = str(kubeconfig) if kubeconfig else None
        self._timeout = timeout

    def __call__(self, kind: str, name: str) -> dict[str, Any] | None:
        command = ["kubectl"]
        if self._kubeconfig:
            command += ["--kubeconfig", self._kubeconfig]
        command += ["-n", self._namespace, "get", kind, name, "-o", "json", "--ignore-not-found"]
        completed = subprocess.run(command, capture_output=True, text=True, timeout=self._timeout, check=False)
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.strip() or f"kubectl exited {completed.returncode}")
        if not completed.stdout.strip():
            return None
        document = json.loads(completed.stdout)
        return document if isinstance(document, dict) else None


class BackgroundPoller:
    """Polls a tracker on a thread until stopped."""

    def __init__(self, tracker: FaultTracker, *, interval_seconds: float = 5.0) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self._tracker = tracker
        self._interval = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="fault-tracker", daemon=True)

    def _run(self) -> None:
        while not self._stop.is_set():
            self._tracker.poll()
            self._stop.wait(self._interval)

    def __enter__(self) -> BackgroundPoller:
        self._tracker.restart()
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=30)
        self._tracker.poll()
