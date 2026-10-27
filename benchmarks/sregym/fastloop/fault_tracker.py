"""Per-fault resolution tracking for composite incidents.

A composite injects several independent faults at once. The conductor's mitigation
oracle answers only "all fixed"; to say which fault was fixed when, a read-only
probe per fault is polled while the agent works. Probes read Kubernetes objects
and fixed runtime observations (no helper pods or repair commands). The problem's
own oracle still grades the end state.

Resolution of a fault is the start of its final unbroken green run, provided that run
lasted at least ``stable_seconds``: a fault that is fixed and then broken again by a later
change is not counted as resolved early.
"""

from __future__ import annotations

import copy
import json
import subprocess
import threading
import time
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.object_probes import (
    admitted_memory_probe,
    cronjob_recovered,
    finalizer_recovered,
    hpa_recovered,
    shared_memory_recovered,
)
from benchmarks.sregym.fastloop.object_probes import (
    deployment_available as _deployment_available,
)
from benchmarks.sregym.fastloop.object_probes import (
    flag_disabled as _flag_disabled,
)
from benchmarks.sregym.fastloop.runtime_observation import KubectlObservationTransport, ReadOnlyObservation
from benchmarks.sregym.fastloop.runtime_probes import Observer, runtime_probe
from benchmarks.sregym.faults.description import FaultSpec, ProblemDescription, describe_sregym_problems
from benchmarks.sregym.faults.probe_catalog import (
    AVAILABILITY_KINDS,
    BASELINE_FIELDS,
    FEATURE_FLAGS,
    FLAG_FIELDS,
    RUNTIME_PROBE_KINDS,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

NETWORK_POLICY_LABEL_KEY = "io.kompose.service"


def composite_faults(problem_id: str) -> tuple[FaultSpec, ...]:
    """Compatibility helper; resolve registered composites rather than maintaining a second catalog."""
    description = _default_description(problem_id)
    return description.ordered_faults() if description.composite else ()


@lru_cache(maxsize=256)
def _default_description(problem_id: str) -> ProblemDescription:
    return describe_sregym_problems(
        (problem_id,), sregym_dir=Path(__file__).resolve().parents[3] / "third_party/sregym"
    )[problem_id]


def describe_sregym_faults(
    problem_ids: tuple[str, ...], *, sregym_dir: Path, app_source_dir: Path | None = None
) -> dict[str, tuple[FaultSpec, ...]]:
    return {
        key: value.ordered_faults()
        for key, value in describe_sregym_problems(
            problem_ids, sregym_dir=sregym_dir, app_source_dir=app_source_dir
        ).items()
    }


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


def _field_value(value: Any, path: tuple[str, ...]) -> Any:
    if not path:
        return copy.deepcopy(value)
    head, *tail = path
    if head == "*":
        if not isinstance(value, list) or any(not isinstance(item, dict) or not item.get("name") for item in value):
            raise ValueError("probe requires a named object list")
        names = [item["name"] for item in value]
        if len(set(names)) != len(names):
            raise ValueError("probe requires distinct list names")
        return {item["name"]: _field_value(item, tuple(tail)) for item in value}
    if not isinstance(value, dict) or head not in value:
        return ("absent",)
    return _field_value(value[head], tuple(tail))


def _restoration_probe(
    read: Callable[[str, str], dict[str, Any] | None], component: str, fields: tuple[str, ...]
) -> Callable[[], bool]:
    # Called before inject(), not at first poll (which happens after injection).
    baseline = read("deployment", component)
    if not baseline or not _deployment_available(lambda _kind, _name: baseline, component):
        raise ValueError(f"probe needs an available pre-injection baseline for {component}")
    paths = tuple(tuple(field.split(".")) for field in fields)
    expected = tuple(_field_value(baseline, path) for path in paths)

    def restored() -> bool:
        current = read("deployment", component)
        return bool(current) and (
            tuple(_field_value(current, path) for path in paths) == expected
            and _deployment_available(lambda _kind, _name: current, component)
        )

    return restored


def _dns_egress_open(read: Callable[[str, str], dict[str, Any] | None], component: str) -> bool:
    policy = read("networkpolicy", f"deny-dns-egress-{component}")
    if policy is None:
        return True
    spec = policy.get("spec") or {}
    if "Egress" not in spec.get("policyTypes", ["Ingress", "Egress"] if "egress" in spec else ["Ingress"]):
        return True
    protocols: set[str] = set()
    for rule in spec.get("egress") or []:
        if rule.get("to"):
            continue  # Cannot prove those destinations contain the DNS resolver.
        if not rule.get("ports"):
            return True
        for port in rule["ports"]:
            if port.get("port") in (None, 53):
                protocols.add(port.get("protocol", "TCP"))
    return {"TCP", "UDP"} <= protocols


def _ingress_routes_to_target(read: Callable[[str, str], dict[str, Any] | None], fault: FaultSpec) -> bool:
    obj = read("ingress", fault.parameters["ingress_name"])
    if not obj:
        return False
    routes = [
        route
        for rule in (obj.get("spec") or {}).get("rules") or []
        for route in (rule.get("http") or {}).get("paths") or []
        if str(route.get("path", "")).startswith(fault.parameters["path"])
    ]
    return bool(routes) and all(
        ((route.get("backend") or {}).get("service") or {}).get("name") == fault.parameters["correct_service"]
        for route in routes
    )


def _probe_for(
    fault: FaultSpec, read: Callable[[str, str], dict[str, Any] | None], component: str
) -> Callable[[], bool]:
    if fault.kind == "hpa_missing_effective_cpu_request":
        return lambda: hpa_recovered(read, fault, component)
    if fault.kind == "dev_shm_exhaustion_hotel_reservation":
        return lambda: shared_memory_recovered(read, fault, component)
    if fault.kind == "finalizer_deadlock_controller":
        return lambda: finalizer_recovered(read, fault, component)
    if fault.kind == "cronjob_sidecar_blocks_completion_hotel_reservation":
        return lambda: cronjob_recovered(read, fault)
    if fault.kind == "mutating_webhook_resource_limits_social_network":
        return admitted_memory_probe(read, component)
    if fault.kind in FLAG_FIELDS:
        flag, fields = FLAG_FIELDS[fault.kind]
        restored = _restoration_probe(read, component, fields)
        return lambda: _flag_disabled(read, flag) and restored()
    if fault.kind == "service_dns_resolution_failure_netpol":
        return lambda: _dns_egress_open(read, component)
    if fault.kind == "ingress_misroute":
        return lambda: _ingress_routes_to_target(read, fault)
    if fault.kind == "internal_traffic_policy_local_astronomy_shop":

        def cluster_routing() -> bool:
            service = read("service", component)
            return (
                bool(service)
                and (service.get("spec") or {}).get("internalTrafficPolicy", "Cluster") == "Cluster"
                and _service_has_endpoints(read, component)
            )

        return cluster_routing
    if fault.kind in BASELINE_FIELDS:
        return _restoration_probe(read, component, BASELINE_FIELDS[fault.kind])
    if fault.kind in FEATURE_FLAGS:
        return lambda: _flag_disabled(read, FEATURE_FLAGS[fault.kind])
    if fault.kind in {"readiness", "configmap", "resource_request", "incorrect_image"} | AVAILABILITY_KINDS:
        return lambda: _deployment_available(read, component)
    if fault.kind == "target_port":
        return lambda: _target_port_matches(read, component)
    if fault.kind == "network_policy":
        name = fault.policy_name or f"deny-all-{component}"
        return lambda: (
            _network_policy_open(read, component, name, fault.pod_label_key) and _deployment_available(read, component)
        )
    if fault.kind == "wrong_selector":
        return lambda: _service_has_endpoints(read, component)
    if fault.kind == "missing_service":
        return lambda: bool(read("service", component)) and _service_has_endpoints(read, component)
    raise ValueError(f"no probe for fault kind {fault.kind!r}")


def probes_for(
    faults: Sequence[FaultSpec], read: Callable[[str, str], dict[str, Any] | None], *, observe: Observer | None = None
) -> dict[str, Callable[[], bool]]:
    probes: dict[str, Callable[[], bool]] = {}
    for fault in faults:
        if not fault.tracking_supported:
            raise ValueError(f"per-fault tracking unsupported: {fault.tracking_reason}")
        if fault.name in probes:
            raise ValueError(f"duplicate probe identity {fault.name}")
        if fault.kind in RUNTIME_PROBE_KINDS:
            observer = observe or getattr(read, "observe", None)
            if observer is None:
                raise ValueError("runtime fault tracking requires the read-only observation transport")
            probes[fault.name] = runtime_probe(fault, read, observer)
            continue
        components = (fault.component,) if fault.kind == "ingress_misroute" else fault.target_components
        target_probes = tuple(_probe_for(fault, read, component) for component in components)
        probes[fault.name] = lambda target_probes=target_probes: all(probe() for probe in target_probes)
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
        stable_seconds: float = wait_for(WaitKind.STABLE),
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

    def __init__(
        self, *, namespace: str, kubeconfig: Path | str | None = None, timeout: float = wait_for(WaitKind.COMMAND)
    ) -> None:
        self._namespace = namespace
        self._kubeconfig = str(kubeconfig) if kubeconfig else None
        self._timeout = timeout
        self._observations = KubectlObservationTransport(namespace=namespace, kubeconfig=kubeconfig)

    def observe(self, request: ReadOnlyObservation) -> str:
        return self._observations(request)

    def __call__(self, kind: str, name: str) -> dict[str, Any] | None:
        command = ["kubectl"]
        if self._kubeconfig:
            command += ["--kubeconfig", self._kubeconfig]
        if kind not in {
            "deployment",
            "service",
            "endpoints",
            "networkpolicy",
            "configmap",
            "ingress",
            "hpa",
            "cronjob",
            "jobs",
            "pods",
            "clusterrole",
            "clusterrolebinding",
            "serviceaccount",
        }:
            raise ValueError(f"resource {kind!r} is outside the read-only fault probe inventory")
        if not name and kind not in {"jobs", "pods"}:
            raise ValueError("only Pod and Job inventories may be listed")
        command += ["-n", self._namespace, "get", kind, *([name] if name else []), "-o", "json", "--ignore-not-found"]
        completed = subprocess.run(command, capture_output=True, text=True, timeout=self._timeout, check=False)
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.strip() or f"kubectl exited {completed.returncode}")
        if not completed.stdout.strip():
            return None
        document = json.loads(completed.stdout)
        return document if isinstance(document, dict) else None


class BackgroundPoller:
    """Polls a tracker on a thread until stopped."""

    def __init__(self, tracker: FaultTracker, *, interval_seconds: float = wait_for(WaitKind.POLL)) -> None:
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
        self._thread.join(timeout=wait_for(WaitKind.COMMAND))
        self._tracker.poll()
