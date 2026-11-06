"""Application-agnostic Kubernetes faults chosen from a workload inventory.

SREGym's injectors are keyed to problem classes of its own applications (fixed label scheme, recovery that hard-codes
the original value, delete-and-reapply that fights a Helm release). These faults keep the same failure classes but are
parameterised by the workloads of whatever app a lane runs: the inventory (Deployments, StatefulSets, Services of the
namespace, optionally filtered by the deployment plan's workload IDs) decides which faults are eligible where. Every
fault is pure data (``Action`` lists for ``kubectl``), including the harness revert used only when SDO does not
recover the app, so selection and the state machine that drives them are testable with a fake cluster.

Nothing here names an application. Nothing is imported from production SDO.
"""

from __future__ import annotations

import copy
import json
import random
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Mapping, Sequence

WORKLOAD_KINDS = ("Deployment", "StatefulSet")
#: Label every injected object carries so ``clean_start.benchmark_owned`` recognises it.
FAULT_LABEL = {"app.kubernetes.io/managed-by": "benchmark-fault"}
FAULT_SELECTOR_KEY = "benchmark.fault"
MISSING_SUFFIX = "-fault-missing"
BAD_TAG = "fault-nonexistent-tag"
OOM_MEMORY = "8Mi"
BLACKHOLE_DNS = "192.0.2.1"  # RFC 5737 TEST-NET-1: never routable


@dataclass(frozen=True)
class Action:
    """One ``kubectl`` call in the application namespace; ``stdin`` carries a manifest for ``apply -f -``."""

    args: tuple[str, ...]
    stdin: str | None = None


@dataclass(frozen=True)
class Workload:
    """A workload and the facts eligibility needs, read from the live (or rendered) inventory."""

    kind: str
    namespace: str
    name: str
    replicas: int
    pod_labels: Mapping[str, str]
    pod_spec: Mapping[str, Any]
    services: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.kind not in WORKLOAD_KINDS:
            raise ValueError(f"unsupported workload kind {self.kind!r}")
        if not self.name or not self.namespace:
            raise ValueError("a workload needs a namespace and a name")
        if not self.pod_spec.get("containers"):
            raise ValueError(f"{self.id} has no containers")

    @property
    def id(self) -> str:
        return f"{self.kind}/{self.namespace}/{self.name}"

    @property
    def ref(self) -> str:
        return f"{self.kind.lower()}/{self.name}"

    @property
    def containers(self) -> list[dict[str, Any]]:
        return list(self.pod_spec["containers"])

    @property
    def pvc_volumes(self) -> list[dict[str, Any]]:
        return [v for v in self.pod_spec.get("volumes") or [] if "persistentVolumeClaim" in v]


def _selects(selector: Mapping[str, str], labels: Mapping[str, str]) -> bool:
    return bool(selector) and all(labels.get(key) == value for key, value in selector.items())


def parse_inventory(listing: Mapping[str, Any], *, plan_ids: Collection[str] | None = None) -> list[Workload]:
    """Workloads of ``kubectl get deployments,statefulsets,services -o json``, sorted by ID.

    ``plan_ids`` (``Kind/namespace/name``, the deployment plan's expected workloads) restricts the result to what the
    plan deploys; without it every top-level (not owner-managed) workload counts. Services are attached to the
    workloads whose pod labels their selector matches.
    """
    items = listing.get("items", [])
    services = [i for i in items if i.get("kind") == "Service"]
    workloads: list[Workload] = []
    for item in items:
        kind = item.get("kind")
        if kind not in WORKLOAD_KINDS:
            continue
        meta = item["metadata"]
        if plan_ids is None and meta.get("ownerReferences"):
            continue
        template = item["spec"]["template"]
        labels = dict((template.get("metadata") or {}).get("labels") or {})
        workload = Workload(
            kind=kind,
            namespace=meta["namespace"],
            name=meta["name"],
            replicas=int(item["spec"].get("replicas", 1)),
            pod_labels=labels,
            pod_spec=template["spec"],
            services=tuple(
                sorted(
                    s["metadata"]["name"]
                    for s in services
                    if _selects((s.get("spec") or {}).get("selector") or {}, labels)
                )
            ),
        )
        if plan_ids is None or workload.id in plan_ids:
            workloads.append(workload)
    return sorted(workloads, key=lambda w: w.id)


class FaultKind(str, Enum):
    POD_KILL = "pod_kill"
    CRASH_LOOP = "crash_loop"
    SCALE_ZERO = "scale_zero"
    BAD_ENV = "bad_env"
    BAD_IMAGE = "bad_image"
    MEMORY_LIMIT = "memory_limit"
    NETWORK_DENY = "network_deny"
    SERVICE_SELECTOR = "service_selector"
    PVC_MISMATCH = "pvc_mismatch"
    DNS_BREAK = "dns_break"


@dataclass(frozen=True)
class FaultSpec:
    """A chosen fault: what to apply, what restores it, and what the harness may expect."""

    kind: FaultKind
    workload: Workload
    inject: tuple[Action, ...]
    revert: tuple[Action, ...]
    #: The platform restores the app on its own (a killed pod is recreated); no detection is then not a failure.
    self_heals: bool = False
    detail: str = ""

    @property
    def problem_id(self) -> str:
        return f"generic_{self.kind.value}_{self.workload.name}"

    @property
    def identity(self) -> dict[str, Any]:
        return {"kind": self.kind.value, "workload": self.workload.id, "detail": self.detail}


def _patch(workload: Workload, body: Mapping[str, Any]) -> Action:
    return Action(("patch", workload.ref, "--type=strategic", "-p", json.dumps(body, sort_keys=True)))


def _pod_patch(workload: Workload, pod_spec: Mapping[str, Any]) -> Action:
    return _patch(workload, {"spec": {"template": {"spec": dict(pod_spec)}}})


def _container_patch(workload: Workload, container: Mapping[str, Any]) -> Action:
    return _pod_patch(workload, {"containers": [dict(container)]})


def _first_container(workload: Workload) -> dict[str, Any]:
    return workload.containers[0]


def _selector_arg(workload: Workload) -> str:
    return ",".join(f"{k}={v}" for k, v in sorted(workload.pod_labels.items()))


def _restore(original: Any) -> Any:
    """A strategic-merge value that restores ``original``; ``None`` deletes a field the original lacked."""
    return copy.deepcopy(original)


def _eligible_labels(workload: Workload) -> bool:
    return bool(workload.pod_labels)


def build(kind: FaultKind, workload: Workload) -> FaultSpec | None:
    """The fault of ``kind`` on ``workload``, or ``None`` when the workload does not support it."""
    first = _first_container(workload)
    cname = first["name"]
    if kind is FaultKind.POD_KILL:
        if not _eligible_labels(workload) or workload.replicas < 1:
            return None
        delete = Action(("delete", "pod", "-l", _selector_arg(workload), "--wait=false"))
        return FaultSpec(kind, workload, (delete,), (), self_heals=True)
    if kind is FaultKind.CRASH_LOOP:
        original = {"command": _restore(first.get("command")), "args": _restore(first.get("args"))}
        return FaultSpec(
            kind,
            workload,
            (_container_patch(workload, {"name": cname, "command": ["/bin/false"], "args": None}),),
            (_container_patch(workload, {"name": cname, **original}),),
            detail=cname,
        )
    if kind is FaultKind.SCALE_ZERO:
        if workload.replicas < 1:
            return None
        return FaultSpec(
            kind,
            workload,
            (Action(("scale", workload.ref, "--replicas=0")),),
            (Action(("scale", workload.ref, f"--replicas={workload.replicas}")),),
        )
    if kind is FaultKind.BAD_ENV:
        var = "FAULT_MISSING_CONFIG"
        ref = {"configMapKeyRef": {"name": workload.name + MISSING_SUFFIX, "key": "value"}}
        return FaultSpec(
            kind,
            workload,
            (_container_patch(workload, {"name": cname, "env": [{"name": var, "valueFrom": ref}]}),),
            (_container_patch(workload, {"name": cname, "env": [{"name": var, "$patch": "delete"}]}),),
            detail=f"{cname}:{var}",
        )
    if kind is FaultKind.BAD_IMAGE:
        image = str(first["image"])
        digestless = image.split("@", 1)[0]
        repository = digestless.rsplit(":", 1)[0] if ":" in digestless.rsplit("/", 1)[-1] else digestless
        return FaultSpec(
            kind,
            workload,
            (_container_patch(workload, {"name": cname, "image": f"{repository}:{BAD_TAG}"}),),
            (_container_patch(workload, {"name": cname, "image": image}),),
            detail=cname,
        )
    if kind is FaultKind.MEMORY_LIMIT:
        resources = first.get("resources") or {}
        tiny = {"limits": {"memory": OOM_MEMORY}, "requests": {"memory": OOM_MEMORY}}
        original = {
            "limits": _restore(resources.get("limits")) if resources.get("limits") else None,
            "requests": _restore(resources.get("requests")) if resources.get("requests") else None,
        }
        return FaultSpec(
            kind,
            workload,
            (_container_patch(workload, {"name": cname, "resources": tiny}),),
            (_container_patch(workload, {"name": cname, "resources": original}),),
            detail=f"{cname}:{OOM_MEMORY}",
        )
    if kind is FaultKind.NETWORK_DENY:
        if not workload.services or not _eligible_labels(workload):
            return None
        name = f"fault-deny-{workload.name}"[:63]
        policy = {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": name, "namespace": workload.namespace, "labels": dict(FAULT_LABEL)},
            "spec": {"podSelector": {"matchLabels": dict(workload.pod_labels)}, "policyTypes": ["Ingress"]},
        }
        return FaultSpec(
            kind,
            workload,
            (Action(("apply", "-f", "-"), json.dumps(policy, sort_keys=True)),),
            (Action(("delete", "networkpolicy", name, "--ignore-not-found=true")),),
            detail=name,
        )
    if kind is FaultKind.SERVICE_SELECTOR:
        if not workload.services:
            return None
        service = workload.services[0]
        inject = ("patch", "service", service, "--type=merge", "-p")
        return FaultSpec(
            kind,
            workload,
            (Action((*inject, json.dumps({"spec": {"selector": {FAULT_SELECTOR_KEY: "broken"}}}))),),
            (Action((*inject, json.dumps({"spec": {"selector": {FAULT_SELECTOR_KEY: None}}}))),),
            detail=service,
        )
    if kind is FaultKind.PVC_MISMATCH:
        if workload.kind != "Deployment" or not workload.pvc_volumes:
            return None  # a StatefulSet's claims are templates; deleting its data is not a recoverable fault
        volume = workload.pvc_volumes[0]
        claim = volume["persistentVolumeClaim"]["claimName"]

        def claim_patch(value: str) -> Action:
            return _pod_patch(
                workload, {"volumes": [{"name": volume["name"], "persistentVolumeClaim": {"claimName": value}}]}
            )

        return FaultSpec(
            kind,
            workload,
            (claim_patch(claim + MISSING_SUFFIX),),
            (claim_patch(claim),),
            detail=f"{volume['name']}:{claim}",
        )
    if kind is FaultKind.DNS_BREAK:
        original = {
            "dnsPolicy": workload.pod_spec.get("dnsPolicy"),
            "dnsConfig": _restore(workload.pod_spec.get("dnsConfig")),
        }
        broken = {"dnsPolicy": "None", "dnsConfig": {"nameservers": [BLACKHOLE_DNS]}}
        return FaultSpec(
            kind,
            workload,
            (_pod_patch(workload, broken),),
            (
                _pod_patch(
                    workload, {"dnsPolicy": original["dnsPolicy"] or "ClusterFirst", "dnsConfig": original["dnsConfig"]}
                ),
            ),
        )
    raise ValueError(f"unknown fault kind {kind!r}")


#: Kinds in the order a stream cycles through them: severe availability faults first, silent ones last.
DEFAULT_KINDS: tuple[FaultKind, ...] = (
    FaultKind.POD_KILL,
    FaultKind.SCALE_ZERO,
    FaultKind.BAD_IMAGE,
    FaultKind.CRASH_LOOP,
    FaultKind.BAD_ENV,
    FaultKind.MEMORY_LIMIT,
    FaultKind.SERVICE_SELECTOR,
    FaultKind.NETWORK_DENY,
    FaultKind.DNS_BREAK,
    FaultKind.PVC_MISMATCH,
)


@dataclass(frozen=True)
class Selection:
    faults: tuple[FaultSpec, ...]
    #: Kinds with no eligible workload in this inventory, with the reason.
    skipped: dict[str, str] = field(default_factory=dict)


def select_faults(
    workloads: Iterable[Workload],
    count: int,
    *,
    seed: int,
    kinds: Sequence[FaultKind] = DEFAULT_KINDS,
    exclude: Collection[str] = (),
) -> Selection:
    """``count`` faults, one kind per round in a seeded order, each on the least-used eligible workload.

    A (kind, workload) pair is never chosen twice and two consecutive faults never share a workload, so every fault
    starts from a baseline the previous one did not touch. ``exclude`` names workloads (by name or ID) that must stay
    untouched. The same inventory and seed always give the same stream.
    """
    if count < 1:
        raise ValueError("count must be at least 1")
    rng = random.Random(seed)
    candidates = [w for w in workloads if w.name not in exclude and w.id not in exclude]
    if not candidates:
        raise ValueError("no workloads to target")
    pool: dict[FaultKind, list[FaultSpec]] = {}
    skipped: dict[str, str] = {}
    for kind in kinds:
        built = [spec for w in candidates if (spec := build(kind, w)) is not None]
        if built:
            pool[kind] = built
        else:
            skipped[kind.value] = "no eligible workload"
    if not pool:
        raise ValueError("no fault kind is eligible on this inventory")
    order = list(pool)
    rng.shuffle(order)
    uses: dict[str, int] = {w.id: 0 for w in candidates}
    chosen: list[FaultSpec] = []
    taken: set[tuple[str, str]] = set()
    cursor = 0
    stalled = 0
    while len(chosen) < count and stalled < len(order):
        kind = order[cursor % len(order)]
        cursor += 1
        previous = chosen[-1].workload.id if chosen else None
        options = [s for s in pool[kind] if (kind.value, s.workload.id) not in taken and s.workload.id != previous]
        if not options:
            stalled += 1
            continue
        stalled = 0
        least = min(uses[s.workload.id] for s in options)
        pick = rng.choice([s for s in options if uses[s.workload.id] == least])
        taken.add((kind.value, pick.workload.id))
        uses[pick.workload.id] += 1
        chosen.append(pick)
    if len(chosen) < count:
        skipped["_count"] = f"only {len(chosen)} distinct (kind, workload) faults are available, {count} requested"
    return Selection(tuple(chosen), skipped)
