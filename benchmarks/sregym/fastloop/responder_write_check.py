"""Class check: the responder can write every object kind a fault creates or mutates.

A fault family whose objects the responder ServiceAccount cannot write forces a
workaround (P53, lane 55b ResourceQuota). This reads the fault footprints of a
sequence plan, asks the API server with a SubjectAccessReview (never
`kubectl auth can-i` on a subresource string) whether the responder may
create/update/patch/delete each kind, and reports the gaps as a table. Kinds the
installer withholds on purpose (credentials, identity, RBAC) are listed
separately with their reason, so only an unintended gap fails the check.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from benchmarks.sregym.adapter import kubectl

WRITE_VERBS = ("create", "update", "patch", "delete")

# footprint kind -> (API group, resource, namespaced). A kind missing here is an
# error: a new fault object needs an explicit grant decision.
KIND_RESOURCES: dict[str, tuple[str, str, bool]] = {
    "ConfigMap": ("", "configmaps", True),
    "CronJob": ("batch", "cronjobs", True),
    "Deployment": ("apps", "deployments", True),
    "HorizontalPodAutoscaler": ("autoscaling", "horizontalpodautoscalers", True),
    "Ingress": ("networking.k8s.io", "ingresses", True),
    "Job": ("batch", "jobs", True),
    "NetworkPolicy": ("networking.k8s.io", "networkpolicies", True),
    "PersistentVolumeClaim": ("", "persistentvolumeclaims", True),
    "ResourceQuota": ("", "resourcequotas", True),
    "Service": ("", "services", True),
    "MutatingWebhookConfiguration": ("admissionregistration.k8s.io", "mutatingwebhookconfigurations", False),
    "ValidatingWebhookConfiguration": ("admissionregistration.k8s.io", "validatingwebhookconfigurations", False),
    "Secret": ("", "secrets", True),
    "ServiceAccount": ("", "serviceaccounts", True),
    "ClusterRole": ("rbac.authorization.k8s.io", "clusterroles", False),
    "ClusterRoleBinding": ("rbac.authorization.k8s.io", "clusterrolebindings", False),
    "Namespace": ("", "namespaces", False),
}

# Withheld on purpose; the repair for these faults acts on other objects.
WITHHELD_BY_DESIGN: dict[str, str] = {
    "Secret": "credential kind: never granted to the responder",
    "ServiceAccount": "identity kind: never granted to the responder",
    "ClusterRole": "RBAC escalation kind: never granted to the responder",
    "ClusterRoleBinding": "RBAC escalation kind: never granted to the responder",
    "Namespace": "cluster lifecycle: the fault's backend namespace is not repaired by deleting it",
}

APP_NAMESPACES = {
    "astronomy_shop": "astronomy-shop",
    "hotel_reservation": "hotel-reservation",
    "social_network": "social-network",
}
FAULT_BACKEND_NAMESPACE = "policy-system"

_DRAW = re.compile(r"FaultDraw\(key='(?P<key>[^']+)', app='(?P<app>[^']+)'")
_FOOTPRINT = re.compile(r"Footprint\(kind='(?P<kind>[^']+)', name='(?P<name>[^']+)'")


@dataclass(frozen=True)
class FaultObject:
    fault: str
    app: str
    kind: str
    name: str

    def __post_init__(self) -> None:
        if self.kind not in KIND_RESOURCES:
            raise ValueError(f"eval: fault {self.fault} touches {self.kind}, which has no responder grant decision")

    @property
    def namespace(self) -> str:
        """Where the responder would write it; empty for cluster scope."""

        namespaced = KIND_RESOURCES[self.kind][2]
        if not namespaced:
            return ""
        if self.app not in APP_NAMESPACES:
            raise ValueError(f"eval: fault {self.fault} has unknown app {self.app}")
        return APP_NAMESPACES[self.app]


@dataclass(frozen=True)
class Row:
    kind: str
    group: str
    resource: str
    namespace: str
    faults: tuple[str, ...]
    denied_verbs: tuple[str, ...]
    status: str  # "ok", "withheld" or "GAP"
    reason: str


Allowed = Callable[[str, str, str, str], bool]  # (namespace, group, resource, verb)


def fault_objects(plan: Path) -> list[FaultObject]:
    """Fault footprints of a sequence plan (the draws of `stream/plan-*.json`)."""

    rows = json.loads(plan.read_text())
    objects: list[FaultObject] = []
    for row in rows:
        for draw in row["draws"]:
            head = _DRAW.search(draw)
            if head is None:
                raise ValueError(f"eval: unreadable fault draw in {plan}")
            for footprint in _FOOTPRINT.finditer(draw):
                objects.append(FaultObject(head["key"], head["app"], footprint["kind"], footprint["name"]))
    return objects


def check(objects: Iterable[FaultObject], allowed: Allowed) -> list[Row]:
    grouped: dict[tuple[str, str], set[str]] = {}
    for obj in objects:
        grouped.setdefault((obj.kind, obj.namespace), set()).add(obj.fault)
    rows = []
    for (kind, namespace), faults in sorted(grouped.items()):
        group, resource, _ = KIND_RESOURCES[kind]
        denied = tuple(verb for verb in WRITE_VERBS if not allowed(namespace, group, resource, verb))
        if not denied:
            status, reason = "ok", ""
        elif kind in WITHHELD_BY_DESIGN:
            status, reason = "withheld", WITHHELD_BY_DESIGN[kind]
        else:
            status, reason = "GAP", "responder cannot write a kind a fault creates or mutates"
        rows.append(Row(kind, group, resource, namespace, tuple(sorted(faults)), denied, status, reason))
    return rows


def render(rows: list[Row]) -> str:
    lines = ["kind | scope | denied verbs | status | faults", "--- | --- | --- | --- | ---"]
    for row in rows:
        faults = ", ".join(row.faults)
        reason = f" ({row.reason})" if row.reason else ""
        lines.append(
            f"{row.kind} | {row.namespace or 'cluster'} | {','.join(row.denied_verbs) or '-'} | {row.status}{reason} | {faults}"
        )
    return "\n".join(lines)


def gaps(rows: Iterable[Row]) -> list[Row]:
    return [row for row in rows if row.status == "GAP"]


def subject_access_review(
    kubeconfig: Path, *, control_namespace: str, run: Callable[..., Any] | None = None
) -> Allowed:
    """Ask the API server as `system:serviceaccount:<control>:sdo-responder`."""

    runner = run or kubectl
    user = f"system:serviceaccount:{control_namespace}:sdo-responder"
    groups = ["system:serviceaccounts", f"system:serviceaccounts:{control_namespace}", "system:authenticated"]

    def allowed(namespace: str, group: str, resource: str, verb: str) -> bool:
        attributes: dict[str, str] = {"verb": verb, "group": group, "resource": resource}
        if namespace:
            attributes["namespace"] = namespace
        review = {
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SubjectAccessReview",
            "spec": {"user": user, "groups": groups, "resourceAttributes": attributes},
        }
        completed = runner(
            ["create", "-f", "-", "-o", "json"],
            namespace=None,
            kubeconfig=kubeconfig,
            input_text=json.dumps(review),
        )
        return bool(json.loads(completed.stdout)["status"].get("allowed", False))

    return allowed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True, help="sequence plan with fault draws (stream/plan-*.json)")
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--control-namespace", required=True, help="namespace of the sdo-responder ServiceAccount")
    args = parser.parse_args(argv)
    rows = check(fault_objects(args.plan), subject_access_review(args.kubeconfig, control_namespace=args.control_namespace))
    print(render(rows))
    return 1 if gaps(rows) else 0


if __name__ == "__main__":
    sys.exit(main())
