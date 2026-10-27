"""Reference-derived namespace prerequisites; no Secret payload is inspected."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.fastloop.baseline_recovery import WORKLOAD_KINDS, ApplicationBaseline

if TYPE_CHECKING:
    from benchmarks.sregym.fastloop.accepted_source import AcceptedSource


@dataclass(frozen=True, order=True)
class Prerequisite:
    namespace: str
    kind: str
    name: str

    def label(self) -> str:
        return f"{self.namespace + '/' if self.namespace else ''}{self.kind}/{self.name}"


def pod_spec(value: dict[str, Any]) -> dict[str, Any] | None:
    spec = value.get("spec", {})
    kind = value.get("kind")
    if kind == "Pod":
        return spec
    if kind in {"Deployment", "DaemonSet", "StatefulSet", "ReplicaSet", "Job"}:
        return spec.get("template", {}).get("spec", {})
    if kind == "CronJob":
        return spec.get("jobTemplate", {}).get("spec", {}).get("template", {}).get("spec", {})
    return None


def prerequisites(value: dict[str, Any], namespace: str) -> set[Prerequisite]:
    """Follow Kubernetes reference fields, never arbitrary ConfigMap/env bytes."""
    refs: set[Prerequisite] = set()

    def add(kind: str, name: str | None, ns: str = namespace) -> None:
        if name:
            refs.add(Prerequisite(ns, kind, name))

    def local(kind: str, reference: dict[str, Any], key: str = "name") -> None:
        if not reference.get("optional", False):
            add(kind, reference.get(key))

    kind = value.get("kind")
    spec = value.get("spec", {})
    pod = pod_spec(value)
    if pod is not None:
        # The namespace controller creates the default account and root CA.
        account = pod.get("serviceAccountName") or pod.get("serviceAccount") or "default"
        if account != "default":
            add("ServiceAccount", account)
        for field, target in [("runtimeClassName", "RuntimeClass"), ("priorityClassName", "PriorityClass")]:
            add(target, pod.get(field), "")
        for ref in pod.get("imagePullSecrets", []):
            local("Secret", ref)
        claims = {v["metadata"]["name"] for v in spec.get("volumeClaimTemplates", [])}
        for volume in pod.get("volumes", []):
            for field, target, key in [
                ("configMap", "ConfigMap", "name"),
                ("secret", "Secret", "secretName"),
                ("persistentVolumeClaim", "PersistentVolumeClaim", "claimName"),
            ]:
                ref = volume.get(field, {})
                if target != "PersistentVolumeClaim" or ref.get(key) not in claims:
                    local(target, ref, key)
            for source in volume.get("projected", {}).get("sources", []):
                local("ConfigMap", source.get("configMap", {}))
                local("Secret", source.get("secret", {}))
            if "csi" in volume:
                add("CSIDriver", volume["csi"].get("driver"), "")
                local("Secret", volume["csi"].get("nodePublishSecretRef", {}))
        for container in [
            *pod.get("containers", []),
            *pod.get("initContainers", []),
            *pod.get("ephemeralContainers", []),
        ]:
            for env in container.get("env", []):
                source = env.get("valueFrom", {})
                local("ConfigMap", source.get("configMapKeyRef", {}))
                local("Secret", source.get("secretKeyRef", {}))
            for source in container.get("envFrom", []):
                local("ConfigMap", source.get("configMapRef", {}))
                local("Secret", source.get("secretRef", {}))
        for claim in spec.get("volumeClaimTemplates", []):
            refs.update(prerequisites({"kind": "PersistentVolumeClaim", "spec": claim.get("spec", {})}, namespace))
    if kind == "StatefulSet":
        add("Service", spec.get("serviceName"))
    if kind == "PersistentVolumeClaim":
        add("StorageClass", spec.get("storageClassName"), "")
        for field in ("dataSource", "dataSourceRef"):
            ref = spec.get(field, {})
            add(ref.get("kind", "Unknown"), ref.get("name"), ref.get("namespace", namespace))
    if kind == "ServiceAccount":
        for ref in value.get("imagePullSecrets", []):
            local("Secret", ref)
    if kind == "RoleBinding":
        ref = value.get("roleRef", {})
        add(ref.get("kind", "Role"), ref.get("name"), "" if ref.get("kind") == "ClusterRole" else namespace)
        for subject in value.get("subjects", []):
            if subject.get("kind") == "ServiceAccount" and subject.get("name") != "default":
                add("ServiceAccount", subject.get("name"), subject.get("namespace", namespace))
    refs.discard(Prerequisite(namespace, "ConfigMap", "kube-root-ca.crt"))
    return refs


def missing_prerequisites(baselines: tuple[ApplicationBaseline, ...]) -> tuple[Prerequisite, ...]:
    available = {
        Prerequisite(b.namespace, v["kind"], v["metadata"]["name"])
        for b in baselines
        for v in b.objects
        if v["kind"] in WORKLOAD_KINDS
    }
    required = {ref for b in baselines for value in b.objects for ref in prerequisites(value, b.namespace)}
    return tuple(sorted(required - available))


def complete_recovery_baselines(
    source: AcceptedSource, baselines: tuple[ApplicationBaseline, ...]
) -> tuple[ApplicationBaseline, ...]:
    """Fill legacy capture omissions from the rendered accepted source only.

    Captured installed objects always win, including their sealed runtime defaults.
    Secrets are never rendered.
    """
    namespaces = {b.namespace for b in baselines}
    available = {Prerequisite(b.namespace, v["kind"], v["metadata"]["name"]) for b in baselines for v in b.objects}
    wanted = {ref for b in baselines for value in b.objects for ref in prerequisites(value, b.namespace)}
    # Bindings are inverse edges: their subjects identify grants a pod account
    # needs, including the namespace controller's implicit default account.
    accounts = {
        Prerequisite(
            b.namespace, "ServiceAccount", pod.get("serviceAccountName") or pod.get("serviceAccount") or "default"
        )
        for b in baselines
        for value in b.objects
        if (pod := pod_spec(value)) is not None
    }
    # A declared default account may carry image-pull references or other
    # configuration even though the namespace creates its empty default.
    wanted.update(accounts)
    rows: dict[Prerequisite, dict[str, Any]] = {}
    for value in source.manifests:
        metadata = value["metadata"]
        if value["kind"] not in WORKLOAD_KINDS or metadata.get("namespace") not in namespaces:
            continue
        identity = Prerequisite(metadata["namespace"], value["kind"], metadata["name"])
        if identity not in available:
            rows[identity] = value

    def read(identity: Prerequisite) -> dict[str, Any]:
        return rows[identity]

    bindings = {identity: read(identity) for identity in rows if identity.kind == "RoleBinding"}
    selected: dict[Prerequisite, dict[str, Any]] = {}
    while True:
        previous = len(wanted), len(selected)
        for identity, value in bindings.items():
            subjects = {
                Prerequisite(subject.get("namespace", identity.namespace), "ServiceAccount", subject["name"])
                for subject in value.get("subjects", [])
                if subject.get("kind") == "ServiceAccount"
            }
            if subjects & (accounts | wanted):
                selected[identity] = value
                wanted.update(prerequisites(value, identity.namespace))
        for identity in sorted(wanted & rows.keys() - available - selected.keys()):
            value = read(identity)
            selected[identity] = value
            wanted.update(prerequisites(value, identity.namespace))
        if previous == (len(wanted), len(selected)):
            break
    return tuple(
        ApplicationBaseline(
            b.namespace, [*b.objects, *(v for ref, v in sorted(selected.items()) if ref.namespace == b.namespace)]
        )
        if any(ref.namespace == b.namespace for ref in selected)
        else b
        for b in baselines
    )
