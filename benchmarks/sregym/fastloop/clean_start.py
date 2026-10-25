"""Benchmark-owned fault cleanup and traffic isolation before a clean baseline.

Injection labels stay on this side of the benchmark boundary. Secret queries
project only namespace/name metadata, never credential values or encoded data.
"""

from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import kubectl

if TYPE_CHECKING:
    from pathlib import Path

INJECTED_SELECTOR = "platform.io/injected=true"
HARNESS_NAMESPACE = "sregym"


def benchmark_owned(value: dict[str, Any]) -> bool:
    labels = value.get("metadata", {}).get("labels", {})
    return any(
        str(label).startswith(("sregym", "benchmark"))
        for key, label in labels.items()
        if key == "app.kubernetes.io/managed-by" or key.startswith(("sregym.", "benchmark."))
    )


def isolate_benchmark_probe(manifest: dict[str, Any]) -> dict[str, Any]:
    """Move only probe placement; its script still addresses the app's Service DNS."""
    value = deepcopy(manifest)
    if benchmark_owned(value):
        value["metadata"]["namespace"] = HARNESS_NAMESPACE
    return value


def install_probe_isolation(grading: Any) -> None:
    """Adapt the shared harness factory for baseline and composite oracle probes."""
    original = grading.probe_pod_manifest
    if getattr(original, "benchmark_isolated", False):
        return

    def isolated(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return isolate_benchmark_probe(original(*args, **kwargs))

    isolated.benchmark_isolated = True
    grading.probe_pod_manifest = isolated


def _injected_objects(kubeconfig: Path) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    for namespaced in (True, False):
        discovery = kubectl(
            ["api-resources", "--verbs=list,delete", f"--namespaced={str(namespaced).lower()}", "-o", "name"],
            namespace=None,
            kubeconfig=kubeconfig,
        ).stdout
        resources = sorted({name for name in discovery.split() if name.split(".")[0] != "secrets" and "/" not in name})
        if not resources:
            continue
        args = ["get", ",".join(resources), "-l", INJECTED_SELECTOR, "-o", "json"]
        if namespaced:
            args.append("--all-namespaces")
        objects.extend(json.loads(kubectl(args, namespace=None, kubeconfig=kubeconfig).stdout)["items"])
    # kubectl's metadata-only projection keeps Secret data out of this process,
    # logs, captures and error messages. Secrets are excluded from JSON listings.
    names = kubectl(
        [
            "get",
            "secrets",
            "--all-namespaces",
            "-l",
            INJECTED_SELECTOR,
            "-o",
            "custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name",
            "--no-headers",
        ],
        namespace=None,
        kubeconfig=kubeconfig,
    ).stdout
    for line in names.splitlines():
        namespace, name = line.split()
        objects.append({"apiVersion": "v1", "kind": "Secret", "metadata": {"namespace": namespace, "name": name}})
    return objects


def assert_clean_baseline(kubeconfig: Path, *, namespace: str | None = None) -> None:
    """Fail closed if even one labelled fault object survives, in either scope."""
    remaining = _injected_objects(kubeconfig)
    if remaining:
        identities = sorted(
            f"{value['metadata'].get('namespace', '<cluster>')}/{value['kind']}/{value['metadata']['name']}"
            for value in remaining
        )
        raise RuntimeError(f"clean baseline rejected: injected objects remain: {', '.join(identities)}")

    if namespace is not None:
        listing = kubectl(
            ["get", "pods,jobs,configmaps", "-o", "json"], namespace=namespace, kubeconfig=kubeconfig
        ).stdout
        foreign = [
            f"{value['kind']}/{value['metadata']['name']}"
            for value in json.loads(listing)["items"]
            if benchmark_owned(value)
        ]
        if foreign:
            raise RuntimeError(f"clean baseline rejected: benchmark objects in {namespace}: {', '.join(foreign)}")
        for resources in (
            ["job/wrk2-job", "configmap/wrk2-payload-script"],
            ["pods", "--selector=job-name=wrk2-job"],
        ):
            legacy = kubectl(
                ["get", *resources, "--ignore-not-found=true", "-o", "name"],
                namespace=namespace,
                kubeconfig=kubeconfig,
            ).stdout
            if legacy.strip():
                raise RuntimeError(f"clean baseline rejected: harness traffic remains in {namespace}: {legacy.strip()}")


def remove_injected_objects(kubeconfig: Path) -> None:
    """Remove only harness-labelled injected objects, including RBAC and finalizers.

    Call after full harness recovery, never during partial composite recovery.
    Foreground deletion prevents a removed injector's dependents surviving admission.
    """
    for value in _injected_objects(kubeconfig):
        metadata = value["metadata"]
        namespace = metadata.get("namespace")
        group = value.get("apiVersion", "v1").partition("/")[0] if "/" in value.get("apiVersion", "v1") else ""
        resource = value["kind"].lower() + (f".{group}" if group else "") + "/" + metadata["name"]
        if metadata.get("finalizers") or value["kind"] == "Secret":
            kubectl(
                ["patch", resource, "--type=merge", "-p", '{"metadata":{"finalizers":[]}}'],
                namespace=namespace,
                kubeconfig=kubeconfig,
            )
        kubectl(
            ["delete", resource, "--ignore-not-found=true", "--cascade=foreground", "--wait=true", "--timeout=60s"],
            namespace=namespace,
            kubeconfig=kubeconfig,
        )
    assert_clean_baseline(kubeconfig)


def prepare_harness_traffic(app: Any, kubeconfig: Path) -> bool:
    """Adapt the app's existing wrk manager; its payload and rate settings stay intact."""
    if not callable(getattr(app, "create_workload", None)) or not hasattr(app, "frontend_service"):
        return False
    # Only the two DeathStarBench apps use this Wrk2 manager contract. Astronomy
    # has its own source-declared load generator and must retain that behavior.
    if app.namespace not in {"hotel-reservation", "social-network"}:
        return False

    def configure_manager() -> None:
        manager = app.wrk
        manager.namespace = HARNESS_NAMESPACE
        manager.wrk.namespace = HARNESS_NAMESPACE
        manager.url = f"http://{app.frontend_service}.{app.namespace}.svc.cluster.local:{app.frontend_port}"

    if hasattr(app, "wrk"):
        configure_manager()

    def start() -> None:
        if not hasattr(app, "wrk"):
            app.create_workload()
        manager = app.wrk
        namespace = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": HARNESS_NAMESPACE}}
        kubectl(["apply", "-f", "-"], namespace=None, kubeconfig=kubeconfig, input_text=json.dumps(namespace))
        configure_manager()
        manager.start()
        # Wrk2 logs API exceptions rather than raising. Confirm both objects
        # exist before retiring the legacy generator, so startup cannot silently
        # admit a lane whose harness traffic was never created.
        kubectl(
            ["get", "job/wrk2-job", "configmap/wrk2-payload-script", "-o", "name"],
            namespace=HARNESS_NAMESPACE,
            kubeconfig=kubeconfig,
        )
        # Upgrade warm lanes created before traffic isolation. Delete only stable
        # harness identities, never a source-declared application workload.
        kubectl(
            [
                "delete",
                "job/wrk2-job",
                "configmap/wrk2-payload-script",
                "--ignore-not-found=true",
                "--cascade=foreground",
                "--wait=true",
                "--timeout=60s",
            ],
            namespace=app.namespace,
            kubeconfig=kubeconfig,
        )

        kubectl(
            [
                "delete",
                "pods",
                "--selector=job-name=wrk2-job",
                "--ignore-not-found=true",
                "--wait=true",
                "--timeout=60s",
            ],
            namespace=app.namespace,
            kubeconfig=kubeconfig,
        )

    app.start_workload = start
    return True
