"""Install stock SREGym oracle telemetry before deploying an astronomy seed.

The harness gitlink pins Prometheus (including its vendored subcharts), Jaeger
and OTel. Never run Helm dependency update: it would float those dependencies.
OpenEBS is the stock conductor prerequisite, vendored from openebs/charts at
73c22b2bd9df529b4ae96b1aeb40e468a6ad3a3c/openebs-operator.yaml.
All pods run inside the existing Docker-capped lane; no extra nodes are added.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from benchmarks.sregym.adapter.budgets import WaitKind, wait_for

Run = Callable[..., subprocess.CompletedProcess[str]]


def prepare_oracle_observability(
    *,
    kubeconfig: Path,
    sregym_dir: Path,
    cluster: str,
    run: Run = subprocess.run,
    remote: bool | None = None,
    managed_storage: bool | None = None,
) -> float:
    """Return the lane's total CPU cap after installing and checking stock telemetry.

    A ``remote`` lane (EKS) has no kind nodes to cap-check (0.0 is returned). ``managed_storage`` (a virtual lane,
    whose StorageClasses are its host's) installs no OpenEBS and lets the claim use the default class. Unset, both
    follow the worker environment (``SREGYM_CLUSTER_PLATFORM``, ``SREGYM_MANAGED_STORAGE``)."""

    if remote is None:
        remote = os.environ.get("SREGYM_CLUSTER_PLATFORM", "kind") != "kind"
    if managed_storage is None:
        managed_storage = os.environ.get("SREGYM_MANAGED_STORAGE", "") == "1"

    observer = sregym_dir / "sregym/observer"
    if not observer.is_dir():
        raise ValueError(f"Oracle observability needs {observer}: the SREGym submodule is not checked out")

    def command(argv: list[str], **kwargs: Any) -> str:
        try:
            return run(
                argv, check=True, text=True, capture_output=True, timeout=wait_for(WaitKind.OPERATION), **kwargs
            ).stdout
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(f"{' '.join(argv)} exited {exc.returncode}: {exc.stderr}") from exc

    cores = 0.0
    if not remote:
        nodes = command(["kind", "get", "nodes", "--name", cluster]).split()
        caps = command(["docker", "inspect", "--format", "{{.HostConfig.NanoCpus}}", *nodes]).split()
        if not nodes or len(caps) != len(nodes) or any(int(cap) <= 0 for cap in caps):
            raise ValueError("Oracle observability requires every lane node to be Docker CPU capped")
        cores = sum(int(cap) for cap in caps) / 1e9
    kube = ["kubectl", "--kubeconfig", str(kubeconfig)]
    if not managed_storage:
        command(
            [*kube, "apply", "--server-side", "-f", str(Path(__file__).parent / "infrastructure/openebs-operator.yaml")]
        )
        command(
            [*kube, "-n", "openebs", "rollout", "status", "deployment/openebs-localpv-provisioner", "--timeout=300s"]
        )
    command(
        [*kube, "apply", "-f", "-"],
        input=json.dumps({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "observe"}}),
    )
    claim = observer / "prometheus/prometheus-pvc.yml"
    if managed_storage:
        claim = _default_class_claim(claim)
    try:
        command([*kube, "-n", "observe", "apply", "-f", str(claim)])
    finally:
        if managed_storage:
            claim.unlink(missing_ok=True)
    command(
        [
            "helm",
            "upgrade",
            "--install",
            "prometheus",
            str(observer / "prometheus/prometheus"),
            "--kubeconfig",
            str(kubeconfig),
            "--namespace",
            "observe",
            "--wait",
            "--timeout",
            "5m",
            # Lanes of one host cluster share its NodePort range; the chart's fixed 32000 allows one lane.
            *(
                ["--set", "server.persistentVolume.storageClass=null", "--set", "server.service.nodePort=null"]
                if managed_storage
                else []
            ),
        ]
    )
    command([*kube, "apply", "-f", str(observer / "jaeger/jaeger.yaml")])
    command(
        [*kube, "apply", "-f", "-"],
        input=json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "jaeger-backend", "namespace": "observe"},
                "spec": {
                    "ports": [{"port": 4317, "name": "otlp-grpc"}, {"port": 16686, "name": "ui"}],
                    "selector": {"app-name": "jaeger"},
                },
            }
        ),
    )
    command([*kube, "apply", "-f", str(observer / "otel_collector/otel-collector.yaml")])
    for deployment in ("prometheus-server", "otel-collector"):
        command([*kube, "-n", "observe", "rollout", "status", f"deployment/{deployment}", "--timeout=180s"])
    command(
        [
            *kube,
            "-n",
            "observe",
            "exec",
            "deploy/prometheus-server",
            "-c",
            "prometheus-server",
            "--",
            "wget",
            "-qO-",
            "http://localhost:9090/api/v1/rules",
        ]
    )
    return cores


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--sregym-dir", type=Path, required=True)
    parser.add_argument("--cluster", required=True)
    args = parser.parse_args()
    cores = prepare_oracle_observability(kubeconfig=args.kubeconfig, sregym_dir=args.sregym_dir, cluster=args.cluster)
    print(json.dumps({"lane_cpu_cap": cores, "namespace": "observe"}))


if __name__ == "__main__":
    main()


def _default_class_claim(claim: Path) -> Path:
    """A copy of the claim manifest without ``storageClassName``, so the cluster's default class serves it."""
    import yaml

    manifest = yaml.safe_load(claim.read_text(encoding="utf-8"))
    manifest.get("spec", {}).pop("storageClassName", None)
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False, encoding="utf-8") as handle:
        yaml.safe_dump(manifest, handle)
    return Path(handle.name)
