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
import subprocess
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
) -> float:
    """Return the lane's total CPU cap after installing and checking stock telemetry."""

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

    nodes = command(["kind", "get", "nodes", "--name", cluster]).split()
    caps = command(["docker", "inspect", "--format", "{{.HostConfig.NanoCpus}}", *nodes]).split()
    if not nodes or len(caps) != len(nodes) or any(int(cap) <= 0 for cap in caps):
        raise ValueError("Oracle observability requires every lane node to be Docker CPU capped")
    cores = sum(int(cap) for cap in caps) / 1e9
    kube = ["kubectl", "--kubeconfig", str(kubeconfig)]
    command(
        [*kube, "apply", "--server-side", "-f", str(Path(__file__).parent / "infrastructure/openebs-operator.yaml")]
    )
    command([*kube, "-n", "openebs", "rollout", "status", "deployment/openebs-localpv-provisioner", "--timeout=300s"])
    command(
        [*kube, "apply", "-f", "-"],
        input=json.dumps({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "observe"}}),
    )
    command([*kube, "-n", "observe", "apply", "-f", str(observer / "prometheus/prometheus-pvc.yml")])
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


def route_astronomy_traces(kubeconfig: Path, *, run: Run = subprocess.run) -> None:
    """Match stock Jaeger.create_external_name_service after application deployment."""
    kube = ["kubectl", "--kubeconfig", str(kubeconfig), "-n", "astronomy-shop"]
    # Stock removes only legacy Jaeger workloads selected by app-name=jaeger.
    for kind in ("deployment", "statefulset"):
        run(
            [*kube, "delete", kind, "-l", "app-name=jaeger", "--ignore-not-found"],
            check=True,
            capture_output=True,
            text=True,
            timeout=wait_for(WaitKind.SLOW_COMMAND),
        )
    changed = False
    for name in ("jaeger", "jaeger-agent", "jaeger-collector", "jaeger-query"):
        existing = run(
            [*kube, "get", "service", name, "--ignore-not-found", "-o", "json"],
            check=True,
            capture_output=True,
            text=True,
            timeout=wait_for(WaitKind.SLOW_COMMAND),
        )
        if existing.stdout.strip():
            spec = json.loads(existing.stdout).get("spec", {})
            if (
                spec.get("type") == "ExternalName"
                and spec.get("externalName") == "otel-collector.observe.svc.cluster.local"
            ):
                continue
        changed = True
        run(
            [*kube, "delete", "service", name, "--ignore-not-found"],
            check=True,
            capture_output=True,
            text=True,
            timeout=wait_for(WaitKind.SLOW_COMMAND),
        )
        run(
            [
                *kube,
                "create",
                "service",
                "externalname",
                name,
                "--external-name",
                "otel-collector.observe.svc.cluster.local",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=wait_for(WaitKind.SLOW_COMMAND),
        )

    if changed:
        run(
            [*kube, "rollout", "restart", "daemonset/otel-collector-agent"],
            check=True,
            capture_output=True,
            text=True,
            timeout=wait_for(WaitKind.SLOW_COMMAND),
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--sregym-dir", type=Path, required=True)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--route-astronomy-traces", action="store_true")
    args = parser.parse_args()
    cores = prepare_oracle_observability(kubeconfig=args.kubeconfig, sregym_dir=args.sregym_dir, cluster=args.cluster)
    if args.route_astronomy_traces:
        route_astronomy_traces(args.kubeconfig)
    print(json.dumps({"lane_cpu_cap": cores, "namespace": "observe"}))


if __name__ == "__main__":
    main()
