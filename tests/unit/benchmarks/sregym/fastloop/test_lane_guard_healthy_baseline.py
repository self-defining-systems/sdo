"""A lease-fenced row with the healthy baseline on must capture it and reach injection.

The row's own wiring runs here: the installed lane guard, the harness kubectl runner and a real kubectl, against a
local stand-in for the API server. Only the publication exec into the controller pod is recorded instead of run.
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.adapter import kubectl
from benchmarks.sregym.adapter.healthy_baseline import capture_snapshots
from benchmarks.sregym.fastloop import cli
from benchmarks.sregym.fastloop.lane_guard import LaneGuard
from benchmarks.sregym.fastloop.worker_client import SregymWorker

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

_VALUE = base64.b64encode(b"synthetic-secret-value").decode()
_SECRETS = [
    {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "with-keys",
            "namespace": "app",
            "labels": {"example.test/owner": "synthetic", "e": ""},
            "annotations": {"kubectl.kubernetes.io/last-applied-configuration": _VALUE},
        },
        "type": "kubernetes.io/tls",
        "data": {"tls.key": _VALUE, "tls.crt": _VALUE},
    },
    {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "empty", "namespace": "app"}, "type": "Opaque"},
]
# group version -> (resource, kind, namespaced)
_API: dict[str, list[tuple[str, str, bool]]] = {
    "v1": [
        ("configmaps", "ConfigMap", True),
        ("services", "Service", True),
        ("pods", "Pod", True),
        ("endpoints", "Endpoints", True),
        ("events", "Event", True),
        ("persistentvolumeclaims", "PersistentVolumeClaim", True),
        ("serviceaccounts", "ServiceAccount", True),
        ("secrets", "Secret", True),
        ("nodes", "Node", False),
    ],
    "apps/v1": [
        ("deployments", "Deployment", True),
        ("replicasets", "ReplicaSet", True),
        ("statefulsets", "StatefulSet", True),
        ("daemonsets", "DaemonSet", True),
    ],
    "batch/v1": [("jobs", "Job", True), ("cronjobs", "CronJob", True)],
    "autoscaling/v2": [("horizontalpodautoscalers", "HorizontalPodAutoscaler", True)],
    "policy/v1": [("poddisruptionbudgets", "PodDisruptionBudget", True)],
    "networking.k8s.io/v1": [("networkpolicies", "NetworkPolicy", True), ("ingresses", "Ingress", True)],
    "discovery.k8s.io/v1": [("endpointslices", "EndpointSlice", True)],
}


def _document(path: str) -> dict[str, Any] | None:
    parts = [part for part in path.split("?")[0].split("/") if part]
    if parts == ["api"]:
        return {"kind": "APIVersions", "versions": ["v1"]}
    if parts == ["apis"]:
        groups = []
        for group_version in _API:
            if "/" not in group_version:
                continue
            group, version = group_version.split("/")
            entry = {"groupVersion": group_version, "version": version}
            groups.append({"name": group, "versions": [entry], "preferredVersion": entry})
        return {"kind": "APIGroupList", "apiVersion": "v1", "groups": groups}
    group_version = parts[1] if parts[0] == "api" else "/".join(parts[1:3])
    rest = parts[2:] if parts[0] == "api" else parts[3:]
    if not rest:
        return {
            "kind": "APIResourceList",
            "apiVersion": "v1",
            "groupVersion": group_version,
            "resources": [
                {"name": name, "singularName": "", "kind": kind, "namespaced": namespaced, "verbs": ["get", "list"]}
                for name, kind, namespaced in _API[group_version]
            ],
        }
    resource = rest[-1]
    kind = next(kind for name, kind, _ in _API[group_version] if name == resource)
    items = _SECRETS if resource == "secrets" else []
    return {"apiVersion": group_version, "kind": f"{kind}List", "metadata": {}, "items": items}


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        document = _document(self.path)
        body = json.dumps(document).encode()
        self.send_response(200 if document is not None else 404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: Any) -> None:  # quiet
        return


@pytest.fixture
def lane_kubeconfig(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    path = tmp_path / "kubeconfig"
    path.write_text(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [{"name": "c", "cluster": {"server": f"http://127.0.0.1:{server.server_address[1]}"}}],
                "users": [{"name": "u", "user": {}}],
                "contexts": [{"name": "x", "context": {"cluster": "c", "user": "u"}}],
                "current-context": "x",
            }
        )
    )
    monkeypatch.setenv("HOME", str(tmp_path))  # kubectl's discovery cache
    monkeypatch.delenv("SREGYM_BASE_KUBECONFIG", raising=False)
    monkeypatch.setenv("KUBECONFIG", str(path))
    # LaneGuard.install patches these process-wide; monkeypatch restores them.
    monkeypatch.setattr(subprocess, "Popen", subprocess.Popen)
    monkeypatch.setattr(SregymWorker, "request", SregymWorker.request)
    try:
        yield path
    finally:
        server.shutdown()
        server.server_close()


class _Inner:
    def inject_after_resume(self, control_namespace: str, generation: str, inject: Any) -> dict[str, float]:
        inject()
        return {}


@pytest.mark.real_kubectl
def test_lease_fenced_row_with_healthy_baseline_reaches_injection_without_secret_values(
    lane_kubeconfig: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    published: list[str] = []
    events: list[str] = []

    def runner(args: list[str], *, namespace: str | None, input_text: str | None = None, check: bool = True):
        if args[0] == "exec":
            published.append(input_text or "")
            return subprocess.CompletedProcess(args, 0, "", "")
        completed = kubectl(args, namespace=namespace, input_text=input_text, check=check)
        assert _VALUE not in completed.stdout + completed.stderr
        return completed

    monkeypatch.setattr(cli, "kubectl", runner)
    monkeypatch.setitem(capture_snapshots.__kwdefaults__, "sleep", lambda _: None)  # no real sample spacing
    LaneGuard("synthetic-50", lane_kubeconfig, "fixture", coord="/bin/true").install()
    row = argparse.Namespace(inject_before_resume=False, healthy_baseline=True)

    cli._cluster_ops(row, "app", ops=_Inner()).inject_after_resume(  # type: ignore[arg-type]
        "ctl", "g1", lambda: events.append("inject")
    )

    assert events == ["inject"]
    snapshots = json.loads(published[0])
    assert snapshots["healthy-0.json"]["secrets"] == [
        {
            "name": "with-keys",
            "namespace": "app",
            "type": "kubernetes.io/tls",
            "labels": {"example.test/owner": "synthetic", "e": ""},
            "dataKeys": ["tls.crt", "tls.key"],
        },
        {"name": "empty", "namespace": "app", "type": "Opaque", "labels": {}, "dataKeys": []},
    ]
    assert _VALUE not in published[0]
