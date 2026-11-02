"""Namespace-local HTTP Kubernetes API backed by a captured corpus snapshot."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlsplit

from benchmarks.sregym.adapter import WaitKind, wait_for

if TYPE_CHECKING:
    from pathlib import Path

_RESOURCES = {
    "v1": {
        "configmaps": ("ConfigMap", "configMaps"),
        "services": ("Service", "services"),
        "pods": ("Pod", "pods"),
        "endpoints": ("Endpoints", "endpoints"),
        "events": ("Event", "events"),
        "persistentvolumeclaims": ("PersistentVolumeClaim", "persistentVolumeClaims"),
        "serviceaccounts": ("ServiceAccount", "serviceAccounts"),
        "nodes": ("Node", "nodes"),
        "secrets": ("Secret", "secrets"),
    },
    "apps/v1": {
        "deployments": ("Deployment", "deployments"),
        "replicasets": ("ReplicaSet", "replicaSets"),
        "statefulsets": ("StatefulSet", "statefulSets"),
        "daemonsets": ("DaemonSet", "daemonSets"),
    },
    "discovery.k8s.io/v1": {"endpointslices": ("EndpointSlice", "endpointSlices")},
    "networking.k8s.io/v1": {
        "networkpolicies": ("NetworkPolicy", "networkPolicies"),
        "ingresses": ("Ingress", "ingresses"),
    },
    "batch/v1": {"jobs": ("Job", "jobs"), "cronjobs": ("CronJob", "cronJobs")},
    "autoscaling/v2": {"horizontalpodautoscalers": ("HorizontalPodAutoscaler", "horizontalPodAutoscalers")},
    "policy/v1": {"poddisruptionbudgets": ("PodDisruptionBudget", "podDisruptionBudgets")},
}


class FakeKubernetesAPIServer:
    def __init__(self, snapshot: dict[str, Any]) -> None:
        self.namespace = snapshot.get("namespace") or snapshot.get("namespaceName")
        if not isinstance(self.namespace, str) or not self.namespace:
            raise ValueError("captured snapshot requires a namespace")
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
        self._condition = threading.Condition(threading.RLock())
        self._revision = 0
        self._events: list[tuple[int, str, dict[str, Any]]] = []
        self._kubelet: dict[str, tuple[dict[str, Any], str]] = {}
        # Objects-only captures (object_snapshot_version 2) store each object once.
        objects_only = snapshot.get("object_snapshot_version", 0) >= 2
        for api_version, group in _RESOURCES.items():
            for plural, (kind, collection) in group.items():
                items = (
                    [
                        item
                        for item in snapshot.get("objects") or []
                        if (item.get("apiVersion"), item.get("kind")) == (api_version, kind)
                        and item.get("metadata", {}).get("namespace") == self.namespace
                    ]
                    if objects_only
                    else snapshot.get(collection) or []
                )
                for item in items:
                    self.set_object(plural, dict(item, apiVersion=api_version, kind=kind))
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                return

            def reply(self, status: int, value: dict[str, Any] | str) -> None:
                data = value.encode() if isinstance(value, str) else json.dumps(value).encode()
                self.send_response(status)
                self.send_header("Content-Type", "text/plain" if isinstance(value, str) else "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:
                parsed = urlsplit(self.path)
                query = parse_qs(parsed.query)
                if query.get("watch") == ["true"]:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    revision = int(query.get("resourceVersion", ["0"])[0])
                    plural = parsed.path.rsplit("/", 1)[-1]
                    # Bounded long poll; the informer reconnects with the last revision.
                    with server._condition:
                        events = server._watch_events(plural, revision)
                        if not events:
                            server._condition.wait_for(
                                lambda: bool(server._watch_events(plural, revision)), timeout=1.0
                            )
                            events = server._watch_events(plural, revision)
                    for event in events:
                        self.wfile.write(json.dumps(event).encode() + b"\n")
                    return
                status, value = server.read(parsed.path)
                self.reply(status, value)

            def do_PATCH(self) -> None:
                try:
                    patch_value = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
                except ValueError:
                    self.reply(400, {"kind": "Status", "message": "invalid JSON patch"})
                    return
                status, value = server.patch(urlsplit(self.path).path, patch_value)
                self.reply(status, value)

        self._http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._http.serve_forever, daemon=True)

    def __enter__(self) -> FakeKubernetesAPIServer:
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._http.shutdown()
        self._http.server_close()
        self._thread.join(timeout=wait_for(WaitKind.PROBE))

    def write_kubeconfig(self, path: Path) -> Path:
        host, port = self._http.server_address[:2]
        value = {
            "apiVersion": "v1",
            "kind": "Config",
            "clusters": [{"name": "replay", "cluster": {"server": f"http://{host}:{port}"}}],
            "contexts": [
                {"name": "replay", "context": {"cluster": "replay", "user": "replay", "namespace": self.namespace}}
            ],
            "users": [{"name": "replay", "user": {}}],
            "current-context": "replay",
        }
        path.write_text(json.dumps(value), encoding="utf-8")
        path.chmod(0o600)
        return path

    def set_object(self, plural: str, value: dict[str, Any]) -> None:
        """Publish a copied object and an informer event; Secret values are discarded."""
        value = json.loads(json.dumps(value))
        if plural == "secrets":
            if "metadata" not in value:
                value = {
                    "metadata": {key: value[key] for key in ("name", "namespace", "labels") if key in value},
                    "type": value.get("type", "Opaque"),
                    "data": dict.fromkeys(value.get("dataKeys", []), ""),
                }
            metadata = value["metadata"]
            value = {
                "apiVersion": "v1",
                "kind": "Secret",
                "type": value.get("type", "Opaque"),
                "metadata": {key: metadata[key] for key in ("name", "namespace", "labels") if key in metadata},
                "data": dict.fromkeys(sorted(set(value.get("data", {})) | set(value.get("stringData", {}))), ""),
            }
        with self._condition:
            key = (plural, value["metadata"]["name"])
            event_type = "MODIFIED" if key in self.objects else "ADDED"
            self._revision += 1
            value["metadata"]["resourceVersion"] = str(self._revision)
            self.objects[key] = value
            self._events.append((self._revision, plural, {"type": event_type, "object": value}))
            self._events = self._events[-256:]
            self._condition.notify_all()

    def _watch_events(self, plural: str, revision: int) -> list[dict[str, Any]]:
        return [event for rev, resource, event in self._events if resource == plural and rev > revision]

    def set_kubelet(self, node: str, summary: dict[str, Any], cadvisor: str = "") -> None:
        with self._condition:
            self._kubelet[node] = (json.loads(json.dumps(summary)), cadvisor)

    def read(self, path: str) -> tuple[int, Any]:
        with self._condition:
            return self._read(path)

    def _read(self, path: str) -> tuple[int, Any]:
        # Subresource seam: pods/<name>/log can be added here independently.
        parts = path.split("/")
        if len(parts) >= 7 and parts[1:4] == ["api", "v1", "nodes"] and parts[5] == "proxy":
            data = self._kubelet.get(parts[4])
            endpoint = "/".join(parts[6:])
            if data is not None and endpoint in {"stats/summary", "metrics/cadvisor"}:
                return 200, data[0] if endpoint == "stats/summary" else data[1]
            return 404, {"kind": "Status", "message": "uncaptured kubelet endpoint"}
        if path == "/version":
            return 200, {"major": "1", "minor": "30", "gitVersion": "v1.30.0"}
        if path == "/api":
            return 200, {"kind": "APIVersions", "versions": ["v1"]}
        if path == "/apis":
            groups = []
            for version in _RESOURCES:
                if version == "v1":
                    continue
                group, short = version.split("/", 1)
                groups.append(
                    {
                        "name": group,
                        "versions": [{"groupVersion": version, "version": short}],
                        "preferredVersion": {"groupVersion": version, "version": short},
                    }
                )
            return 200, {"kind": "APIGroupList", "groups": groups}
        for api_version, resources in _RESOURCES.items():
            prefix = "/api/v1" if api_version == "v1" else f"/apis/{api_version}"
            if path == prefix:
                return 200, {
                    "kind": "APIResourceList",
                    "groupVersion": api_version,
                    "resources": [
                        {
                            "name": plural,
                            "singularName": kind.lower(),
                            "namespaced": kind != "Node",
                            "kind": kind,
                            "verbs": ["get", "list", "watch", "patch", "update"],
                        }
                        for plural, (kind, _) in resources.items()
                    ],
                }
            parts = path.removeprefix(prefix + "/").split("/") if path.startswith(prefix + "/") else []
            if parts and parts[0] == "nodes" and api_version == "v1":
                parts = ["namespaces", self.namespace, *parts]
            if (
                len(parts) not in {3, 4}
                or parts[0] != "namespaces"
                or parts[1] != self.namespace
                or parts[2] not in resources
            ):
                continue
            plural = parts[2]
            kind = resources[plural][0]
            if len(parts) == 4:
                value = self.objects.get((plural, parts[3]))
                return (200, value) if value is not None else (404, {"kind": "Status", "message": "not found"})
            return 200, {
                "apiVersion": api_version,
                "kind": kind + "List",
                "metadata": {"resourceVersion": str(self._revision)},
                "items": [value for (resource, _), value in self.objects.items() if resource == plural],
            }
        return 404, {"kind": "Status", "message": "uncaptured Kubernetes API path"}

    def patch(self, path: str, value: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        status, current = self.read(path)
        if status != 200 or current.get("kind", "").endswith("List"):
            return 404, {"kind": "Status", "message": "uncaptured Kubernetes object"}

        def merge(target: dict[str, Any], source: dict[str, Any]) -> None:
            for key, item in source.items():
                if isinstance(item, dict) and isinstance(target.get(key), dict):
                    merge(target[key], item)
                else:
                    target[key] = item

        with self._condition:
            current = json.loads(json.dumps(current))
            merge(current, value)
            plural = path.split("/")[-2]
            self.set_object(plural, current)
            return 200, self.objects[(plural, current["metadata"]["name"])]
