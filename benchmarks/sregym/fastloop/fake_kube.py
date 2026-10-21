"""Namespace-local HTTP Kubernetes API backed by a captured corpus snapshot."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from pathlib import Path

_RESOURCES = {
    "v1": {
        "configmaps": ("ConfigMap", "configMaps"),
        "services": ("Service", "services"),
        "pods": ("Pod", "pods"),
        "endpoints": ("Endpoints", "endpoints"),
        "events": ("Event", "events"),
    },
    "apps/v1": {"deployments": ("Deployment", "deployments"), "replicasets": ("ReplicaSet", "replicaSets")},
    "discovery.k8s.io/v1": {"endpointslices": ("EndpointSlice", "endpointSlices")},
    "networking.k8s.io/v1": {"networkpolicies": ("NetworkPolicy", "networkPolicies")},
}


class FakeKubernetesAPIServer:
    def __init__(self, snapshot: dict[str, Any]) -> None:
        self.namespace = snapshot.get("namespace") or snapshot.get("namespaceName")
        if not isinstance(self.namespace, str) or not self.namespace:
            raise ValueError("captured snapshot requires a namespace")
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
        for api_version, group in _RESOURCES.items():
            for plural, (kind, collection) in group.items():
                for item in snapshot.get(collection) or []:
                    value = json.loads(json.dumps(item))
                    value["apiVersion"] = api_version
                    value["kind"] = kind
                    self.objects[(plural, value["metadata"]["name"])] = value
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *_args: Any) -> None:
                return

            def reply(self, status: int, value: dict[str, Any]) -> None:
                data = json.dumps(value).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:
                status, value = server.read(urlsplit(self.path).path)
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
        self._thread.join(timeout=2)

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

    def read(self, path: str) -> tuple[int, dict[str, Any]]:
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
                            "namespaced": True,
                            "kind": kind,
                            "verbs": ["get", "list", "watch", "patch", "update"],
                        }
                        for plural, (kind, _) in resources.items()
                    ],
                }
            parts = path.removeprefix(prefix + "/").split("/") if path.startswith(prefix + "/") else []
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
                "metadata": {},
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

        merge(current, value)
        return 200, current
