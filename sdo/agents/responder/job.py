from __future__ import annotations

import json
import os
import ssl
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

SERVICE_ACCOUNT_ROOT = Path("/var/run/secrets/kubernetes.io/serviceaccount")


class KubernetesResultPublisher:
    def __init__(self, *, namespace: str, api_host: str | None = None, api_port: str | None = None) -> None:
        self.namespace = namespace
        host = api_host or os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
        port = api_port or os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        self.base_url = f"https://{host}:{port}"

    def publish_payload(self, name: str, key: str, payload: str) -> None:
        token = (SERVICE_ACCOUNT_ROOT / "token").read_text(encoding="utf-8").strip()
        body = json.dumps(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": name, "namespace": self.namespace},
                "data": {key: payload},
            }
        ).encode()
        request = urllib.request.Request(
            f"{self.base_url}/api/v1/namespaces/{self.namespace}/configmaps",
            data=body,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            method="POST",
        )
        context = ssl.create_default_context(cafile=str(SERVICE_ACCOUNT_ROOT / "ca.crt"))
        try:
            with urllib.request.urlopen(request, context=context, timeout=30):
                return
        except urllib.error.HTTPError as exc:
            if exc.code != 409:
                raise
        # Idempotent Job replay may encounter the result created by its prior
        # incarnation. A matching existing result is already success.
        get_request = urllib.request.Request(
            f"{self.base_url}/api/v1/namespaces/{self.namespace}/configmaps/{name}",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(get_request, context=context, timeout=30) as response:
            existing = json.loads(response.read())
        if existing.get("data", {}).get(key) != payload:
            raise RuntimeError(f"existing result ConfigMap {name!r} has different payload")


def write_application_kubeconfig(namespace: str) -> Path | None:
    """Point in-pod kubectl at the incident's application namespace.

    A responder Job may run in the controller's own namespace, so the pod's
    service-account namespace is not the application. Outside a pod (no
    service-account token) the ambient kubeconfig is left untouched.
    """

    token = SERVICE_ACCOUNT_ROOT / "token"
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "").strip()
    if not token.is_file() or not host:
        return None
    port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
    config = {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [
            {
                "name": "in-cluster",
                "cluster": {
                    "server": f"https://{host}:{port}",
                    "certificate-authority": str(SERVICE_ACCOUNT_ROOT / "ca.crt"),
                },
            }
        ],
        "users": [{"name": "responder", "user": {"tokenFile": str(token)}}],
        "contexts": [
            {"name": "application", "context": {"cluster": "in-cluster", "user": "responder", "namespace": namespace}}
        ],
        "current-context": "application",
    }
    proxy_url = os.environ.get("SDO_API_PROXY_URL", "")
    if proxy_url:
        if proxy_url != "http://127.0.0.1:18080":
            raise ValueError("SDO_API_PROXY_URL must be the responder localhost proxy")
        config["clusters"][0]["cluster"] = {"server": proxy_url}
        config["users"][0]["user"] = {}
    path = Path(tempfile.gettempdir()) / "sdo-responder-kubeconfig.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    path.chmod(0o600)
    return path
