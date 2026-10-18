from __future__ import annotations

import json
import os
import ssl
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from sdo.agents.responder.codex import ResponderExecutionError, execute_incident
from sdo.agents.responder.credentials import prepare_claude_home, prepare_codex_home
from sdo.agents.views import (
    IncidentRequest,
    IncidentResult,
)

if TYPE_CHECKING:
    from collections.abc import Callable

DEFAULT_REQUEST_PATH = Path("/sdo/request/incident-request.json")
SERVICE_ACCOUNT_ROOT = Path("/var/run/secrets/kubernetes.io/serviceaccount")


class ResultPublisher(Protocol):
    def publish(self, name: str, result: IncidentResult) -> None: ...


class KubernetesResultPublisher:
    def __init__(self, *, namespace: str, api_host: str | None = None, api_port: str | None = None) -> None:
        self.namespace = namespace
        host = api_host or os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
        port = api_port or os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        self.base_url = f"https://{host}:{port}"

    def publish(self, name: str, result: IncidentResult) -> None:
        self.publish_payload(name, "incident-result.json", result.model_dump_json(exclude_none=True))

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
    path = Path(tempfile.gettempdir()) / "sdo-responder-kubeconfig.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    path.chmod(0o600)
    return path


def run_job(
    *,
    request_path: Path,
    result_name: str,
    publisher: ResultPublisher,
    executor: Callable[[IncidentRequest], IncidentResult] = execute_incident,
) -> IncidentResult:
    request = IncidentRequest.model_validate_json(request_path.read_text(encoding="utf-8"))
    kubeconfig = write_application_kubeconfig(request.namespace)
    if kubeconfig is not None:
        os.environ["KUBECONFIG"] = str(kubeconfig)
    result = executor(request)
    if result.incident_id != request.incident_id:
        raise ResponderExecutionError("responder result incident_id does not match request")
    publisher.publish(result_name, result)
    return result


def main() -> int:
    namespace = os.environ.get("SDO_NAMESPACE", "")
    result_name = os.environ.get("SDO_RESULT_CONFIGMAP", "")
    if not namespace or not result_name:
        print("SDO_NAMESPACE and SDO_RESULT_CONFIGMAP are required", file=sys.stderr)
        return 2
    try:
        prepare_codex_home()
        prepare_claude_home()
        run_job(
            request_path=Path(os.environ.get("SDO_REQUEST_PATH", str(DEFAULT_REQUEST_PATH))),
            result_name=result_name,
            publisher=KubernetesResultPublisher(namespace=namespace),
        )
    except (OSError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
