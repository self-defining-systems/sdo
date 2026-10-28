"""Positive Kubernetes evidence for a writer that cannot release itself."""

from __future__ import annotations

import json
import os
import ssl
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from libs.agent_cli.session_ownership import SessionHandoffError, WorkerIdentity, WorkerTermination
from sdo.agents.responder.job import SERVICE_ACCOUNT_ROOT

if TYPE_CHECKING:
    from collections.abc import Callable

WORKER_IDENTITY_ENV = ("SDO_WORKER_NAMESPACE", "SDO_WORKER_POD_NAME", "SDO_WORKER_POD_UID", "SDO_WORKER_CONTAINER")


def worker_identity() -> WorkerIdentity | None:
    values = [os.environ.get(key, "") for key in WORKER_IDENTITY_ENV]
    if not any(values):
        return None
    try:
        return WorkerIdentity(*values)
    except (TypeError, ValueError) as exc:
        raise SessionHandoffError("worker Pod identity is incomplete or invalid") from exc


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


class KubernetesWriterObserver:
    """GET the recorded Pod; API uncertainty and stale status never grant release."""

    def __init__(self, *, read_pod: Callable[[WorkerIdentity, float], dict[str, Any] | None] | None = None) -> None:
        self.read_pod = read_pod or self._read_pod

    def __call__(self, worker: WorkerIdentity, timeout: float) -> WorkerTermination | None:
        if timeout <= 0:
            return None
        try:
            pod = self.read_pod(worker, min(2.0, timeout))
            observed_at = datetime.now(UTC).isoformat()
            # None means the exact named GET returned 404, not transport failure.
            # The immutable UID was captured by the owning Pod before its turn.
            if pod is None:
                return WorkerTermination(worker, "pod_gone", "NotFound", observed_at)
            metadata = pod.get("metadata", {})
            if (metadata.get("namespace"), metadata.get("name"), metadata.get("uid")) != (
                worker.namespace,
                worker.pod_name,
                worker.pod_uid,
            ):
                return None
            status = pod.get("status", {})
            containers = status.get("containerStatuses", [])
            if status.get("phase") in {"Failed", "Succeeded"}:
                if any(c.get("state", {}).get(k) is not None for c in containers for k in ("running", "waiting")):
                    return None
                return WorkerTermination(
                    worker,
                    "pod_terminated",
                    status["phase"],
                    observed_at,
                    resource_version=metadata.get("resourceVersion", ""),
                )
            if pod.get("spec", {}).get("restartPolicy") != "Never":
                return None
            matches = [c for c in containers if c.get("name") == worker.container]
            if len(matches) != 1:
                return None
            state = matches[0].get("state", {})
            terminated = state.get("terminated")
            if set(state) != {"terminated"} or not isinstance(terminated, dict) or not terminated.get("finishedAt"):
                return None
            return WorkerTermination(
                worker,
                "container_terminated",
                terminated.get("reason") or "Terminated",
                observed_at,
                finished_at=terminated["finishedAt"],
                exit_code=terminated.get("exitCode"),
                resource_version=metadata.get("resourceVersion", ""),
            )
        except (OSError, ValueError, TypeError, AttributeError, KeyError):
            return None

    @staticmethod
    def _read_pod(worker: WorkerIdentity, timeout: float) -> dict[str, Any] | None:
        host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        token = (SERVICE_ACCOUNT_ROOT / "token").read_text().strip()
        context = ssl.create_default_context(cafile=str(SERVICE_ACCOUNT_ROOT / "ca.crt"))
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=context), _NoRedirect())
        request = urllib.request.Request(
            f"https://{host}:{port}/api/v1/namespaces/{worker.namespace}/pods/{worker.pod_name}",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with opener.open(request, timeout=timeout) as response:
                body = response.read((1 << 20) + 1)
        except urllib.error.HTTPError as exc:
            exc.close()
            if exc.code == 404:
                return None
            raise
        if len(body) > 1 << 20:
            raise ValueError("worker Pod status exceeds observation limit")
        pod = json.loads(body)
        if not isinstance(pod, dict):
            raise ValueError("worker Pod status is not an object")
        return pod
