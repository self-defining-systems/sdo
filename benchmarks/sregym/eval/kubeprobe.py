"""User-journey probe through the Kubernetes API service proxy (``kube:<ns>/<svc>:<port>/<path>``).

The lane's Services are cluster-internal, so the host prober asks the API server to proxy each request to a ready
endpoint of the Service. That needs only the lane kubeconfig, survives pod restarts (no port-forward) and fails like a
user would: an upstream 5xx or an empty endpoint set is an unhealthy sample.
"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from benchmarks.sregym.eval.health import ProbeResult, ProbeUnavailable

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

PREFIX = "kube:"
#: kubectl could not reach the API server at all: a harness/host fault (load), not evidence about the app.
_CLIENT_SIDE = ("Unable to connect to the server", "TLS handshake timeout", "connection refused", "no route to host")


class ProbeInfraError(ProbeUnavailable):
    """The probe itself could not run (API server unreachable from the host); the sample is dropped, not unhealthy."""


def parse(spec: str) -> tuple[str, str, str, str, str]:
    """``kube:ns/svc:port/path[|expect=text]`` -> (namespace, service, port, path, expected body substring)."""
    if not spec.startswith(PREFIX):
        raise ValueError(f"not a kube probe spec: {spec!r}")
    spec, _, expected = spec.partition("|expect=")
    namespace, _, rest = spec[len(PREFIX) :].partition("/")
    service_port, _, path = rest.partition("/")
    service, _, port = service_port.partition(":")
    if not (namespace and service and port):
        raise ValueError(f"kube probe spec needs ns/svc:port/path, got {spec!r}")
    return namespace, service, port, path, expected


def kube_probe(specs: tuple[str, ...], kubeconfig: Path, *, timeout_seconds: float = 4.0) -> Callable[[], ProbeResult]:
    targets = [parse(spec) for spec in specs]

    def probe() -> ProbeResult:
        for namespace, service, port, path, expected in targets:
            raw = f"/api/v1/namespaces/{namespace}/services/{service}:{port}/proxy/{path}"
            try:
                completed = subprocess.run(
                    [
                        "kubectl",
                        "--kubeconfig",
                        str(kubeconfig),
                        f"--request-timeout={int(timeout_seconds)}s",
                        "get",
                        "--raw",
                        raw,
                    ],
                    capture_output=True,
                    text=True,
                    timeout=timeout_seconds + 2,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                raise ProbeInfraError(f"kubectl timed out for {service}:{port}") from None
            if completed.returncode != 0:
                if any(marker in completed.stderr for marker in _CLIENT_SIDE):
                    raise ProbeInfraError(completed.stderr.strip()[-160:])
                return ProbeResult(False, f"{service}:{port}/{path}: {completed.stderr.strip()[-160:]}")
            if expected and expected not in completed.stdout:
                return ProbeResult(False, f"{service}:{port}/{path}: body lacks {expected!r}")
        return ProbeResult(True)

    return probe
