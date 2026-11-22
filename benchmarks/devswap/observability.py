"""The control namespace's observability stack (Prometheus and Jaeger) on a lane: replace its pods for a fresh store.

Their data lives in emptyDir volumes, so replacing the pods empties it. Benchmark-neutral: it names only the two
Deployments SDO installs in its control namespace.
"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from benchmarks.devswap.kube import run

if TYPE_CHECKING:
    from benchmarks.devswap.kube import Kubectl

#: The control namespace's observability Deployments: their data lives in emptyDir volumes.
OBSERVABILITY_DEPLOYMENTS = ("sdo-prometheus", "sdo-jaeger")
OBSERVABILITY_ROLLOUT_WAIT_S = 240


def _present_observability(kube: Kubectl, control: str) -> list[str]:
    present = []
    for name in OBSERVABILITY_DEPLOYMENTS:
        found = subprocess.run(
            kube.cmd("-n", control, "get", "deployment", name, "-o", "name"),
            capture_output=True,
            timeout=60,
            check=False,
        )
        if found.returncode == 0:
            present.append(name)
    return present


def restart_observability(kube: Kubectl, control: str) -> None:
    """Start replacing the control namespace's Prometheus and Jaeger pods so their emptyDir data (TSDB, WAL, traces)
    is gone; ``await_observability`` waits for the rollouts. Reset puts the app back to the golden copy; the
    observability stack kept every earlier run, and on a reused lane Prometheus (1 Gi) was OOM-killed and
    crash-looped on WAL replay. The restart overlaps the app's readiness wait (minutes), so it costs the reset
    nothing. A lane without them is left as it is."""
    for name in _present_observability(kube, control):
        run(kube.cmd("-n", control, "rollout", "restart", f"deployment/{name}"), f"restart {name}", 60)


def await_observability(kube: Kubectl, control: str) -> None:
    for name in _present_observability(kube, control):
        run(
            kube.cmd(
                "-n", control, "rollout", "status", f"deployment/{name}", f"--timeout={OBSERVABILITY_ROLLOUT_WAIT_S}s"
            ),
            f"wait for {name}",
            OBSERVABILITY_ROLLOUT_WAIT_S + 30,
        )
