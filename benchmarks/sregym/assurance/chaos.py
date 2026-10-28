"""Chaos actions against SDO itself, run in a background thread during one incident.

Each action waits for its trigger (for example the responder pod running),
injects its fault into SDO's own machinery, and returns a one-line log of what
it did. Actions touch only the run's own kind cluster.
"""

from __future__ import annotations

import json
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import kubectl

if TYPE_CHECKING:
    from collections.abc import Callable

RESPONDER_SELECTOR = "app.kubernetes.io/name=sdo-responder"
CONTROLLER_SELECTOR = "job-name=sdo-controller-run"
PROBER_POD = "sdo-prober"


class ChaosError(RuntimeError):
    """A chaos trigger never fired, or its action failed."""


@dataclass
class ChaosContext:
    control_namespace: str
    namespace: str
    cluster: str
    stop: threading.Event = field(default_factory=threading.Event)
    log: list[str] = field(default_factory=list)

    def note(self, message: str) -> None:
        self.log.append(f"{time.strftime('%H:%M:%S')} {message}")

    def wait_for(self, predicate: Callable[[], bool], what: str, timeout: float = 900.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.stop.is_set():
                raise ChaosError(f"stopped before {what}")
            if predicate():
                return
            time.sleep(1.0)
        raise ChaosError(f"timed out waiting for {what}")


def _pods(namespace: str, selector: str) -> list[dict[str, Any]]:
    completed = kubectl(["get", "pods", "-l", selector, "-o", "json"], namespace=namespace, check=False)
    if completed.returncode != 0:
        return []
    items = json.loads(completed.stdout).get("items", [])
    return [item for item in items if isinstance(item, dict)]


def _running(pods: list[dict[str, Any]]) -> list[str]:
    return [str(pod["metadata"]["name"]) for pod in pods if pod.get("status", {}).get("phase") == "Running"]


def responder_running(ctx: ChaosContext) -> list[str]:
    return _running(_pods(ctx.control_namespace, RESPONDER_SELECTOR))


def kill_controller_mid_incident(ctx: ChaosContext) -> None:
    """Delete the controller pod while the responder works; its Job must recreate it and rejoin the incident."""

    ctx.wait_for(lambda: bool(responder_running(ctx)), "a running responder pod")
    time.sleep(5)
    pods = _running(_pods(ctx.control_namespace, CONTROLLER_SELECTOR))
    for pod in pods:
        kubectl(["delete", "pod", pod, "--wait=false"], namespace=ctx.control_namespace)
    ctx.note(f"deleted controller pod(s) {pods} while responder {responder_running(ctx)} ran")


def kill_responder_mid_mitigation(ctx: ChaosContext) -> None:
    """Delete the responder pod inside its pre-repair pause (the directive holds it there)."""

    ctx.wait_for(lambda: bool(responder_running(ctx)), "a running responder pod")
    time.sleep(20)
    pods = responder_running(ctx)
    for pod in pods:
        kubectl(["delete", "pod", pod, "--wait=false"], namespace=ctx.control_namespace)
    ctx.note(f"deleted responder pod(s) {pods}")


def kill_prober(ctx: ChaosContext) -> None:
    """Delete the synthetic-traffic prober while the responder works and must verify through it."""

    ctx.wait_for(lambda: bool(responder_running(ctx)), "a running responder pod")
    kubectl(["delete", "pod", PROBER_POD, "--wait=false"], namespace=ctx.control_namespace)
    ctx.note("deleted the prober pod")


def pause_apiserver(ctx: ChaosContext, seconds: float = 25.0) -> None:
    """Freeze this run's kind control-plane container (the API server) briefly, mid-incident."""

    ctx.wait_for(lambda: bool(responder_running(ctx)), "a running responder pod")
    time.sleep(10)
    container = f"{ctx.cluster}-control-plane"
    if not ctx.cluster.startswith("assure-"):
        raise ChaosError(f"refusing to pause {container}: not an assurance cluster")
    subprocess.run(["docker", "pause", container], check=True, capture_output=True)
    ctx.note(f"paused {container}")
    try:
        time.sleep(seconds)
    finally:
        subprocess.run(["docker", "unpause", container], check=True, capture_output=True)
        ctx.note(f"unpaused {container} after {seconds:.0f}s")


def heal_fault(ctx: ChaosContext) -> None:
    """Remove the deny-all NetworkPolicy while the responder waits, as a self-healing transient would."""

    ctx.wait_for(lambda: bool(responder_running(ctx)), "a running responder pod")
    time.sleep(5)
    completed = kubectl(["get", "networkpolicy", "-o", "json"], namespace=ctx.namespace, check=False)
    if completed.returncode != 0:
        raise ChaosError(f"cannot list NetworkPolicies: {completed.stderr.strip()[:300]}")
    names = [
        str(item["metadata"]["name"])
        for item in json.loads(completed.stdout).get("items", [])
        if not item.get("spec", {}).get("ingress") and not item.get("spec", {}).get("egress")
    ]
    if not names:
        raise ChaosError("no deny-all NetworkPolicy to heal")
    for name in names:
        kubectl(["delete", "networkpolicy", name, "--wait=true"], namespace=ctx.namespace)
    ctx.note(f"healed the fault without the responder: deleted NetworkPolicy {names}")


def concurrent_memory_commit(ctx: ChaosContext) -> None:
    """Commit a conflicting edit to the operational repository while the incident is open."""

    ctx.wait_for(lambda: bool(responder_running(ctx)), "a running responder pod")
    pods = _running(_pods(ctx.control_namespace, CONTROLLER_SELECTOR))
    if not pods:
        raise ChaosError("no running controller pod to commit through")
    script = (
        "set -e; cd /workspace/application; "
        "printf '\\nOperator note.\\n' >> .sdo/playbooks/health-objective/README.md; "
        "git -c user.name=operator -c user.email=operator@example.com commit -qam "
        "'operator: concurrent edit to the health-objective playbook'; git rev-parse HEAD"
    )
    completed = kubectl(
        ["exec", pods[0], "-c", "controller", "--", "sh", "-c", script], namespace=ctx.control_namespace, check=False
    )
    ctx.note(f"concurrent commit rc={completed.returncode}: {(completed.stdout + completed.stderr).strip()[:300]}")


CHAOS: dict[str, Callable[[ChaosContext], None]] = {
    "kill-controller": kill_controller_mid_incident,
    "kill-responder": kill_responder_mid_mitigation,
    "kill-prober": kill_prober,
    "pause-apiserver": pause_apiserver,
    "concurrent-commit": concurrent_memory_commit,
    "heal-fault": heal_fault,
}


class ChaosThread(threading.Thread):
    def __init__(self, name: str, ctx: ChaosContext) -> None:
        if name not in CHAOS:
            raise ValueError(f"unknown chaos action {name!r}; expected one of {sorted(CHAOS)}")
        super().__init__(name=f"chaos-{name}", daemon=True)
        self.action = CHAOS[name]
        self.ctx = ctx
        self.error: str | None = None

    def run(self) -> None:
        try:
            self.action(self.ctx)
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"

    def finish(self, timeout: float = 120.0) -> None:
        self.ctx.stop.set()
        self.join(timeout)
