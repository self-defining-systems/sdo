"""``kubectl`` against one lane's cluster, and the error every dev-swap step raises."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Any

CONTROLLER_JOB = "sdo-controller-run"
CONTROLLER_CONTAINER = "controller"
APPLICATION_ROOT = "/workspace/application"
KUBECTL_TIMEOUT_S = 25


class SwapError(RuntimeError):
    """A swap step cannot do its job on this lane; the message is the one-line reason a caller records."""


def run(args: list[str], what: str, timeout: float = 600, **kwargs: Any) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False, **kwargs)
    if proc.returncode != 0:
        raise SwapError(f"{what} failed ({proc.returncode}): {(proc.stderr or proc.stdout).strip()[-300:]}")
    return proc


@dataclass
class Kubectl:
    """kubectl against one lane's cluster: its kubeconfig file and context."""

    context: str
    kubeconfig: str = ""

    def cmd(self, *args: str) -> list[str]:
        out = ["kubectl"]
        if self.kubeconfig:
            out += ["--kubeconfig", self.kubeconfig]
        if self.context:
            out += ["--context", self.context]
        return [*out, *args]

    def get_json(self, *args: str) -> dict[str, Any] | None:
        """``kubectl <args> -o json`` as an object; None when kubectl fails (the object is not there)."""
        proc = subprocess.run(
            self.cmd(*args, "-o", "json"), capture_output=True, text=True, timeout=KUBECTL_TIMEOUT_S, check=False
        )
        if proc.returncode != 0:
            return None
        value = json.loads(proc.stdout)
        return value if isinstance(value, dict) else None

    def read_controller_pods(self, control: str) -> list[dict[str, Any]] | None:
        """The controller Job's pods; None when kubectl could not read them (which is not the same as no pods)."""
        pods = self.get_json("-n", control, "get", "pods", "-l", f"job-name={CONTROLLER_JOB}")
        return None if pods is None else list(pods.get("items") or [])

    def controller_pods(self, control: str) -> list[dict[str, Any]]:
        """The controller Job's pods; empty when they cannot be read (a poll then tries again)."""
        return self.read_controller_pods(control) or []

    def controller_suspended(self, control: str) -> bool | None:
        """Whether the controller Job is suspended; None when the Job could not be read."""
        job = self.get_json("-n", control, "get", "job", CONTROLLER_JOB)
        if job is None:
            return None
        return bool((job.get("spec") or {}).get("suspend"))

    def controller_ready(self, control: str) -> bool:
        for pod in self.controller_pods(control):
            if (pod.get("metadata") or {}).get("deletionTimestamp"):
                continue
            conditions = (pod.get("status") or {}).get("conditions") or []
            if any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions):
                return True
        return False

    def controller_pod(self, control: str) -> str:
        names = [
            p["metadata"]["name"]
            for p in self.controller_pods(control)
            if (p.get("status") or {}).get("phase") == "Running" and not p["metadata"].get("deletionTimestamp")
        ]
        if not names:
            raise SwapError(f"no running controller pod in {control}")
        return sorted(names)[-1]

    def exec_controller(self, control: str, script: str, timeout: float = 300) -> bytes:
        pod = self.controller_pod(control)
        proc = subprocess.run(
            self.cmd("-n", control, "exec", pod, "-c", CONTROLLER_CONTAINER, "--", "sh", "-c", script),
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        if proc.returncode != 0:
            raise SwapError(f"exec in {pod} failed ({proc.returncode}): {proc.stderr.decode(errors='replace')[-300:]}")
        return proc.stdout
