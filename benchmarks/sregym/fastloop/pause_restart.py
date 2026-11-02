"""Restart the controller inside the injection pause (the P51 trigger).

Every injection holds the controller in a baseline-preserving maintenance pause
while the fault is written. A controller that starts (or restarts) inside that
window has never run its informers, so it must start them when resumed. The
wrapper deletes the controller Pod after the pause is acknowledged and before the
fault is injected; the Job recreates it, and the harness then resumes it.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, kubectl, wait_for

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from benchmarks.sregym.adapter import ClusterOps

RESTART_TIMEOUT_SECONDS = wait_for(WaitKind.ROLLOUT)


class PauseRestartError(RuntimeError):
    """The controller did not restart inside the pause; the scenario would prove nothing."""


class RestartControllerInPauseOps:
    """A :class:`ClusterOps` that restarts the controller Pod after the injection pause begins."""

    def __init__(
        self,
        inner: ClusterOps,
        evidence: Path,
        *,
        timeout_seconds: float = RESTART_TIMEOUT_SECONDS,
        kubectl_runner: Callable[..., Any] = kubectl,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._inner = inner
        self._evidence = evidence
        self._timeout = timeout_seconds
        self._kubectl = kubectl_runner
        self._sleep = sleep
        self._monotonic = monotonic

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]:
        def restart_then_inject() -> None:
            self._restart(control_namespace, generation)
            inject()

        return self._inner.inject_after_resume(control_namespace, generation, restart_then_inject)

    def _restart(self, control_namespace: str, generation: str) -> None:
        before = self._inner.controller_pod(control_namespace)
        if before is None:
            raise PauseRestartError(f"no controller Pod in {control_namespace!r} to restart inside the pause")
        deleted = self._kubectl(
            ["delete", "pod", before.name, "--wait=true", "--request-timeout=120s"],
            namespace=control_namespace,
            check=False,
        )
        if deleted.returncode != 0:
            raise PauseRestartError(f"cannot delete controller Pod {before.name}: {deleted.stderr.strip()[:300]}")
        deadline = self._monotonic() + self._timeout
        while True:
            after = self._inner.controller_pod(control_namespace)
            if after is not None and after.uid != before.uid:
                ready = self._kubectl(
                    ["wait", "--for=condition=Ready", f"pod/{after.name}", "--timeout=120s"],
                    namespace=control_namespace,
                    check=False,
                )
                if ready.returncode == 0:
                    break
            if self._monotonic() >= deadline:
                raise PauseRestartError(f"controller Pod {before.name} was not replaced within {self._timeout:.0f}s")
            self._sleep(1.0)
        # The new process reads the paused maintenance state; give it a moment to settle before injection.
        self._sleep(5.0)
        row = {
            "control_namespace": control_namespace,
            "generation": generation,
            "old_pod": before.name,
            "old_uid": before.uid,
            "new_pod": after.name,
            "new_uid": after.uid,
            "restarted_at": datetime.now(timezone.utc).isoformat(),
        }
        self._evidence.parent.mkdir(parents=True, exist_ok=True)
        with self._evidence.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
