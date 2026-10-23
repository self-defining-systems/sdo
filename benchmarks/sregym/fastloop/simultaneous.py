"""Simultaneous composite injection: every fault lands before the controller observes the namespace.

The default persistent stage resumes the controller, waits for an all-clear baseline, then injects.
For a composite the faults then appear over several seconds, and the controller dispatches about a
second after the first finding, so learned detectors for the later faults can only fire after
dispatch. :class:`InjectBeforeResumeOps` instead holds the controller in maintenance (paused) while
the whole composite is injected, then resumes it. A resume makes the controller re-list the
namespace and evaluate every detector once, so all faults are observed in the same first
evaluation. This is harness-only; the production controller is unchanged.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import Clock, wait_for_maintenance_ack

if TYPE_CHECKING:
    from benchmarks.sregym.adapter import ClusterOps

PauseWaiter = Callable[["ClusterOps", str, str], None]


def _wait_for_pause(ops: ClusterOps, control_namespace: str, generation: str) -> None:
    wait_for_maintenance_ack(ops, control_namespace, generation, Clock())


class InjectBeforeResumeOps:
    """A :class:`ClusterOps` that injects while the controller is paused, then resumes it."""

    def __init__(self, inner: ClusterOps, *, wait_for_pause: PauseWaiter = _wait_for_pause) -> None:
        self._inner = inner
        self._wait_for_pause = wait_for_pause

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def inject_after_resume(
        self, control_namespace: str, generation: str, inject: Callable[[], None]
    ) -> dict[str, float]:
        paused_generation = f"{generation}-presim"
        self._inner.set_maintenance(control_namespace, paused=True, generation=paused_generation)
        self._wait_for_pause(self._inner, control_namespace, paused_generation)
        inject()
        self._inner.set_maintenance(control_namespace, paused=False, generation=generation)
        return {"injection_before_resume": 1.0}
