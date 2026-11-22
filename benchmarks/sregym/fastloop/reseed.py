"""Swap a resident SDO install's state for a fresh install's, in place, at a row's planned memory.

A lane keeps its SDO install (control namespace, repository volume, credentials, RBAC, transport, observability) across
rows. What a row starts from is state: the accepted memory, the controller's persisted state and onboarding, sessions,
sealed evidence and the controller process that holds them in memory. This module orders the swap; the Kubernetes work
of each step belongs to ``WarmEnvironment``.

The order is the contract:

1. the earlier row's sealed evidence is delivered before anything is deleted (it lives on the volume the swap rewrites);
2. the controller (and its responder standby) stops before any state under it changes: the process caches the accepted
   commit and its state, so state moved behind a live controller is unsafe;
3. every control-state record is retired (the host copy of the accepted repository is already at the planned commit:
   the transaction put it there before restoring the application, which restores from that source);
4. the control namespace's Prometheus and Jaeger get fresh stores;
5. the controller is installed again from the row's own configuration (Go ``install-existing``: a new Job with this
   row's evidence run, the volume seeded from the host copy), waited for, and recorded as the lane's controller.

A failure raises ``ReseedError`` naming the phase; the controller may then be stopped, and the caller's only way on is
the full reset, which deletes the control namespace anyway.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

PHASES = (
    "settle_evidence",
    "read_head",
    "stop_controller",
    "retire_control_state",
    "fresh_observability",
    "install_controller",
    "wait_started",
    "record_controller",
    "await_observability",
)


class ReseedError(RuntimeError):
    """A reseed step failed; ``phase`` names it and ``reason`` is ``<phase>:<error type>`` (never the error's text)."""

    def __init__(self, phase: str, cause: BaseException) -> None:
        super().__init__(f"eval: reseed failed in {phase}: {type(cause).__name__}")
        self.phase = phase
        self.reason = f"{phase}:{type(cause).__name__}"


@dataclass(frozen=True)
class ReseedSteps:
    """One callable per phase of ``PHASES`` (tests substitute them)."""

    settle_evidence: Callable[[], object]
    read_head: Callable[[], str]
    stop_controller: Callable[[str], object]
    retire_control_state: Callable[[], object]
    fresh_observability: Callable[[], object]
    install_controller: Callable[[], object]
    wait_started: Callable[[], object]
    record_controller: Callable[[], object]
    await_observability: Callable[[], object]


def reseed_lane(steps: ReseedSteps, *, monotonic: Callable[[], float] = time.monotonic) -> dict[str, float]:
    """Run the swap in order and return the seconds each phase took."""
    seconds: dict[str, float] = {}
    head = ""

    def phase(name: str, operation: Callable[[], Any]) -> Any:
        started = monotonic()
        try:
            return operation()
        except Exception as exc:
            logger.warning("reseed: %s failed: %s", name, exc)
            raise ReseedError(name, exc) from exc
        finally:
            seconds[name] = round(monotonic() - started, 3)

    phase("settle_evidence", steps.settle_evidence)
    head = phase("read_head", steps.read_head)
    phase("stop_controller", lambda: steps.stop_controller(head))
    phase("retire_control_state", steps.retire_control_state)
    phase("fresh_observability", steps.fresh_observability)
    phase("install_controller", steps.install_controller)
    phase("wait_started", steps.wait_started)
    phase("record_controller", steps.record_controller)
    phase("await_observability", steps.await_observability)
    logger.info("reseed: %s", ", ".join(f"{name} {value:g}s" for name, value in seconds.items()))
    return seconds
