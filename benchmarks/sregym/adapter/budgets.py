"""Every eval wait, derived from one place.

An eval wait that is shorter than the SDO work it waits for loses rows (catalog patterns P20, P25, P26,
P38, P41, P48, P50). So no wait in ``fastloop`` or ``adapter`` is a seconds literal. Each is
``wait_for(kind, queue_len)``: the bound of the work named by ``kind``, times the queue ahead of it,
plus the kind's margin. The clock is only a backstop; a wait that polls an owner must still end on that
owner's terminal state.

The SDO bounds below are the values the harness configures or that production fixes in Go. Production
does not publish them, so ``SdoBounds`` is the one place the eval states them.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class SdoBounds:
    """The SDO limits every SDO-owned wait is built from."""

    #: Controller ``--response-timeout``; also the reflection's frozen deadline (run.go,
    #: registered_reflection.go). The harness passes it, so ``--timeout`` of a row overrides it.
    response_timeout_seconds: float = 1800.0
    #: Controller ``--verification-timeout``: one health verification sample (native_operator.py).
    verification_timeout_seconds: float = 300.0
    #: Closure and queued-reflection retries back off 2+4+8+16+32+60+60 s before the work is abandoned
    #: (closure_retry.go, reflection_queue.go). Not published by production.
    retry_backoff_seconds: float = 182.0
    #: Commit, cleanup and detector-rollout handoff after the last attempt. Eval assumption.
    closeout_seconds: float = 120.0

    def __post_init__(self) -> None:
        for name in ("response_timeout_seconds", "verification_timeout_seconds"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive, got {getattr(self, name)}")
        for name in ("retry_backoff_seconds", "closeout_seconds"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must not be negative, got {getattr(self, name)}")

    @property
    def reflection_attempt_seconds(self) -> float:
        """Worst case to finish or abandon one queued reflection (the FIFO runs one at a time)."""
        return self.response_timeout_seconds + self.retry_backoff_seconds + self.closeout_seconds


DEFAULT_BOUNDS = SdoBounds()


class WaitKind(Enum):
    """What a wait is for. Each member's bound is stated once, in ``bound``."""

    # SDO-owned work: scales with the response timeout and with the queue ahead of it.
    RESPONSE = "response"
    REFLECTION_ATTEMPT = "reflection_attempt"
    #: Verified closure to the acknowledgment of its reflection (the response and one retry round).
    REFLECTION_DRAIN = "reflection_drain"
    #: Injection to the end of a graded incident: response, reflection-free verification and a retry.
    INCIDENT_VERIFICATION = "incident_verification"
    COMPOSITE_DEADLINE = "composite_deadline"
    VERIFICATION_SAMPLE = "verification_sample"
    # Harness-owned operations. Fixed backstops; none of them waits on SDO work.
    POLL = "poll"
    POLL_FAST = "poll_fast"
    SAMPLE = "sample"
    SNAPSHOT_READ = "snapshot_read"
    PROBE = "probe"
    COMMAND = "command"
    MINUTE = "minute"
    SLOW_COMMAND = "slow_command"
    ROLLOUT = "rollout"
    OPERATION = "operation"
    LONG_OPERATION = "long_operation"
    CLUSTER_OP = "cluster_op"
    DEPLOY_OP = "deploy_op"
    IDLE = "idle"
    STABLE = "stable"
    HELPER_LIFETIME = "helper_lifetime"
    REFRESH_SETTLE = "refresh_settle"
    CLOSEOUT_SETTLE = "closeout_settle"


_FIXED_SECONDS: dict[WaitKind, float] = {
    WaitKind.POLL_FAST: 1.0,
    WaitKind.SAMPLE: 2.0,
    WaitKind.SNAPSHOT_READ: 15.0,
    WaitKind.POLL: 5.0,
    WaitKind.PROBE: 10.0,
    WaitKind.COMMAND: 30.0,
    WaitKind.MINUTE: 60.0,
    WaitKind.SLOW_COMMAND: 120.0,
    WaitKind.ROLLOUT: 300.0,
    WaitKind.OPERATION: 600.0,
    WaitKind.LONG_OPERATION: 900.0,
    WaitKind.CLUSTER_OP: 1800.0,
    WaitKind.DEPLOY_OP: 2400.0,
    WaitKind.IDLE: 600.0,
    WaitKind.STABLE: 20.0,
    WaitKind.HELPER_LIFETIME: 120.0,
    WaitKind.REFRESH_SETTLE: 45.0,
    WaitKind.CLOSEOUT_SETTLE: 120.0,
}

#: Added after the queue product, so a wait never ends exactly at the owner's own bound.
_MARGIN_SECONDS: dict[WaitKind, float] = {}


def bound(kind: WaitKind, bounds: SdoBounds = DEFAULT_BOUNDS) -> float:
    """The longest SDO (or the harness operation) takes for one unit of the work ``kind`` names."""
    if kind is WaitKind.RESPONSE:
        return bounds.response_timeout_seconds
    if kind is WaitKind.REFLECTION_ATTEMPT:
        return bounds.reflection_attempt_seconds
    if kind is WaitKind.REFLECTION_DRAIN:
        return 2 * bounds.response_timeout_seconds
    if kind is WaitKind.INCIDENT_VERIFICATION:
        return 2 * bounds.response_timeout_seconds + bounds.verification_timeout_seconds
    if kind is WaitKind.COMPOSITE_DEADLINE:
        return bounds.response_timeout_seconds + 600.0
    if kind is WaitKind.VERIFICATION_SAMPLE:
        return bounds.verification_timeout_seconds
    return _FIXED_SECONDS[kind]


def wait_for(kind: WaitKind, queue_len: int = 1, *, bounds: SdoBounds = DEFAULT_BOUNDS) -> float:
    """``bound(kind) * max(1, queue_len) + margin``: the wait that cannot end before the owner does."""
    if queue_len < 0:
        raise ValueError(f"queue_len must not be negative, got {queue_len}")
    return bound(kind, bounds) * max(1, queue_len) + _MARGIN_SECONDS.get(kind, 0.0)
