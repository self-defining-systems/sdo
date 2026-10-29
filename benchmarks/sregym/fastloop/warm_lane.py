"""Fail-closed benchmark transaction for a persistent lane, before injection.

A successful transaction retains both process identities and binds fresh baseline
observations to the accepted revision. No benchmark grade grants admission.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import re
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path


class WarmLaneError(RuntimeError):
    """Neither warm reuse nor the ordinary full reset established a usable lane."""


class WarmLaneBusy(WarmLaneError):
    """An earlier incident still owns this lane; even fallback must not destroy it."""


@contextmanager
def own_lane(directory: Path, lane: str) -> Iterator[None]:
    """One warm caller per host/lane; keep the lock inode after releasing it."""
    if not lane or not directory.is_absolute():
        raise ValueError("warm ownership needs an absolute shared lock directory and lane identity")
    directory.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(lane.encode()).hexdigest()
    with (directory / f"warm-lane-{key}.lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise WarmLaneBusy("another warm run owns this lane") from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


@dataclass(frozen=True)
class LaneIdentity:
    controller_uid: str
    standby_uid: str
    accepted_revision: str

    def __post_init__(self) -> None:
        if not self.controller_uid or not self.standby_uid:
            raise ValueError("warm lane requires controller and standby identities")
        if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", self.accepted_revision):
            raise ValueError("warm lane requires a full accepted revision")


class Lane(Protocol):
    def inspect(self) -> LaneIdentity: ...
    def pause(self) -> None: ...
    def reset_changed(self) -> None: ...
    def adopt(self) -> str: ...
    def prove(self, not_before: datetime, revision: str) -> dict[str, Any]: ...
    def full_reset(self) -> None: ...


@dataclass(frozen=True)
class WarmReceipt:
    reused: bool
    identity: LaneIdentity | None
    baseline_not_before: str | None
    phase_seconds: dict[str, float]
    fallback_reason: str | None = None


def prepare_lane(
    lane: Lane,
    evidence: Path,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    monotonic: Callable[[], float] = time.monotonic,
) -> WarmReceipt:
    """Prepare one row; fallback performs full reset, whose ordinary admission still follows.

    This function never injects. A reset or proof failure cannot be mistaken for a
    ready receipt. Errors retain boundary/type only: external exceptions may carry
    credential-bearing command bodies.
    """
    timings: dict[str, float] = {}
    identity: LaneIdentity | None = None
    boundary: datetime | None = None
    phase = "inspect"

    def run(name: str, operation: Callable[[], Any]) -> Any:
        nonlocal phase
        phase = name
        started = monotonic()
        try:
            return operation()
        finally:
            timings[name] = monotonic() - started

    try:
        identity = run("inspect", lane.inspect)
        if not isinstance(identity, LaneIdentity):
            raise TypeError("lane inspection returned no identity")
        run("pause", lane.pause)
        run("reset", lane.reset_changed)
        revision = run("adopt", lane.adopt)
        if revision != identity.accepted_revision:
            raise ValueError("accepted revision changed during reset")
        boundary = now()
        if boundary.tzinfo is None:
            raise ValueError("baseline boundary must be timezone-aware")
        proof = run("prove", lambda: lane.prove(boundary, revision))
        if proof.get("passed") is not True:
            raise ValueError("fresh baseline proof did not pass")
        current = run("confirm_identity", lane.inspect)
        if current != identity:
            raise ValueError("lane identity changed during warm transaction")
    except WarmLaneBusy:
        _publish(evidence, WarmReceipt(False, None, None, timings, "prior_incident_open"))
        raise
    except Exception as exc:
        reason = f"{phase}:{type(exc).__name__}"
        try:
            run("full_reset", lane.full_reset)
        except Exception:
            _publish(evidence, WarmReceipt(False, None, None, timings, reason + ";full_reset_failed"))
            raise WarmLaneError(f"eval: warm preparation failed ({reason}); full reset failed") from None
        receipt = WarmReceipt(False, None, None, timings, reason)
    else:
        receipt = WarmReceipt(True, identity, boundary.isoformat(), timings)
    _publish(evidence, receipt)
    return receipt


def _publish(path: Path, receipt: WarmReceipt) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"schema": "sdo.warm-lane/v1", **asdict(receipt)}, indent=2) + "\n")
    temporary.replace(path)
