"""Fail-closed benchmark transaction for a persistent lane, before injection.

The lane keeps its SDO install; the transaction restores the application, then swaps the install's state for a fresh
install's at the row's planned memory (``reseed``: the controller process is replaced by design) and binds fresh
baseline observations to the accepted revision. No benchmark grade grants admission.
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

from benchmarks.sregym.fastloop.resident_start import ResidentStartMismatch

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path


#: How much of a failure's text (one line) a receipt keeps.
REFUSAL_DETAIL_CHARS = 300


class WarmLaneError(RuntimeError):
    """Neither warm reuse nor the ordinary full reset established a usable lane."""


class WarmLaneRefusal(ValueError):
    """The lane differs from what a warm start needs; the message is this package's own text, safe to record."""


class WarmLaneBusy(WarmLaneError):
    """An earlier incident still owns this lane; even fallback must not destroy it."""


@contextmanager
def own_lane(directory: Path, lane: str) -> Iterator[None]:
    """One row caller per host/lane; keep the lock inode after releasing it."""
    if not lane or not directory.is_absolute():
        raise ValueError("warm ownership needs an absolute shared lock directory and lane identity")
    directory.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(lane.encode()).hexdigest()
    with (directory / f"warm-lane-{key}.lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise WarmLaneBusy("another run owns this lane") from None
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
    def reidentify(self) -> LaneIdentity:
        """The running controller and standby now, without the checks ``inspect`` makes of an idle lane."""
        ...

    def reseed(self) -> None:
        """Swap the install's state for a fresh install's at the planned memory; replaces the controller process."""
        ...

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
    #: What differed from a fresh install, when that is why the lane was not reused: this package's own text, never an
    #: external error's.
    violations: tuple[str, ...] = ()
    #: The failure's text on one line, truncated, so a fallback is diagnosable (kubectl stderr and our own refusals).
    detail: str = ""


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
        if run("pre_reseed_identity", lane.reidentify) != identity:
            raise ValueError("lane identity changed before the reseed")
        # What the replaced controller does is after this instant: its start-up, onboarding and evaluations are the
        # row's own, and the fresh-install comparison holds them to it.
        boundary = now()
        if boundary.tzinfo is None:
            raise ValueError("baseline boundary must be timezone-aware")
        run("reseed", lane.reseed)
        # The controller process was replaced by the reseed; from here its new identity is the one that must hold.
        reidentified: LaneIdentity = run("reidentify", lane.reidentify)
        identity = reidentified
        revision = run("adopt", lane.adopt)
        if revision != reidentified.accepted_revision:
            raise ValueError("accepted revision changed during reseed")
        proof = run("prove", lambda: lane.prove(boundary, revision))
        if proof.get("passed") is not True:
            raise ValueError("fresh baseline proof did not pass")
        current = run("confirm_identity", lane.reidentify)
        if current != identity:
            raise ValueError("lane identity changed during warm transaction")
    except WarmLaneBusy:
        _publish(evidence, WarmReceipt(False, None, None, timings, "prior_incident_open"))
        raise
    except Exception as exc:
        reason = f"{phase}:{getattr(exc, 'reason', None) or type(exc).__name__}"
        try:
            run("full_reset", lane.full_reset)
        except Exception:
            _publish(evidence, WarmReceipt(False, None, None, timings, reason + ";full_reset_failed"))
            raise WarmLaneError(f"eval: warm preparation failed ({reason}); full reset failed") from None
        violations = tuple(exc.violations) if isinstance(exc, ResidentStartMismatch) else ()
        detail = " ".join(str(exc).split())[:REFUSAL_DETAIL_CHARS]
        receipt = WarmReceipt(False, None, None, timings, reason, violations, detail)
    else:
        receipt = WarmReceipt(True, identity, boundary.isoformat(), timings)
    _publish(evidence, receipt)
    return receipt


def _publish(path: Path, receipt: WarmReceipt) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"schema": "sdo.warm-lane/v1", **asdict(receipt)}, indent=2) + "\n")
    temporary.replace(path)
