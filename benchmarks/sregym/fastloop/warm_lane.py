"""Fail-closed benchmark transaction for a persistent lane, before injection.

The lane keeps its SDO install; the transaction restores the application and swaps the install's state for a fresh
install's at the row's planned memory (``reseed``: the controller process is replaced by design), then binds fresh
baseline observations to the accepted revision. No benchmark grade grants admission.

The restore touches only the application namespace(s) and the reseed only the SDO control namespace and its volume, so
after the pause the two run concurrently. Their one dependency is the new controller's start: its onboarding records the
application's configuration and every judgement it makes after the boundary must be of this row's application, so the
reseed installs the controller (and the transaction takes the boundary) only once the restore's writes are done and its
workloads are Ready. What follows in the restore (the sustained-health and traffic verification) only observes the
application and overlaps the controller's start; the transaction joins both before the proof. The critical path is
``pause -> restore writes -> install -> started -> prove``, with the old controller's stop and state retirement and the
restore's verification beside it.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import re
import threading
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
    def reset_changed(self, restored: Callable[[], None] | None = None) -> None:
        """Restore the application; call ``restored`` once its writes are done and its workloads Ready (what follows
        only observes it). ``restored`` raises when the transaction already failed elsewhere: stop there."""
        ...

    def reidentify(self) -> LaneIdentity:
        """The running controller and standby now, without the checks ``inspect`` makes of an idle lane."""
        ...

    def reseed(self, application_restored: Callable[[], None] | None = None) -> None:
        """Swap the install's state for a fresh install's at the planned memory; replaces the controller process.

        ``application_restored`` is called right before the new controller is installed: it returns once the restore's
        writes are done (and raises when the restore failed)."""
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
        phase = "reset"
        overlapped = _restore_and_reseed(lane, identity, timings, now=now, monotonic=monotonic)
        if isinstance(overlapped, tuple):
            phase, failure = overlapped
            raise failure
        boundary = overlapped
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


class _Abandoned(Exception):
    """The other branch of the overlapped restore and reseed failed: this one stops at its next safe point."""


def _restore_and_reseed(
    lane: Lane,
    identity: LaneIdentity,
    timings: dict[str, float],
    *,
    now: Callable[[], datetime],
    monotonic: Callable[[], float],
) -> datetime | tuple[str, BaseException]:
    """Restore the application and reseed the install concurrently (see the module docstring for the dependency).

    Returns the baseline boundary, taken after the restore's writes and before the new controller's install, or the
    first failure as ``(phase, error)`` once both branches have stopped: a fallback never runs beside a live branch.
    Timings: ``reset`` and ``reseed`` per branch, ``reset.restore`` (to the restored signal), ``reset.verify``,
    ``pre_reseed_identity``, ``reseed.await_application`` (the reseed waiting for the restore) and
    ``reset_and_reseed`` (the wall of both).
    """
    restored, failed, lock = threading.Event(), threading.Event(), threading.Lock()
    failures: list[tuple[str, BaseException]] = []
    boundaries: list[datetime] = []
    started = monotonic()

    def fail(phase: str, exc: BaseException) -> None:
        with lock:
            if not isinstance(exc, _Abandoned):
                failures.append((phase, exc))
            failed.set()
            restored.set()  # a reseed waiting for the application stops waiting

    def mark_restored() -> None:
        with lock:
            if failed.is_set():
                raise _Abandoned("the reseed failed")
            if not restored.is_set():
                timings["reset.restore"] = monotonic() - started
                restored.set()

    def application_restored() -> None:
        waited = monotonic()
        restored.wait()
        timings["reseed.await_application"] = monotonic() - waited
        if failed.is_set():
            raise _Abandoned("the restore failed")
        # What the replaced controller does is after this instant: its start-up, onboarding and evaluations are the
        # row's own, of the restored application, and the fresh-install comparison holds them to it.
        boundary = now()
        if boundary.tzinfo is None:
            raise ValueError("baseline boundary must be timezone-aware")
        boundaries.append(boundary)

    def restore() -> None:
        try:
            lane.reset_changed(mark_restored)
            mark_restored()  # a restore that signalled nothing is restored when it returns
        except BaseException as exc:  # noqa: BLE001 - joined and re-raised by the transaction
            fail("reset", exc)
        finally:
            timings["reset"] = monotonic() - started
            if "reset.restore" in timings:
                timings["reset.verify"] = timings["reset"] - timings["reset.restore"]

    def reseed() -> None:
        phase = "pre_reseed_identity"
        try:
            checked = monotonic()
            try:
                current = lane.reidentify()
            finally:
                timings["pre_reseed_identity"] = monotonic() - checked
            # The controller about to be stopped is the one inspected; nothing replaced it since.
            if current != identity:
                raise ValueError("lane identity changed before the reseed")
            if failed.is_set():
                return  # the restore already failed: leave the controller for the fallback
            phase = "reseed"
            reseeding = monotonic()
            try:
                lane.reseed(application_restored)
            finally:
                timings["reseed"] = monotonic() - reseeding
            if not boundaries:
                raise ValueError("the reseed installed the controller without waiting for the restored application")
        except BaseException as exc:  # noqa: BLE001 - joined and re-raised by the transaction
            fail(phase, exc)

    branch = threading.Thread(target=restore, name="warm-restore", daemon=True)
    branch.start()
    try:
        reseed()
    finally:
        branch.join()
        timings["reset_and_reseed"] = monotonic() - started
    if failures:
        return failures[0]
    return boundaries[0]


def _publish(path: Path, receipt: WarmReceipt) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"schema": "sdo.warm-lane/v1", **asdict(receipt)}, indent=2) + "\n")
    temporary.replace(path)
