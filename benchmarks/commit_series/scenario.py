"""One commit of a series, walked against a live lane: push it as an engineer, record what SDO did.

The replayer (``benchmarks.common.replay``) pushes; the reads and the verdict wait are the scenario machinery the toy
and GitLab replays already use. Everything here sees SDO through its accepted repository, the holds and operability
ConfigMaps and the controller state, never through its code. ``run_commit`` takes injected ops so the decisions are
tested without a cluster.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.fastloop.gitlab_replay import hold_detail, operability_digest, operability_summary
from benchmarks.sregym.fastloop.toy_scenarios import (
    ScenarioOps,
    ScenarioTimings,
    upstream_commit,
    wait_verdict,
    watch_for_incident,
)

if TYPE_CHECKING:
    from benchmarks.common.replay.replay import ReplayRecord
    from benchmarks.common.replay.series import Series


class SeriesOps(ScenarioOps, Protocol):
    def push_commit(self, index: int) -> ReplayRecord: ...
    def agent_task_count(self) -> int: ...
    def now(self) -> float: ...
    def head_deployment(self) -> dict[str, Any] | None: ...
    def observed_at(self) -> float | None: ...


@dataclass(frozen=True)
class Bounds:
    """Seconds SDO has to reach a verdict, the quiet window that follows, and how long an open incident may take."""

    verdict: float = 900.0
    steady: float = 60.0
    resolution: float = 900.0
    poll: float = 5.0
    #: After a position another one follows, the next push waits until no incident has been open for this long
    #: (``until_quiet``): an event, not a fixed window, so a release that opens incidents late (retried jobs of a
    #: restarted worker) is not met by the next push, and one that opens none costs this many seconds.
    steady_between: float = 20.0
    until_quiet: bool = False
    #: The longest a non-final position waits for quiet, counting only time the controller observes. A release that
    #: keeps opening incidents (10.6.0: a chain of them, each a few minutes) is SDO's work, not a reason to hold the
    #: series for half an hour: the next push goes after this, and the record says SDO was not quiet
    #: (``noisy_before_next``).
    quiet_cap: float = 180.0

    def __post_init__(self) -> None:
        for name in ("verdict", "steady", "resolution", "poll", "steady_between", "quiet_cap"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")

    def at(self, index: int, total: int) -> Bounds:
        """The bounds of position ``index`` of ``total``: the quiet window is the full one at the end of the series."""
        if index >= total:
            return self
        return replace(self, steady=min(self.steady, self.steady_between), until_quiet=True)

    @property
    def timings(self) -> ScenarioTimings:
        return ScenarioTimings(verdict=self.verdict, steady=self.steady, poll=self.poll)


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _incident_closed(ops: SeriesOps, bounds: Bounds) -> bool:
    """Whether an incident open at the end of the quiet window closed within ``resolution``."""
    end = ops.clock() + bounds.resolution
    while ops.incident_open():
        if ops.clock() >= end:
            return False
        ops.sleep(bounds.poll)
    return True


#: A controller that has evaluated nothing for this long is not observing (a rebuild, a restart): its normal detector
#: cadence leaves gaps of up to about 15 s between persisted evaluations, a rebuild minutes.
OBSERVATION_STALL_S = 30.0


def watch_until_quiet(ops: SeriesOps, timings: ScenarioTimings, quiet: float, cap: float, patience: float) -> bool:
    """Whether an incident was seen before SDO had none open for ``quiet`` seconds of its own observation.

    Time is the controller's observation clock (``observed_at``: its newest persisted detector evaluation). One that
    has evaluated nothing for ``OBSERVATION_STALL_S`` (a rebuild after a deploy, a restart) says nothing by having no
    incident open: the quiet window starts again at its next evaluation, and neither the stall nor the jump after it
    counts against ``cap``, the bound on a noisy controller. ``patience`` bounds the whole wait in wall time."""
    end = ops.clock() + patience
    seen, clear_since, observed_time = False, None, 0.0
    last = ops.observed_at()
    last_fresh = ops.clock()
    while ops.clock() < end and observed_time < cap:
        ops.sleep(timings.poll)
        now, observed = ops.clock(), ops.observed_at()
        if observed is None or last is None or observed <= last:
            if now - last_fresh >= OBSERVATION_STALL_S:
                clear_since = None
            last = observed if observed is not None else last
            continue
        resumed = now - last_fresh >= OBSERVATION_STALL_S
        if resumed:
            clear_since = None
        else:
            observed_time += observed - last
        last, last_fresh = observed, now
        if ops.incident_open():
            seen, clear_since = True, None
            continue
        clear_since = observed if clear_since is None else clear_since
        if observed - clear_since >= quiet:
            return seen
    return seen


def run_commit(ops: SeriesOps, series: Series, index: int, bounds: Bounds, label: str) -> dict[str, Any]:
    """Push commit ``index`` (1-based) and return its record: the outcome, times and what SDO reported.

    ``valid`` is true for every outcome SDO can have (accepted, held, incident, timeout) and for a commit the
    series cannot apply (conflict, empty): those are results, not harness failures.
    """
    record: dict[str, Any] = {
        "operator": label,
        "series": series.name,
        "index": index,
        "commit": series.commits[index - 1],
        "valid": True,
    }
    known = {c.sha for c in ops.accepted_commits()}
    blocked = ops.incident_open()  # SDO deploys nothing while an incident is open: not the verdict on this push
    before = ops.snapshot()
    started = ops.clock()
    record["lifecycle"] = {"inject_at": _iso(ops.now())}
    pushed = ops.push_commit(index)
    if pushed.status in ("conflict", "skipped"):
        return {**record, "outcome": "conflict", "conflict_paths": list(pushed.paths)}
    if pushed.status == "empty":
        return {**record, "outcome": "empty"}
    sha = pushed.pushed_sha
    verdict, accepted, hold = wait_verdict(ops, bounds.timings, sha, incident_ends=not blocked)
    deploy_seconds = round(ops.clock() - started, 1)
    detail = hold_detail(hold) if hold else None
    noisy = False
    if bounds.until_quiet:
        incident = (
            watch_until_quiet(ops, bounds.timings, bounds.steady_between, bounds.quiet_cap, bounds.resolution)
            or verdict == "incident"
        )
        # The next push goes now: an incident still open is SDO's work on this release, not a wait for the series.
        noisy = ops.incident_open()
        resolved = not noisy
    else:
        incident = watch_for_incident(ops, bounds.timings) or verdict == "incident"
        resolved = _incident_closed(ops, bounds) if ops.incident_open() else True
    adopted = accepted or upstream_commit(ops.accepted_commits(), sha)
    after = ops.snapshot()
    return {
        **record,
        "pushed": sha,
        "outcome": verdict or "timeout",
        "deploy_seconds": deploy_seconds,
        "wall_seconds": round(ops.clock() - started, 1),
        "accepted_commit": adopted.sha if adopted else "",
        "accepted_commits": [c.sha for c in ops.accepted_commits() if c.sha not in known],
        "hold": None
        if detail is None
        else {"kind": detail.kind, "cause": detail.cause, "owner": detail.owner, "reason": detail.reason},
        "blocked_by_open_incident": blocked,
        "head_deployment": ops.head_deployment(),
        "incident_opened": incident,
        "noisy_before_next": noisy,
        "incident_resolved": resolved,
        "agent_tasks": after.agent_tasks - before.agent_tasks,
        "pod_failures": max(0, after.pod_failures - before.pod_failures),
        "controller_relaunches": after.controller_relaunches - before.controller_relaunches,
        "operability": operability_summary(
            ops.operability(), adopted.sha if adopted else "", operability_digest(adopted) if adopted else ""
        ),
    }
