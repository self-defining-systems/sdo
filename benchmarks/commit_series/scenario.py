"""One commit of a series, walked against a live lane: push it as an engineer, record what SDO did.

The replayer (``benchmarks.common.replay``) pushes; the reads and the verdict wait are the scenario machinery the toy
and GitLab replays already use. Everything here sees SDO through its accepted repository, the holds and operability
ConfigMaps and the controller state, never through its code. ``run_commit`` takes injected ops so the decisions are
tested without a cluster.
"""

from __future__ import annotations

from dataclasses import dataclass
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


@dataclass(frozen=True)
class Bounds:
    """Seconds SDO has to reach a verdict, the quiet window that follows, and how long an open incident may take."""

    verdict: float = 900.0
    steady: float = 60.0
    resolution: float = 900.0
    poll: float = 5.0

    def __post_init__(self) -> None:
        for name in ("verdict", "steady", "resolution", "poll"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")

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
    if ops.operability() is None:
        # SDO deploys a branch head only against the deployment it recorded for the commit before it. A seeded
        # install (install-existing of a memory baseline) records none, publishes no operability report, and the
        # push would wait out the verdict bound unseen.
        return {
            **record,
            "valid": False,
            "invalid_reason": "SDO has no recorded deployment (a seeded install): it cannot deploy a pushed commit. "
            "Install the series' base through SDO's deployer (host), not install-existing.",
        }
    known = {c.sha for c in ops.accepted_commits()}
    before = ops.snapshot()
    started = ops.clock()
    record["lifecycle"] = {"inject_at": _iso(ops.now())}
    pushed = ops.push_commit(index)
    if pushed.status in ("conflict", "skipped"):
        return {**record, "outcome": "conflict", "conflict_paths": list(pushed.paths)}
    if pushed.status == "empty":
        return {**record, "outcome": "empty"}
    sha = pushed.pushed_sha
    verdict, accepted, hold = wait_verdict(ops, bounds.timings, sha)
    deploy_seconds = round(ops.clock() - started, 1)
    detail = hold_detail(hold) if hold else None
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
        "incident_opened": incident,
        "incident_resolved": resolved,
        "agent_tasks": after.agent_tasks - before.agent_tasks,
        "pod_failures": max(0, after.pod_failures - before.pod_failures),
        "controller_relaunches": after.controller_relaunches - before.controller_relaunches,
        "operability": operability_summary(
            ops.operability(), adopted.sha if adopted else "", operability_digest(adopted) if adopted else ""
        ),
    }
