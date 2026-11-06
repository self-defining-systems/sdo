"""Toy commit scenarios: a benign and a broken upstream commit, driven and judged against injected cluster reads.

The drivers push a scripted commit (``toy_upstream.replay_commit``) and then watch SDO's accepted repository, its
upstream hold record, the workload and the control namespace through ``ScenarioOps``. The assertions are pure
checks over those reads, so they run hermetically with a fake world; the kubectl-backed ``ScenarioOps`` is wired
when a live lane exists. Parsers for the live reads (accepted-commit trailers, the ``sdo-upstream-holds``
ConfigMap, pod JSON) live here and are tested on real Git output and synthetic documents.

Nothing here imports SDO production code; the contracts are strings: the ``SDO-Actor`` and ``SDO-Upstream``
trailers and the ``<reason>: <detail>`` form of a hold's reason.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.adapter import WaitKind, wait_for

if TYPE_CHECKING:
    from collections.abc import Mapping


UPSTREAM_ACTOR = "upstream"
ACTOR_TRAILER = "SDO-Actor"
UPSTREAM_TRAILER = "SDO-Upstream"
#: Hold reasons a failed rollout may legitimately carry (E7 rollout, E8 health); the rest are other holds.
BROKEN_COMMIT_REASONS = frozenset({"rollout", "health"})
KNOWN_REASONS = frozenset(
    {"ref_rewritten", "memory_touched", "materialize", "merge_conflict", "render", "shape_change", "build"}
    | BROKEN_COMMIT_REASONS
)
#: ``git log`` arguments whose output ``parse_accepted_log`` reads; run in the accepted repository.
ACCEPTED_LOG_ARGS = ("log", "--first-parent", "--format=%x1e%H%x1f%(trailers:only,unfold)")
_HOLD_KEY = re.compile(r"hold-[0-9a-f]{16}")


@dataclass(frozen=True)
class AcceptedCommit:
    sha: str
    trailers: Mapping[str, str]


@dataclass(frozen=True)
class HoldRecord:
    revision: str
    reason: str

    @property
    def reason_kind(self) -> str:
        """The typed part of the reason, before the first colon (empty when the reason is untyped)."""
        kind, sep, _ = self.reason.partition(":")
        return kind.strip() if sep else ""


@dataclass(frozen=True)
class Snapshot:
    """Everything a scenario compares before and after the push."""

    head: str
    images: Mapping[str, str]  # workload -> image reference in its spec
    digests: Mapping[str, str]  # workload -> image ID its running pods use
    agent_tasks: int
    pod_failures: int
    controller_relaunches: int


class ScenarioOps(Protocol):
    """The clock, the upstream push and the cluster reads of a scenario (injected so assertions are deterministic)."""

    def clock(self) -> float: ...
    def sleep(self, seconds: float) -> None: ...
    def push(self, name: str) -> str: ...
    def snapshot(self) -> Snapshot: ...
    def accepted_commits(self) -> list[AcceptedCommit]: ...
    def holds(self) -> list[HoldRecord]: ...
    def incident_open(self) -> bool: ...


@dataclass(frozen=True)
class ScenarioTimings:
    """Bounds in seconds: how long SDO has to reach a verdict, how long the world must then stay quiet, the poll."""

    verdict: float
    steady: float
    poll: float

    def __post_init__(self) -> None:
        for name in ("verdict", "steady", "poll"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")

    @classmethod
    def standard(cls) -> ScenarioTimings:
        return cls(
            verdict=wait_for(WaitKind.OPERATION), steady=wait_for(WaitKind.MINUTE, 2), poll=wait_for(WaitKind.POLL)
        )


@dataclass
class ScenarioResult:
    name: str
    pushed: str
    violations: list[str] = field(default_factory=list)
    verdict: str = ""

    @property
    def passed(self) -> bool:
        return not self.violations


def parse_accepted_log(output: str) -> list[AcceptedCommit]:
    """Parse ``git ACCEPTED_LOG_ARGS`` output, newest first."""
    commits: list[AcceptedCommit] = []
    for record in output.split("\x1e"):
        if not record.strip():
            continue
        sha, _, block = record.partition("\x1f")
        trailers: dict[str, str] = {}
        for line in block.splitlines():
            key, sep, value = line.partition(":")
            if sep and key.strip():
                trailers[key.strip()] = value.strip()
        commits.append(AcceptedCommit(sha.strip(), trailers))
    return commits


def parse_holds(configmap: Mapping[str, Any]) -> list[HoldRecord]:
    """Holds of the ``sdo-upstream-holds`` ConfigMap (``kubectl get -o json``); a malformed entry is an error."""
    holds: list[HoldRecord] = []
    for key, value in sorted((configmap.get("data") or {}).items()):
        if not _HOLD_KEY.fullmatch(key):
            raise ValueError(f"unexpected key {key!r} in the upstream holds ConfigMap")
        document = json.loads(value)
        revision, reason = document.get("revision"), document.get("reason")
        if not isinstance(revision, str) or not revision or not isinstance(reason, str) or not reason:
            raise ValueError(f"hold {key} lacks a revision or reason")
        holds.append(HoldRecord(revision, reason))
    return holds


def workload_digests(pods: Mapping[str, Any]) -> dict[str, str]:
    """Running image ID per app (label ``app``) from ``kubectl get pods -o json``; running containers only."""
    digests: dict[str, str] = {}
    for pod in pods.get("items", []):
        app = (pod["metadata"].get("labels") or {}).get("app")
        if app is None or pod.get("status", {}).get("phase") != "Running":
            continue
        for container in pod["status"].get("containerStatuses", []):
            if container.get("imageID"):
                digests[app] = container["imageID"]
    return digests


def pod_failure_count(pods: Mapping[str, Any]) -> int:
    """Container restarts plus pods in phase Failed."""
    total = 0
    for pod in pods.get("items", []):
        total += pod.get("status", {}).get("phase") == "Failed"
        total += sum(c.get("restartCount", 0) for c in pod.get("status", {}).get("containerStatuses", []))
    return total


def upstream_commit(commits: list[AcceptedCommit], pushed: str) -> AcceptedCommit | None:
    """The accepted commit that adopted ``pushed``, if any."""
    return next((c for c in commits if c.trailers.get(UPSTREAM_TRAILER) == pushed), None)


def _wait_verdict(
    ops: ScenarioOps, timings: ScenarioTimings, pushed: str
) -> tuple[str, AcceptedCommit | None, HoldRecord | None]:
    """Poll until the push is accepted, held, or an incident is open; "" when none happens in time."""
    deadline = ops.clock() + timings.verdict
    while True:
        if commit := upstream_commit(ops.accepted_commits(), pushed):
            return "accepted", commit, None
        if hold := next((h for h in ops.holds() if h.revision == pushed), None):
            return "held", None, hold
        if ops.incident_open():
            return "incident", None, None
        if ops.clock() >= deadline:
            return "", None, None
        ops.sleep(timings.poll)


def _watch(ops: ScenarioOps, timings: ScenarioTimings) -> bool:
    """Whether an incident was open at any poll of the steady window (one more look at its end)."""
    end = ops.clock() + timings.steady
    seen = False
    while ops.clock() < end:
        seen = seen or ops.incident_open()
        ops.sleep(timings.poll)
    return seen or ops.incident_open()


def run_benign(ops: ScenarioOps, timings: ScenarioTimings, commit: str = "benign") -> ScenarioResult:
    """Push a benign commit: accepted by Go alone, the new image runs, no agent work, nothing disturbed."""
    before = ops.snapshot()
    pushed = ops.push(commit)
    result = ScenarioResult(commit, pushed)
    verdict, accepted, hold = _wait_verdict(ops, timings, pushed)
    result.verdict = verdict
    if verdict == "":
        result.violations.append(f"pushed {pushed[:12]} was neither accepted nor held within {timings.verdict:g}s")
    elif verdict == "held":
        assert hold is not None
        result.violations.append(f"benign commit {pushed[:12]} was held: {hold.reason}")
    elif verdict == "incident":
        result.violations.append(f"an incident opened before {pushed[:12]} was accepted")
    else:
        assert accepted is not None
        if (actor := accepted.trailers.get(ACTOR_TRAILER)) != UPSTREAM_ACTOR:
            result.violations.append(
                f"accepted commit {accepted.sha[:12]} has {ACTOR_TRAILER} {actor!r}, expected upstream"
            )
    incident = _watch(ops, timings)
    after = ops.snapshot()
    if incident:
        result.violations.append("an incident opened during the steady window")
    if verdict == "accepted":
        if after.head == before.head:
            result.violations.append("accepted head did not move")
        unchanged = sorted(w for w, d in after.digests.items() if before.digests.get(w) == d)
        missing = sorted(set(before.digests) - set(after.digests))
        if unchanged:
            result.violations.append(f"workloads still run the old image: {', '.join(unchanged)}")
        if missing or not after.digests:
            result.violations.append(f"workloads not running after the advance: {', '.join(missing) or 'all'}")
    if after.agent_tasks != before.agent_tasks:
        result.violations.append(f"agent tasks went from {before.agent_tasks} to {after.agent_tasks}; expected none")
    if after.pod_failures != before.pod_failures:
        result.violations.append(f"pod failures went from {before.pod_failures} to {after.pod_failures}")
    relaunches = after.controller_relaunches - before.controller_relaunches
    if relaunches > 1:
        result.violations.append(f"controller relaunched {relaunches} times; expected at most one")
    return result


def run_broken(
    ops: ScenarioOps,
    timings: ScenarioTimings,
    commit: str = "broken",
    reasons: frozenset[str] = BROKEN_COMMIT_REASONS,
) -> ScenarioResult:
    """Push a broken commit: never accepted, held with a typed reason or an incident opened, no rollback by Go."""
    before = ops.snapshot()
    pushed = ops.push(commit)
    result = ScenarioResult(commit, pushed)
    verdict, accepted, hold = _wait_verdict(ops, timings, pushed)
    result.verdict = verdict
    at_verdict = ops.snapshot()
    if verdict == "":
        result.violations.append(f"pushed {pushed[:12]} was neither held nor an incident within {timings.verdict:g}s")
    elif verdict == "accepted":
        assert accepted is not None
        result.violations.append(f"broken commit {pushed[:12]} was accepted as {accepted.sha[:12]}")
    elif verdict == "held":
        assert hold is not None
        if hold.reason_kind not in KNOWN_REASONS:
            result.violations.append(f"hold reason {hold.reason!r} is not typed")
        elif hold.reason_kind not in reasons:
            result.violations.append(f"hold reason {hold.reason_kind!r} is not one of {sorted(reasons)}")
        if at_verdict.head != before.head:
            result.violations.append("accepted head moved although the commit was held")
    incident = _watch(ops, timings) or verdict == "incident"
    after = ops.snapshot()
    if upstream_commit(ops.accepted_commits(), pushed) is not None and verdict != "accepted":
        result.violations.append("the broken commit was accepted after the verdict")
    if not incident and verdict == "held":
        # Go leaves the cluster as the failed advance left it; only an incident may change it afterwards.
        reverted = sorted(
            w
            for w, image in before.images.items()
            if at_verdict.images.get(w) != image and after.images.get(w) == image
        )
        if reverted:
            result.violations.append(f"cluster was rolled back without an incident: {', '.join(reverted)}")
        if after.images != at_verdict.images:
            result.violations.append("workload images changed after the hold with no incident open")
    return result


def helm_onboarding_arguments(deployment_plan: Mapping[str, Any]) -> list[str]:
    """Host flags that hand the harness-provided ``DeploymentPlan`` to SDO (no agent authors the deployment)."""
    return ["--deployment-plan", json.dumps(deployment_plan, sort_keys=True, separators=(",", ":"))]
