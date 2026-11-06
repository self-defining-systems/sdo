"""``fastloop gitlab replay``: push successive GitLab chart releases to the followed ref and record what SDO did.

The lane follows a Git ref (``gitlab_lane.lane_upstream``). The replayer, an external process of the benchmark, bumps
the vendored upstream chart release by release (the commit a chart-bump upstream would make) and waits for SDO's
outcome per release: advanced (accepted commit and operability digest), held (typed cause and owner), repaired (the
revision was adopted after an agent's ``.sdo/`` commit) or incident. It records outcome, agent tasks, wall time and
human edits per release in the run directory. A hold nobody can act on ends the replay: the replayer never edits
anything on SDO's behalf.

Everything here reads SDO through JSON and Git strings only (accepted-commit trailers, the holds and operability
ConfigMaps, the state ConfigMap); nothing imports SDO production code. The driver takes injected ``ReplayOps`` so the
decisions are tested with a fake lane; ``KubectlReplayOps`` is the live implementation.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

from apps.gitlab import assemble
from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.gitlab_releases import Release, ReleaseList, load_releases
from benchmarks.sregym.fastloop.toy_live import KubectlScenarioOps
from benchmarks.sregym.fastloop.toy_operability import (
    OperabilityRead,
    parse_hold_routing,
    report_violations,
)
from benchmarks.sregym.fastloop.toy_scenarios import (
    ACTOR_TRAILER,
    UPSTREAM_ACTOR,
    AcceptedCommit,
    HoldRecord,
    ScenarioOps,
    ScenarioTimings,
    upstream_commit,
    wait_verdict,
    watch_for_incident,
)
from benchmarks.sregym.fastloop.toy_upstream import UpstreamError, forwarded_remote, push_change

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.toy_upstream import UpstreamLayout

#: Roles that can act on a hold: the deployer (patch, render, build), the designer (detection) and the responder.
ACTING_OWNERS = frozenset({"deployer", "designer", "responder"})
OPERABILITY_TRAILER = "SDO-Operability"
CHART_DIRECTORY = ("chart", "charts", "gitlab")
SDO_PREFIX = ".sdo/"
SUMMARY_FILE = "gitlab-replay.json"
_IDENTITY = ("-c", "user.name=gitlab-replay", "-c", "user.email=gitlab-replay@invalid")
_CHART_VERSION = re.compile(r"^version:\s*(\S+)\s*$", re.MULTILINE)
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")

ADVANCED, HELD, REPAIRED, INCIDENT, TIMEOUT = "advanced", "held", "repaired", "incident", "timeout"
ADOPTED_OUTCOMES = frozenset({ADVANCED, REPAIRED})


@dataclass(frozen=True)
class ReplayTimings:
    """Seconds SDO has to reach a verdict, for an owner to start acting on a hold and to finish, plus quiet and poll."""

    verdict: float
    owner_action: float
    resolution: float
    settle: float
    poll: float

    def __post_init__(self) -> None:
        for name in ("verdict", "owner_action", "resolution", "settle", "poll"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")

    @classmethod
    def standard(cls) -> ReplayTimings:
        """Image pulls and rolling ~11 GitLab workloads make a verdict slow; an owner should start within minutes."""
        return cls(
            verdict=wait_for(WaitKind.LONG_OPERATION),
            owner_action=wait_for(WaitKind.MINUTE, 3),
            resolution=wait_for(WaitKind.RESPONSE, 2),
            settle=wait_for(WaitKind.MINUTE, 2),
            poll=wait_for(WaitKind.POLL),
        )

    def scenario(self, *, steady: float | None = None) -> ScenarioTimings:
        return ScenarioTimings(verdict=self.verdict, steady=steady or self.settle, poll=self.poll)


class ReplayOps(ScenarioOps, Protocol):
    """``ScenarioOps`` (``push`` takes a chart version here) plus the reads the replay adds."""

    def agent_task_count(self) -> int: ...
    def changed_paths(self, sha: str) -> list[str]: ...
    def accepted_chart_version(self) -> str: ...


@dataclass(frozen=True)
class HoldDetail:
    kind: str
    cause: str
    owner: str
    items: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class RepairCommit:
    sha: str
    actor: str
    paths: tuple[str, ...]


@dataclass
class ReleaseRecord:
    """One release's outcome, in the shape ``analyze-experiment`` reads (see references/operability-report.md)."""

    version: str
    app_version: str
    pushed: str = ""
    outcome: str = ""
    accepted_commit: str = ""
    operability_digest: str = ""
    hold: HoldDetail | None = None
    incident_opened: bool = False
    agent_tasks: int = 0
    wall_seconds: float = 0.0
    human_edits: int = 0
    repair_commits: list[RepairCommit] = field(default_factory=list)
    changed_workloads: list[str] = field(default_factory=list)
    pod_failures: int = 0
    controller_relaunches: int = 0
    expected: dict[str, Any] = field(default_factory=dict)
    operability: dict[str, Any] | None = None
    violations: list[str] = field(default_factory=list)
    stop_reason: str = ""

    @property
    def adopted(self) -> bool:
        return self.outcome in ADOPTED_OUTCOMES

    @property
    def passed(self) -> bool:
        return not self.violations and not self.stop_reason


def parse_chart_version(chart_yaml: str) -> str:
    """The chart's own ``version`` (the top-level key; a dependency's version is indented)."""
    match = _CHART_VERSION.search(chart_yaml)
    if match is None:
        raise ValueError("Chart.yaml has no version")
    return match.group(1)


def operability_digest(commit: AcceptedCommit) -> str:
    """The advance report digest on an accepted commit's ``SDO-Operability`` trailer (empty when it carries none)."""
    match = _DIGEST.search(commit.trailers.get(OPERABILITY_TRAILER, ""))
    return match.group(0) if match else ""


def hold_detail(hold: HoldRecord) -> HoldDetail:
    routing = parse_hold_routing(hold.reason)
    if routing is None:
        return HoldDetail("", "", "", (), hold.reason)
    return HoldDetail(routing.kind, routing.cause, routing.owner, routing.items, hold.reason)


def operability_summary(read: OperabilityRead | None) -> dict[str, Any] | None:
    if read is None:
        return None
    return {
        "stage": read.stage,
        "accepted_commit": read.accepted_commit,
        "digest": read.digest,
        "items": {item.id: item.status for item in read.items},
    }


def expectation(release: Release) -> dict[str, Any]:
    """What the list says this release changes, recorded beside the outcome for the analysis."""
    changes = release.changes
    return {
        "images_changed": changes.images_changed if changes else 0,
        "values_keys_added": changes.values_keys_added if changes else 0,
        "shape_change": changes.changes_shape if changes else False,
        "zero_agent_turns": release.expects_zero_agent_turns,
    }


def lane_violations(ops: ReplayOps) -> list[str]:
    """Why the lane is not steady: no green operability report, or an incident open, before the first push."""
    out = report_violations(ops.operability(), {c.sha for c in ops.accepted_commits()})
    if ops.incident_open():
        out.append("an incident is open before the replay starts")
    return out


def plan_replay(releases: ReleaseList, current: str, limit: int | None = None) -> tuple[Release, ...]:
    """The releases to push to a lane whose accepted source holds chart ``current``."""
    pending = releases.after(current)
    if not pending:
        raise ValueError(f"the lane already holds {current}, the last release of the list; nothing to replay")
    return pending if limit is None else pending[:limit]


def _new_commits(ops: ReplayOps, known: set[str]) -> list[AcceptedCommit]:
    return [c for c in ops.accepted_commits() if c.sha not in known]


def _await_resolution(
    ops: ReplayOps,
    timings: ReplayTimings,
    pushed: str,
    before_tasks: int,
    hold: HoldDetail | None,
    record: ReleaseRecord,
) -> bool:
    """After a hold or incident: wait for an owner to act, then for the revision to be adopted or the incident to close.

    Returns whether the pushed revision was adopted. Sets ``record.stop_reason`` when nobody acted or the owner did not
    finish; the caller stops the replay then. A closed incident with the revision still unadopted is not a stop: the
    next release supersedes it.
    """
    acted = False
    deadline = ops.clock() + timings.owner_action
    while True:
        incident = ops.incident_open()
        adopted = upstream_commit(ops.accepted_commits(), pushed) is not None
        if adopted and not incident:
            return True
        if record.incident_opened and not incident:
            return False  # the responder's incident closed
        record.incident_opened = record.incident_opened or incident
        if not acted and (incident or ops.agent_task_count() > before_tasks):
            acted = True
            deadline = ops.clock() + timings.resolution
        if ops.clock() >= deadline:
            window = timings.resolution if acted else timings.owner_action
            owner = f" (owner {hold.owner}, cause {hold.cause})" if hold else ""
            record.stop_reason = (
                f"{'the owner did not finish within' if acted else 'no owner action within'} {window:g}s{owner}"
            )
            return adopted
        ops.sleep(timings.poll)


def replay_release(ops: ReplayOps, release: Release, timings: ReplayTimings) -> ReleaseRecord:
    """Push one release and record what SDO did with it (stop reason set when the replay cannot continue)."""
    record = ReleaseRecord(release.version, release.app_version, expected=expectation(release))
    known = {c.sha for c in ops.accepted_commits()}
    before = ops.snapshot()
    started_wall = ops.clock()
    record.pushed = ops.push(release.version)
    verdict, accepted, hold = wait_verdict(ops, timings.scenario(), record.pushed)
    detail = hold_detail(hold) if hold else None
    record.hold = detail
    adopted = verdict == "accepted"
    if verdict == "":
        record.outcome = TIMEOUT
        record.stop_reason = (
            f"{record.pushed[:12]} was neither accepted, held nor an incident within {timings.verdict:g}s"
        )
    elif verdict == "held":
        assert detail is not None
        if detail.owner not in ACTING_OWNERS:
            reason = "no routing prefix" if not detail.owner else f"owner {detail.owner!r} is not an agent role"
            record.stop_reason = (
                f"hold {detail.kind or detail.reason!r} has no owner action ({reason}; cause {detail.cause or '-'})"
            )
        else:
            adopted = _await_resolution(ops, timings, record.pushed, before.agent_tasks, detail, record)
    elif verdict == "incident":
        record.incident_opened = True
        adopted = _await_resolution(ops, timings, record.pushed, before.agent_tasks, None, record)
    record.wall_seconds = round(ops.clock() - started_wall, 1)

    adopted_commit = accepted or upstream_commit(ops.accepted_commits(), record.pushed)
    after = ops.snapshot()
    record.agent_tasks = after.agent_tasks - before.agent_tasks
    record.pod_failures = after.pod_failures - before.pod_failures
    record.controller_relaunches = after.controller_relaunches - before.controller_relaunches
    commits = _new_commits(ops, known)
    others = [c for c in commits if c.trailers.get(ACTOR_TRAILER) != UPSTREAM_ACTOR]
    record.human_edits = sum(1 for c in others if c.trailers.get(ACTOR_TRAILER) == "human")
    record.repair_commits = [
        RepairCommit(c.sha, c.trailers.get(ACTOR_TRAILER, ""), tuple(ops.changed_paths(c.sha))) for c in others
    ]
    if record.stop_reason == "" and adopted and adopted_commit is not None:
        record.accepted_commit = adopted_commit.sha
        record.operability_digest = operability_digest(adopted_commit)
        record.outcome = REPAIRED if record.repair_commits else ADVANCED
        if record.incident_opened and not record.repair_commits:
            record.outcome = INCIDENT
    elif record.outcome != TIMEOUT:
        record.outcome = INCIDENT if record.incident_opened else HELD
    if record.stop_reason == "" and record.adopted and watch_for_incident(ops, timings.scenario()):
        record.incident_opened = True
        record.stop_reason = "an incident opened during the quiet window"
    record.changed_workloads = sorted(w for w, d in after.digests.items() if before.digests.get(w) not in (None, d))
    if record.hold is None:  # an accepted advance may still owe a follow-up (a designer refresh hold)
        record.hold = next((hold_detail(h) for h in ops.holds() if h.revision == record.pushed), None)
    record.operability = operability_summary(ops.operability())
    record.violations = release_violations(record, adopted_commit, release)
    return record


def release_violations(record: ReleaseRecord, adopted: AcceptedCommit | None, release: Release) -> list[str]:
    out: list[str] = []
    if record.human_edits:
        out.append(f"{record.human_edits} human edit(s) reached the accepted history")
    for repair in record.repair_commits:
        outside = [p for p in repair.paths if not p.startswith(SDO_PREFIX)]
        if outside:
            out.append(
                f"repair {repair.sha[:12]} by {repair.actor or 'unknown'} changed files outside .sdo/: {outside}"
            )
    if record.outcome == ADVANCED:
        if record.agent_tasks and release.expects_zero_agent_turns:
            out.append(f"advance took {record.agent_tasks} agent task(s); the common case takes none")
        if adopted is not None and adopted.trailers.get(ACTOR_TRAILER) != UPSTREAM_ACTOR:
            out.append(f"advance commit {adopted.sha[:12]} has {ACTOR_TRAILER} {adopted.trailers.get(ACTOR_TRAILER)!r}")
        if not record.operability_digest:
            out.append(f"advance commit {record.accepted_commit[:12]} carries no {OPERABILITY_TRAILER} digest")
        if record.expected["images_changed"] and not record.changed_workloads:
            out.append(f"the release changes {record.expected['images_changed']} images but no workload runs a new one")
    return out


@dataclass
class ReplaySummary:
    """The whole replay: the plan, one record per attempted release, and why it ended."""

    lane: str
    started_at: str
    baseline: str
    planned: list[str]
    results: list[ReleaseRecord] = field(default_factory=list)
    preflight: list[str] = field(default_factory=list)
    stopped: str = ""

    @property
    def human_edits(self) -> int:
        return sum(r.human_edits for r in self.results)

    @property
    def passed(self) -> bool:
        return not self.preflight and not self.stopped and all(r.passed for r in self.results)

    def as_json(self) -> dict[str, Any]:
        return {
            **{k: v for k, v in asdict(self).items() if k != "results"},
            "results": [asdict(r) for r in self.results],
            "human_edits": self.human_edits,
            "agent_tasks": sum(r.agent_tasks for r in self.results),
            "wall_seconds": round(sum(r.wall_seconds for r in self.results), 1),
            "passed": self.passed,
        }


def run_replay(
    ops: ReplayOps,
    releases: ReleaseList,
    timings: ReplayTimings,
    *,
    lane: str,
    limit: int | None = None,
    write: Callable[[ReplaySummary, ReleaseRecord | None], None] = lambda *_: None,
) -> ReplaySummary:
    """Replay the releases after the lane's current chart in order; stop at the first stop reason.

    ``write`` is called after the preflight and after every release (the summary so far, and the finished record).
    """
    current = ops.accepted_chart_version()
    planned = plan_replay(releases, current, limit)
    summary = ReplaySummary(
        lane, datetime.now(UTC).isoformat(timespec="seconds"), current, [r.version for r in planned]
    )
    summary.preflight = lane_violations(ops)
    write(summary, None)
    if summary.preflight:
        summary.stopped = "the lane is not steady: " + "; ".join(summary.preflight)
        write(summary, None)
        return summary
    for release in planned:
        record = replay_release(ops, release, timings)
        summary.results.append(record)
        if record.stop_reason:
            summary.stopped = f"{release.version}: {record.stop_reason}"
        write(summary, record)
        if summary.stopped:
            break
    return summary


# ---- the live lane -------------------------------------------------------------------------------------------------


def fetch_archive(release: Release, cache: Path, opener: Callable[..., Any] = urllib.request.urlopen) -> Path:
    """The release's chart archive in ``cache``, downloaded once and verified against the list's sha256."""
    cache.mkdir(parents=True, exist_ok=True)
    target = cache / f"{assemble.load_pins()['gitlab_chart']['name']}-{release.version}.tgz"
    if not target.is_file() or hashlib.sha256(target.read_bytes()).hexdigest() != release.sha256:
        with opener(release.url, timeout=wait_for(WaitKind.COMMAND)) as response:
            data = response.read()
        actual = hashlib.sha256(data).hexdigest()
        if actual != release.sha256:
            raise ValueError(f"{release.url}: sha256 {actual} does not match the list's {release.sha256}")
        target.write_bytes(data)
    return target


def push_release(release: Release, archive: Path, remote: str, layout: UpstreamLayout) -> str:
    """Replace the vendored upstream chart by ``archive`` and bump the umbrella dependency, as a chart-bump commit."""
    pins = release.pins(assemble.load_pins())

    def bump(clone: Path) -> None:
        vendored = clone.joinpath(*CHART_DIRECTORY)
        if not vendored.is_dir():
            raise UpstreamError(f"the followed ref has no {'/'.join(CHART_DIRECTORY)}; is this a gitlab lane?")
        shutil.rmtree(vendored)
        assemble.vendor_chart(archive, clone / "chart", pins)
        assemble.bump_dependency(clone / "chart" / "Chart.yaml", pins["gitlab_chart"]["name"], release.version)

    return push_change(
        remote, layout, f"chore(deps): bump the gitlab chart to {release.version}", bump, identity=_IDENTITY
    )


class KubectlReplayOps(KubectlScenarioOps):
    """The live lane: ``KubectlScenarioOps`` reads, and a ``push`` of a release through a port-forward."""

    def __init__(
        self, environment: FastloopEnvironment, layout: UpstreamLayout, releases: ReleaseList, cache: Path
    ) -> None:
        super().__init__(environment, layout)
        self.releases = releases
        self.cache = cache

    def push(self, name: str) -> str:
        release = self.releases.get(name)
        archive = fetch_archive(release, self.cache)
        with forwarded_remote(self.environment.kubeconfig, self.layout) as remote:
            return push_release(release, archive, remote, self.layout)

    def changed_paths(self, sha: str) -> list[str]:
        return self._accepted("show", "--name-only", "--format=", sha).split()

    def accepted_chart_version(self) -> str:
        return parse_chart_version(self._accepted("show", "HEAD:" + "/".join((*CHART_DIRECTORY, "Chart.yaml"))))


def release_file(run_dir: Path, version: str) -> Path:
    return run_dir / f"replay-gitlab-{version}.json"


def write_records(run_dir: Path, summary: ReplaySummary, record: ReleaseRecord | None) -> None:
    (run_dir / SUMMARY_FILE).write_text(json.dumps(summary.as_json(), indent=2, default=str) + "\n", encoding="utf-8")
    if record is not None:
        release_file(run_dir, record.version).write_text(
            json.dumps(asdict(record), indent=2, default=str) + "\n", encoding="utf-8"
        )


def replay(args: Any) -> int:
    """``fastloop gitlab replay``: replay the releases after the lane's chart; exit 0 only when every one passed."""
    from pathlib import Path

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.gitlab_lane import NAMESPACE, lane_upstream

    environment = FastloopEnvironment.load(args.run_dir.resolve())
    if environment.app != "gitlab" or environment.namespace != NAMESPACE:
        raise ValueError(f"{environment.run_dir} is a {environment.app} lane; release replay handles gitlab lanes only")
    releases = load_releases()
    standard = ReplayTimings.standard()
    timings = ReplayTimings(
        verdict=args.verdict_seconds or standard.verdict,
        owner_action=args.owner_action_seconds or standard.owner_action,
        resolution=args.resolution_seconds or standard.resolution,
        settle=args.settle_seconds or standard.settle,
        poll=standard.poll,
    )
    cache = args.archive_cache or Path(tempfile.gettempdir()) / "gitlab-chart-archives"
    ops = KubectlReplayOps(environment, lane_upstream(environment), releases, cache)
    plan = plan_replay(releases, ops.accepted_chart_version(), args.limit)
    for release in plan:  # download and verify every archive before the first push
        fetch_archive(release, cache)
    summary = run_replay(
        ops,
        releases,
        timings,
        lane=environment.cluster,
        limit=args.limit,
        write=lambda s, r: write_records(environment.run_dir, s, r),
    )
    print(json.dumps(summary.as_json(), indent=2, default=str))
    return 0 if summary.passed else 1


def add_gitlab_replay_parser(commands: Any) -> None:
    """Register ``fastloop gitlab replay`` on the ``gitlab`` subcommand's subparsers."""
    from pathlib import Path

    replay_parser = commands.add_parser(
        "replay", help="push successive chart releases to the lane's followed ref and record SDO's outcome for each"
    )
    replay_parser.add_argument("--run-dir", type=Path, required=True)
    replay_parser.add_argument("--limit", type=int, help="replay at most this many releases")
    replay_parser.add_argument("--archive-cache", type=Path, help="where chart archives are downloaded (verified)")
    replay_parser.add_argument(
        "--verdict-seconds", type=float, help="how long SDO has to advance, hold or open an incident"
    )
    replay_parser.add_argument("--owner-action-seconds", type=float, help="how long a hold's owner has to start acting")
    replay_parser.add_argument("--resolution-seconds", type=float, help="how long an acting owner has to finish")
    replay_parser.add_argument("--settle-seconds", type=float, help="quiet window after each adopted release")
    replay_parser.set_defaults(handler=replay)
