"""One commit's record: accepted, held, incident, timeout and conflict, against a scripted lane (no cluster)."""

from __future__ import annotations

from dataclasses import dataclass, field

from benchmarks.commit_series.scenario import Bounds, run_commit
from benchmarks.common.replay.replay import ReplayRecord
from benchmarks.common.replay.series import Series
from benchmarks.sregym.fastloop.toy_scenarios import AcceptedCommit, HoldRecord, Snapshot

SERIES = Series(
    name="demo",
    description="three synthetic commits",
    source="synthetic",
    repo="/unused",
    base="b0",
    commits=("c1", "c2", "c3"),
)
BOUNDS = Bounds(verdict=60, steady=10, resolution=30, poll=5, steady_between=10)
HOLD = "cause=rollout owner=responder items=web rollout: web did not become ready"


@dataclass
class Lane:
    """What SDO does with the next push: one of accepted, held, incident, ignored (nothing happens)."""

    fate: str
    status: str = "pushed"
    t: float = 0.0
    commits: list[AcceptedCommit] = field(default_factory=list)
    hold_list: list[HoldRecord] = field(default_factory=list)
    incident: bool = False
    tasks: int = 0
    pushed: str = ""
    incident_closes_at: float = 1e9

    def clock(self) -> float:
        return self.t

    def now(self) -> float:
        return 1_700_000_000.0 + self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds
        if self.incident and self.t >= self.incident_closes_at:
            self.incident = False

    def push_commit(self, index: int) -> ReplayRecord:
        if self.status != "pushed":
            return ReplayRecord(f"c{index}", self.status, ("app.txt",))  # type: ignore[arg-type]
        self.pushed = f"sha{index}"
        if self.fate == "accepted":
            self.commits.insert(0, AcceptedCommit(f"acc{index}", {"SDO-Upstream": self.pushed}))
        elif self.fate == "held":
            self.hold_list.append(HoldRecord(self.pushed, HOLD))
        elif self.fate == "incident":
            self.incident, self.incident_closes_at, self.tasks = True, self.t + 20, 2
        return ReplayRecord(f"c{index}", "pushed", pushed_sha=self.pushed, attempts=1)

    def accepted_commits(self) -> list[AcceptedCommit]:
        return self.commits

    def holds(self) -> list[HoldRecord]:
        return self.hold_list

    def snapshot(self) -> Snapshot:
        return Snapshot(head="", images={}, digests={}, agent_tasks=self.tasks, pod_failures=0, controller_relaunches=0)

    def incident_open(self) -> bool:
        return self.incident

    def operability(self) -> None:
        return None

    def agent_task_count(self) -> int:
        return self.tasks

    def head_deployment(self) -> None:
        return None


def record(fate: str, **kw: str) -> dict[str, object]:
    return run_commit(Lane(fate, **kw), SERIES, 2, BOUNDS, "exec:sdo-demo-r2")  # type: ignore[arg-type]


def test_an_accepted_commit_records_its_deploy_time_and_accepted_head() -> None:
    got = record("accepted")
    assert got["outcome"] == "accepted"
    assert got["accepted_commit"] == "acc2"
    assert got["pushed"] == "sha2"
    assert got["valid"] is True
    assert got["incident_opened"] is False
    assert got["series"] == "demo"
    assert got["index"] == 2


def test_a_held_commit_is_a_valid_result_with_the_holds_routing() -> None:
    got = record("held")
    assert got["outcome"] == "held"
    assert got["valid"] is True
    assert got["hold"]["owner"] == "responder"  # type: ignore[index]
    assert got["hold"]["cause"] == "rollout"  # type: ignore[index]
    assert got["accepted_commit"] == ""


def test_an_incident_is_recorded_with_the_agent_tasks_it_took_and_whether_it_closed() -> None:
    got = record("incident")
    assert got["outcome"] == "incident"
    assert got["incident_opened"] is True
    assert got["incident_resolved"] is True
    assert got["agent_tasks"] == 2


def test_a_commit_nobody_answers_times_out_and_stays_valid() -> None:
    got = record("ignored")
    assert got["outcome"] == "timeout"
    assert got["valid"] is True


def test_a_commit_that_does_not_apply_is_a_conflict_result_not_a_failure() -> None:
    got = record("accepted", status="conflict")
    assert got["outcome"] == "conflict"
    assert got["conflict_paths"] == ["app.txt"]
    assert got["valid"] is True
    assert got["lifecycle"]["inject_at"].endswith("Z")  # type: ignore[index]


def test_an_incident_open_before_the_push_is_not_the_verdict_on_the_push() -> None:
    """Finding S3: SDO deploys nothing while an incident is open; the position that starts under one was graded
    ``incident`` at once and its own push was never judged. It is a timeout that names the blocking incident."""
    lane = Lane("ignored", incident=True)
    got = run_commit(lane, SERIES, 2, BOUNDS, "exec:sdo-demo-r2")  # type: ignore[arg-type]
    assert got["outcome"] == "timeout"
    assert got["blocked_by_open_incident"] is True
    assert got["valid"] is True


def test_a_push_made_with_no_incident_open_still_ends_on_the_incident_it_opens() -> None:
    got = record("incident")
    assert got["outcome"] == "incident"
    assert got["blocked_by_open_incident"] is False


class LateIncidentLane(Lane):
    """Accepted at once; 8 s later an incident opens (a failing queue's retries) and closes 40 s after the push."""

    def incident_open(self) -> bool:
        return self.pushed != "" and self.opens <= self.t - self.pushed_at < self.closes

    opens: float = 8.0
    closes: float = 40.0
    pushed_at: float = 0.0

    def push_commit(self, index: int) -> ReplayRecord:
        self.pushed_at = self.t
        return super().push_commit(index)


def test_a_position_another_follows_waits_until_sdo_is_quiet_not_for_a_fixed_window() -> None:
    """The next push must not meet the incident the last release opened a few seconds after its verdict: the quiet
    window of a non-final position ends when no incident has been open for ``steady_between`` seconds."""
    bounds = Bounds(verdict=60, steady=10, resolution=300, poll=5, steady_between=10)
    lane = LateIncidentLane("accepted")
    got = run_commit(lane, SERIES, 1, bounds.at(1, 3), "exec:sdo-demo-r1")  # type: ignore[arg-type]
    assert got["incident_opened"] is True
    assert lane.t - lane.pushed_at >= LateIncidentLane.closes + bounds.steady_between, lane.t
    assert not lane.incident_open()
    # The last position keeps the fixed window: it does not wait for an incident to go quiet after it.
    last = LateIncidentLane("accepted")
    got = run_commit(last, SERIES, 3, bounds.at(3, 3), "exec:sdo-demo-r3")  # type: ignore[arg-type]
    assert last.t - last.pushed_at < LateIncidentLane.closes + bounds.steady_between


def test_a_release_that_keeps_opening_incidents_does_not_hold_the_series_past_the_quiet_cap() -> None:
    """10.6.0 opens a chain of incidents for half an hour; the next push waited all of it (a position cycle of 42
    minutes on EKS). The wait for quiet is capped, and the record says SDO was not quiet."""

    class Chain(LateIncidentLane):
        closes = 10_000.0  # never quiet

    bounds = Bounds(verdict=60, steady=10, resolution=900, poll=5, steady_between=10, quiet_cap=60)
    lane = Chain("accepted")
    got = run_commit(lane, SERIES, 1, bounds.at(1, 3), "exec:sdo-demo-r1")  # type: ignore[arg-type]
    assert lane.t - lane.pushed_at <= 60 + 15, lane.t
    assert got["noisy_before_next"] is True
    assert got["incident_opened"] is True
