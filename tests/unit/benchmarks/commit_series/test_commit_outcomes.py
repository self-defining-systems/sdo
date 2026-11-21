"""One commit's record: accepted, held, incident, timeout and conflict, against a scripted lane (no cluster)."""

from __future__ import annotations

from dataclasses import dataclass, field

from benchmarks.commit_series.scenario import Bounds, run_commit
from benchmarks.common.replay.replay import ReplayRecord
from benchmarks.common.replay.series import Series
from benchmarks.sregym.fastloop.toy_operability import OperabilityRead
from benchmarks.sregym.fastloop.toy_scenarios import AcceptedCommit, HoldRecord, Snapshot

SERIES = Series(
    name="demo",
    description="three synthetic commits",
    source="synthetic",
    repo="/unused",
    base="b0",
    commits=("c1", "c2", "c3"),
)
BOUNDS = Bounds(verdict=60, steady=10, resolution=30, poll=5)
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
    deployed: bool = True

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

    def operability(self) -> OperabilityRead | None:
        if not self.deployed:
            return None
        return OperabilityRead("base", "complete", "sha256:x", (), "base", "complete", {})

    def agent_task_count(self) -> int:
        return self.tasks


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


def test_a_seeded_install_with_no_recorded_deployment_cannot_run_a_series() -> None:
    """SDO deploys a branch head only when it recorded the deployment of the commit before it. A seeded install
    (install-existing) has none, so the push would wait out the whole verdict bound for nothing: the run is invalid,
    names the cause and pushes nothing."""
    lane = Lane("accepted", deployed=False)
    got = run_commit(lane, SERIES, 1, BOUNDS, "exec:sdo-demo-r1")  # type: ignore[arg-type]
    assert got["valid"] is False
    assert "recorded deployment" in str(got["invalid_reason"])
    assert lane.pushed == ""
