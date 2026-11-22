"""The lane's hold read: SDO's head hold (``sdo-head-hold``) is matched to the commit the replayer pushed.

SDO publishes its own commit on the branch before it judges a push (a health refresh), so the hold names that commit,
never the pushed one; the pushed commit is its ancestor. The cluster is faked at the two commands the reads run
(``kubectl get configmap``, ``kubectl exec git merge-base``).
"""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.commit_series.adapter import LaneOps
from benchmarks.common.replay.series import Series

if TYPE_CHECKING:
    from pathlib import Path

PUSHED = "53e41cd" + "0" * 33
REFRESH = "ab30f61" + "0" * 33
HOLD = {"commit": REFRESH, "reason": "rollout", "detail": "workload Job/fg1/m-1a is missing from deployment plan"}


def series() -> Series:
    return Series(name="demo", description="d", source="s", repo="/unused", base="b0", commits=("c1",))


def lane(tmp_path: Path) -> LaneOps:
    (tmp_path / "source-branch.json").write_text(json.dumps({"namespace": "fg1", "ref": "sdo-source"}))
    return LaneOps(tmp_path, tmp_path / "kubeconfig", series())


class Cluster:
    """kubectl against a lane whose control namespace holds ``configmaps`` and whose branch has ``ancestry``."""

    def __init__(self, configmaps: dict[str, dict], ancestry: set[tuple[str, str]]) -> None:
        self.configmaps, self.ancestry, self.calls = configmaps, ancestry, []

    def __call__(self, namespace: str, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        self.calls.append((namespace, args))
        if args[:2] == ("get", "configmap"):
            data = self.configmaps.get(args[2])
            if data is None:
                return subprocess.CompletedProcess(args, 1, "", f'Error from server (NotFound): configmaps "{args[2]}"')
            return subprocess.CompletedProcess(args, 0, json.dumps({"data": data}), "")
        if args[0] == "exec" and "merge-base" in args:
            ancestor, commit = args[-2:]
            return subprocess.CompletedProcess(args, 0 if (ancestor, commit) in self.ancestry else 1, "", "")
        raise AssertionError(f"unexpected kubectl call {args}")


def test_a_hold_on_sdos_commit_over_the_pushed_one_is_the_pushed_commits_hold(tmp_path: Path) -> None:
    ops = lane(tmp_path)
    ops._kubectl = Cluster({"sdo-head-hold": {"hold": json.dumps(HOLD)}}, {(PUSHED, REFRESH)})  # type: ignore[method-assign]
    ops.pushed = [PUSHED]  # type: ignore[attr-defined]
    holds = ops.holds()
    assert [h.revision for h in holds] == [PUSHED]
    assert "rollout" in holds[0].reason
    assert "Job/fg1/m-1a is missing" in holds[0].reason


def test_a_hold_on_a_commit_the_push_is_not_under_is_not_its_hold(tmp_path: Path) -> None:
    ops = lane(tmp_path)
    ops._kubectl = Cluster({"sdo-head-hold": {"hold": json.dumps(HOLD)}}, set())  # type: ignore[method-assign]
    ops.pushed = [PUSHED]  # type: ignore[attr-defined]
    assert ops.holds() == []


def test_no_hold_record_is_no_hold(tmp_path: Path) -> None:
    ops = lane(tmp_path)
    ops._kubectl = Cluster({}, set())  # type: ignore[method-assign]
    ops.pushed = [PUSHED]  # type: ignore[attr-defined]
    assert ops.holds() == []


def test_a_git_failure_other_than_not_an_ancestor_is_loud(tmp_path: Path) -> None:
    ops = lane(tmp_path)
    cluster = Cluster({"sdo-head-hold": {"hold": json.dumps(HOLD)}}, set())

    def broken(namespace: str, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        if args[0] == "exec":
            return subprocess.CompletedProcess(args, 125, "", "error: unable to upgrade connection")
        return cluster(namespace, *args, check=check)

    ops._kubectl = broken  # type: ignore[method-assign]
    ops.pushed = [PUSHED]  # type: ignore[attr-defined]
    with pytest.raises(RuntimeError, match="unable to upgrade connection"):
        ops.holds()


# ---- a deployed head: SDO adds no commit, so the notice it keeps is the proof ----------------------------------------

NOTICE = {
    "commit": REFRESH,
    "effective_tree": "t1",
    "patches_digest": "d1",
    "verdict": "clear",
    "at": "2026-11-22T04:00:00Z",
}


def test_a_head_deployed_by_sdo_is_an_accepted_commit_of_the_push_under_it(tmp_path: Path) -> None:
    """Finding S1: a head deploy adds no commit and no SDO-Upstream trailer, so ``accepted`` was never observable and
    every deployed release was graded ``timeout``. SDO's notice (``sdo-head-deployment``) names the head it deployed
    (its own commit on top of the push); the push is its ancestor."""
    ops = lane(tmp_path)
    ops._kubectl = Cluster(  # type: ignore[method-assign]
        {"sdo-head-deployment": {"deployment": json.dumps(NOTICE)}}, {(PUSHED, REFRESH)}
    )
    ops.pushed = [PUSHED]  # type: ignore[attr-defined]
    ops._accepted = lambda *a: ""  # type: ignore[method-assign]
    found = [c for c in ops.accepted_commits() if c.trailers.get("SDO-Upstream") == PUSHED]
    assert [c.sha for c in found] == [REFRESH]
    assert ops.head_deployment() == NOTICE


def test_a_deployment_of_another_push_is_not_this_pushs_acceptance(tmp_path: Path) -> None:
    ops = lane(tmp_path)
    ops._kubectl = Cluster({"sdo-head-deployment": {"deployment": json.dumps(NOTICE)}}, set())  # type: ignore[method-assign]
    ops.pushed = [PUSHED]  # type: ignore[attr-defined]
    ops._accepted = lambda *a: ""  # type: ignore[method-assign]
    assert ops.accepted_commits() == []


def test_a_controller_that_is_relaunching_does_not_end_the_wait(tmp_path: Path) -> None:
    """Finding S7: SDO restarts its controller after a head deploy (the health refresh); a read in the gap raised and
    invalidated the position. The last answer stands until the controller is back."""
    ops = lane(tmp_path)
    ops._kubectl = Cluster({}, set())  # type: ignore[method-assign]
    ops.pushed = []  # type: ignore[attr-defined]
    answers = iter(["", RuntimeError("no running controller pod")])

    def accepted(*args: str) -> str:
        answer = next(answers)
        if isinstance(answer, Exception):
            raise answer
        return answer

    ops._accepted = accepted  # type: ignore[method-assign]
    assert ops.accepted_commits() == []
    assert ops.accepted_commits() == []  # the relaunch gap


def test_a_lane_still_starting_gets_time_before_the_series_is_refused(tmp_path: Path) -> None:
    """Finding S9: the start proof ran once; a controller still finishing its start made the whole series invalid."""
    ops = lane(tmp_path)
    answers = iter([["the operator is not quiet: busy: startup"], []])
    ops._start_problems = lambda env, lane_dir: next(answers)  # type: ignore[method-assign]
    slept: list[float] = []
    ops.sleep = slept.append  # type: ignore[method-assign]
    assert ops.start_problems({}, tmp_path) == []
    assert slept == [15.0]
    ops._start_problems = lambda env, lane_dir: ["still busy"]  # type: ignore[method-assign]
    ops.clock = iter([0.0, 100.0, 400.0]).__next__  # type: ignore[method-assign]
    assert ops.start_problems({}, tmp_path, patience=300.0) == ["still busy"]
