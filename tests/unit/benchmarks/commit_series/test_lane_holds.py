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
