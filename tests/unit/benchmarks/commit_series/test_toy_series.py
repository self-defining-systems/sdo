"""The toy series chains: each commit applies on the branch left by the one before, through the real replayer."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from benchmarks.commit_series import toy
from benchmarks.common.replay.replay import replay
from benchmarks.common.replay.series import Series

if TYPE_CHECKING:
    from pathlib import Path


def git(path: Path, *args: str) -> str:
    base = ["git", "-c", "user.name=t", "-c", "user.email=t@invalid", "-C", str(path)]
    return subprocess.run([*base, *args], check=True, capture_output=True, text=True).stdout.strip()


def test_each_toy_commit_replays_onto_the_branch_one_at_a_time(tmp_path: Path) -> None:
    series = Series.load(toy.build(tmp_path / "series"))
    remote = tmp_path / "remote.git"
    git(tmp_path, "init", "-q", "--bare", "-b", "sdo-source", str(remote))
    seed = tmp_path / "seed"
    git(tmp_path, "clone", "-q", series.repo, str(seed))
    git(seed, "checkout", "-q", series.base)
    git(seed, "push", "-q", str(remote), "HEAD:refs/heads/sdo-source")
    pushed = []
    for index in (1, 2, 3):
        result = replay(series.config(index, str(remote), "sdo-source"))
        assert [r.status for r in result.records] == ["pushed"]
        pushed.append(result.records[0].pushed_sha)
    log = git(remote, "log", "--format=%s%n%b", "sdo-source")
    assert log.count("Replayed-From:") == 3
    assert [git(remote, "rev-parse", f"sdo-source~{n}") for n in (2, 1, 0)] == pushed
    final = git(remote, "show", "sdo-source:main.go")
    assert "journey ok v2" in final
    assert "not ready" in final


def test_the_toy_spec_says_why_it_is_synthetic(tmp_path: Path) -> None:
    series = Series.load(toy.build(tmp_path))
    assert "synthetic" in series.source
    assert len(series.commits) == 3
