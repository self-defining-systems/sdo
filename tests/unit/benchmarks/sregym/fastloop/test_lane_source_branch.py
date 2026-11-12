"""A lane's SDO commits to, and deploys from, the branch the benchmark replays commits onto."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop import gitlab_lane
from benchmarks.sregym.fastloop import toy_upstream as up
from tests.unit.benchmarks.sregym.fastloop.test_toy_upstream import LAYOUT, git, seeded

if TYPE_CHECKING:
    from pathlib import Path


def test_the_gitlab_lane_points_sdo_at_the_branch_the_replay_pushes_to(tmp_path: Path) -> None:
    environment = SimpleNamespace(
        workspace=tmp_path,
        namespace=gitlab_lane.NAMESPACE,
        application=gitlab_lane.APPLICATION,
        images=SimpleNamespace(controller="controller:x", responder="responder:x"),
        cluster="fastloop-w0",
        evidence_root=None,
    )
    config = gitlab_lane.lane_runtime_config(environment, {"plan": 1})
    layout = gitlab_lane.lane_upstream(environment)
    assert config.source_branch_url == layout.cluster_url
    assert config.source_branch == layout.ref
    assert config.source_branch not in {"main", "master"}


def test_a_replayed_commit_is_an_engineers_push_to_that_branch(tmp_path: Path) -> None:
    workspace, remote = seeded(tmp_path)
    up.seed_upstream(workspace, remote, LAYOUT)
    sha = up.replay_commit("benign", remote, LAYOUT)
    remote_dir = tmp_path / "remote.git"
    assert git(remote_dir, "rev-parse", f"refs/heads/{LAYOUT.ref}") == sha
    assert git(remote_dir, "log", "-1", "--format=%an <%ae>", sha) == "bench-replayer <bench-replayer@localhost>"
    assert "Replayed-From:" in git(remote_dir, "log", "-1", "--format=%B", sha)


def test_the_replayer_never_touches_the_sdo_directory(tmp_path: Path) -> None:
    workspace, remote = seeded(tmp_path)
    up.seed_upstream(workspace, remote, LAYOUT)
    sha = up.replay_commit("benign", remote, LAYOUT)
    changed = git(tmp_path / "remote.git", "diff", "--name-only", f"{sha}^", sha).splitlines()
    assert changed
    assert not any(path == ".sdo" or path.startswith(".sdo/") for path in changed)


@pytest.mark.parametrize("branch", ["main", "master"])
def test_a_lane_cannot_use_a_reserved_branch(branch: str) -> None:
    from benchmarks.sregym.adapter.runtime import RuntimeConfig

    with pytest.raises(ValueError, match="main or master"):
        RuntimeConfig.__post_init__(SimpleNamespace(source_branch_url="git://x/y", source_branch=branch))
