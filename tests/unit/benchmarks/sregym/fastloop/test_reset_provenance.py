"""Reset receipts bind lineage to the actual reset generation, never an old head."""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop.parallel_memory import memory_ancestry
from benchmarks.sregym.fastloop.reset_provenance import publish_reset_generation, read_reset_generation

if TYPE_CHECKING:
    from pathlib import Path


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def test_reset_generation_uses_actual_post_reset_head_and_preserves_old_head(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@example.invalid")
    git(repo, "commit", "--allow-empty", "-qm", "seed")
    seed = git(repo, "rev-parse", "HEAD")
    git(repo, "commit", "--allow-empty", "-qm", "abandoned")
    abandoned = git(repo, "rev-parse", "HEAD")
    git(repo, "reset", "--hard", seed)
    path = tmp_path / "row-000" / "reset-generation.json"
    publish_reset_generation(path, repo, seed, abandoned)
    receipt = read_reset_generation(path, repo)
    assert receipt["accepted_before"] == seed
    assert receipt["pre_reset_head"] == abandoned
    assert receipt["run_id"] == "row-000"
    assert receipt["generation"]
    git(repo, "commit", "--allow-empty", "-qm", "accepted")
    after = git(repo, "rev-parse", "HEAD")
    assert memory_ancestry(repo, receipt["accepted_before"], after) == [after]
    with pytest.raises(ValueError, match="rewound"):
        memory_ancestry(repo, abandoned, after)
    # A later actual rewind must still fail the same lineage gate.
    git(repo, "checkout", "--orphan", "unrelated")
    git(repo, "commit", "--allow-empty", "-qm", "unrelated")
    with pytest.raises(ValueError, match="rewound"):
        memory_ancestry(repo, receipt["accepted_before"], git(repo, "rev-parse", "HEAD"))
    saved = json.loads(path.read_text())
    saved["run_id"] = "another-generation"
    path.write_text(json.dumps(saved))
    with pytest.raises(ValueError, match="identity"):
        read_reset_generation(path, repo)


def test_failed_reset_cannot_publish_planned_head_as_observed(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@example.invalid")
    git(repo, "commit", "--allow-empty", "-qm", "actual")
    path = tmp_path / "row" / "reset-generation.json"
    with pytest.raises((ValueError, subprocess.CalledProcessError)):
        publish_reset_generation(path, repo, "0" * 40, git(repo, "rev-parse", "HEAD"))
    assert not path.exists()
