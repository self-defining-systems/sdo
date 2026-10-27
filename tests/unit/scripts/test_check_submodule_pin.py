"""The SREGym pin check judges real Git history: a branch may keep or advance the base pin, never revert it."""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/check_submodule_pin.py"
PIN = "third_party/sregym"


def git(directory: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(directory), *args], text=True, stderr=subprocess.PIPE).strip()


def init(directory: Path) -> Path:
    directory.mkdir()
    git(directory, "init", "-q", "-b", "main")
    git(directory, "config", "user.email", "fixture@example.com")
    git(directory, "config", "user.name", "Fixture")
    git(directory, "config", "uploadpack.allowAnySHA1InWant", "true")
    return directory


def commit(directory: Path, name: str) -> str:
    (directory / name).write_text(name)
    git(directory, "add", ".")
    git(directory, "commit", "-qm", name)
    return git(directory, "rev-parse", "HEAD")


@dataclass
class World:
    fork: Path
    parent: Path
    first: str
    second: str
    side: str

    def pin(self, sha: str, message: str = "pin") -> str:
        git(self.parent, "update-index", "--add", "--cacheinfo", f"160000,{sha},{PIN}")
        git(self.parent, "commit", "-qm", message)
        return git(self.parent, "rev-parse", "HEAD")

    def run(self, *args: str, cwd: Path | None = None, **env: str) -> subprocess.CompletedProcess[str]:
        clean = {k: v for k, v in os.environ.items() if k != "GITHUB_BASE_REF"}
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--repository", str(cwd or self.parent), "--remote", str(self.fork), *args],
            text=True,
            capture_output=True,
            env={**clean, **env},
        )


@pytest.fixture
def world(tmp_path: Path) -> World:
    fork = init(tmp_path / "fork")
    first = commit(fork, "first")
    second = commit(fork, "second")
    git(fork, "checkout", "-q", "-b", "side", first)
    side = commit(fork, "side")
    parent = init(tmp_path / "parent")
    (parent / ".gitmodules").write_text(f'[submodule "{PIN}"]\n\tpath = {PIN}\n\turl = {fork}\n')
    commit(parent, "root")
    return World(fork, parent, first, second, side)


def test_unchanged_pin_passes(world: World) -> None:
    world.pin(world.second)
    git(world.parent, "branch", "base")
    git(world.parent, "commit", "-q", "--allow-empty", "-m", "work")
    result = world.run("--base", "base")
    assert result.returncode == 0, result.stderr
    assert "unchanged" in result.stdout


def test_advanced_pin_passes(world: World) -> None:
    world.pin(world.first)
    git(world.parent, "branch", "base")
    world.pin(world.second)
    result = world.run("--base", "base")
    assert result.returncode == 0, result.stderr
    assert "advanced" in result.stdout


def test_reverted_pin_fails_with_the_restore_command(world: World) -> None:
    world.pin(world.second)
    git(world.parent, "branch", "base")
    world.pin(world.first, "merge kept the older pin")
    result = world.run("--base", "base")
    assert result.returncode == 1
    assert world.second in result.stderr and "git update-index --cacheinfo" in result.stderr


def test_diverged_pin_fails(world: World) -> None:
    world.pin(world.second)
    git(world.parent, "branch", "base")
    world.pin(world.side)
    assert world.run("--base", "base").returncode == 1


def test_head_pin_missing_from_the_fork_fails(world: World) -> None:
    world.pin(world.first)
    git(world.parent, "branch", "base")
    world.pin("1" * 40)
    result = world.run("--base", "base")
    assert result.returncode == 1
    assert "not fetchable" in result.stderr


def test_unfetchable_base_pin_skips_cleanly(world: World) -> None:
    world.pin("2" * 40)
    git(world.parent, "branch", "base")
    world.pin(world.first)
    result = world.run("--base", "base")
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("SKIP")


def test_missing_base_branch_skips(world: World) -> None:
    world.pin(world.first)
    result = world.run("--base", "no-such-branch")
    assert result.returncode == 0 and result.stdout.startswith("SKIP")


def test_default_base_must_be_contained_in_head(world: World) -> None:
    world.pin(world.first)
    result = world.run()
    assert result.returncode == 0 and result.stdout.startswith("SKIP")


def test_shallow_pull_request_checkout_fetches_the_base_branch(world: World, tmp_path: Path) -> None:
    """Hosted CI checks out one merge commit with no history and names the base only in GITHUB_BASE_REF."""
    world.pin(world.second)
    git(world.parent, "branch", "dev")
    git(world.parent, "checkout", "-q", "-b", "feature")
    world.pin(world.first, "merge kept the older pin")
    shallow = tmp_path / "shallow"
    subprocess.run(
        ["git", "clone", "-q", "--depth=1", "--branch", "feature", f"file://{world.parent}", str(shallow)], check=True
    )
    assert git(shallow, "rev-parse", "--is-shallow-repository") == "true"
    failing = world.run(cwd=shallow, GITHUB_BASE_REF="dev")
    assert failing.returncode == 1 and world.second in failing.stderr
    world.pin(world.second, "restored")
    fixed = tmp_path / "fixed"
    subprocess.run(
        ["git", "clone", "-q", "--depth=1", "--branch", "feature", f"file://{world.parent}", str(fixed)], check=True
    )
    assert world.run(cwd=fixed, GITHUB_BASE_REF="dev").returncode == 0


def test_check_errors_runs_the_pin_check() -> None:
    assert "check_submodule_pin.py" in (ROOT / "scripts/check_errors.sh").read_text()
