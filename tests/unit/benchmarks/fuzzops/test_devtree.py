"""The dev-mounted source tree of a fuzzops lane: what is projected, and what a swap may change."""

from __future__ import annotations

import subprocess
from pathlib import Path  # noqa: TC003

import pytest

from benchmarks.fuzzops import devtree


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t", *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    for rel in (
        "sdo/agents/x.py",
        "sdo/operator/go.mod",
        "sdo/operator/a/a.go",
        "libs/l.py",
        "examples/e.txt",
        "benchmarks/__init__.py",
        "benchmarks/sregym/__init__.py",
        "benchmarks/sregym/fastloop/f.py",
        "benchmarks/fuzzops/hooks.py",
        "tests/t.py",
    ):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n")
    _git(tmp_path, "init", "-q", str(root))
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "base")
    return root


def test_sync_projects_the_pod_sources_and_the_binary(repo: Path, tmp_path: Path) -> None:
    binary = tmp_path / "sdo-operator"
    binary.write_text("bin")
    dest = tmp_path / "dev"
    devtree.sync(repo, dest, binary)
    assert (dest / "sdo/agents/x.py").is_file()
    assert (dest / "libs/l.py").is_file()
    assert (dest / "benchmarks/sregym/fastloop/f.py").is_file()
    assert (dest / ".fastloop/sdo-operator").read_text() == "bin"
    assert not (dest / "tests").exists()
    assert not (dest / "benchmarks/fuzzops").exists()


def test_sync_removes_what_the_new_tree_dropped(repo: Path, tmp_path: Path) -> None:
    binary = tmp_path / "sdo-operator"
    binary.write_text("bin")
    dest = tmp_path / "dev"
    devtree.sync(repo, dest, binary)
    (repo / "sdo/agents/x.py").unlink()
    devtree.sync(repo, dest, binary)
    assert not (dest / "sdo/agents/x.py").exists()


def test_python_only_change_may_be_swapped(repo: Path) -> None:
    lane = devtree.fingerprint(repo)
    (repo / "sdo/agents/x.py").write_text("y\n")
    _git(repo, "commit", "-qam", "py")
    assert devtree.swap_problem(repo, lane) == ""


def test_go_change_cannot_be_swapped(repo: Path) -> None:
    lane = devtree.fingerprint(repo)
    (repo / "sdo/operator/a/a.go").write_text("y\n")
    _git(repo, "commit", "-qam", "go")
    assert "sdo/operator" in devtree.swap_problem(repo, lane)


def test_go_change_can_be_swapped_with_a_rebuilt_binary(repo: Path) -> None:
    lane = devtree.fingerprint(repo)
    (repo / "sdo/operator/a/a.go").write_text("y\n")
    _git(repo, "commit", "-qam", "go")
    assert devtree.binary_changed(repo, lane)
    assert devtree.swap_problem(repo, lane, rebuild_binary=True) == ""
    assert not devtree.binary_changed(repo, devtree.fingerprint(repo))


def test_other_compiled_changes_still_refuse_a_binary_swap(repo: Path) -> None:
    lane = devtree.fingerprint(repo)
    (repo / "docker").mkdir()
    (repo / "docker/f").write_text("y\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "image")
    assert "docker" in devtree.swap_problem(repo, lane, rebuild_binary=True)


def test_operator_binary_cache_key_follows_the_operator_source(repo: Path) -> None:
    from benchmarks.fuzzops import hooks

    before = hooks.operator_cache_key(repo)
    (repo / "sdo/agents/x.py").write_text("changed\n")
    _git(repo, "commit", "-qam", "py")
    assert hooks.operator_cache_key(repo) == before
    (repo / "sdo/operator/a/a.go").write_text("changed\n")
    _git(repo, "commit", "-qam", "go")
    assert hooks.operator_cache_key(repo) != before


def test_a_lane_without_a_fingerprint_cannot_be_swapped(repo: Path) -> None:
    assert devtree.swap_problem(repo, {})


def test_wipe_problems_name_every_way_the_lane_left_the_prepared_state() -> None:
    from benchmarks.fuzzops import hooks

    before = {"head": "a" * 40, "status": "", "outcomes": ""}
    assert hooks.wipe_problems(before, dict(before)) == []
    found = hooks.wipe_problems(before, {"head": "b" * 40, "status": " M .sdo/x", "outcomes": "outcome.json"})
    assert len(found) == 3


def test_swap_receipt_has_the_fields_the_bench_reads_after_prepare(tmp_path: Path) -> None:
    from benchmarks.fuzzops import hooks

    receipt = hooks.swap_receipt(tmp_path, "abc", "m1", 3.14)
    assert {"pack", "operator_version", "memory"} <= receipt.keys()
    assert receipt["pack"] == str((tmp_path / "pack").resolve())
