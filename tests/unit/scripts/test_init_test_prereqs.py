"""Prerequisite setup preserves recorded Git pins, including fork-only objects."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/init_test_prereqs.py"
MESSAGE = "SDO_TEST_PREREQUISITES_MISSING"


def git(directory: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(directory), *args], text=True, stderr=subprocess.PIPE).strip()


def repository(directory: Path) -> Path:
    directory.mkdir()
    git(directory, "init", "-q")
    git(directory, "config", "user.email", "fixture@example.com")
    git(directory, "config", "user.name", "Fixture")
    (directory / "file").write_text("first\n")
    commit(directory)
    return directory


def commit(directory: Path) -> str:
    git(directory, "add", ".")
    git(directory, "commit", "-qm", "fixture")
    return git(directory, "rev-parse", "HEAD")


def pin(parent: Path, path: str, remote: Path, sha: str) -> None:
    modules = parent / ".gitmodules"
    with modules.open("a") as stream:
        stream.write(f'[submodule "{path}"]\n\tpath = {path}\n\turl = {remote}\n')
    git(parent, "add", ".gitmodules")
    git(parent, "update-index", "--add", "--cacheinfo", f"160000,{sha},{path}")
    git(parent, "commit", "-qm", "pin")


def run(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--repository", str(root), *args],
        text=True,
        capture_output=True,
        env={**os.environ, "GIT_ALLOW_PROTOCOL": "file"},
        timeout=30,
    )


def test_uninitialized_checkout_fails_with_one_named_actionable_message(tmp_path: Path) -> None:
    upstream = repository(tmp_path / "upstream")
    root = repository(tmp_path / "root")
    pin(root, "third_party/sregym", upstream, git(upstream, "rev-parse", "HEAD"))
    result = run(root, "--check")
    assert result.returncode != 0
    assert MESSAGE in result.stderr
    assert "uv run python scripts/init_test_prereqs.py" in result.stderr
    assert "third_party/sregym" in result.stderr


def test_initializes_all_nested_pins_idempotently_without_changing_gitlinks(tmp_path: Path) -> None:
    chart = repository(tmp_path / "chart")
    app = repository(tmp_path / "app")
    chart_sha = git(chart, "rev-parse", "HEAD")
    pin(app, "charts/fixture", chart, chart_sha)
    harness = repository(tmp_path / "harness")
    pin(harness, "applications", app, git(app, "rev-parse", "HEAD"))
    root = repository(tmp_path / "root")
    sha = git(harness, "rev-parse", "HEAD")
    pin(root, "third_party/sregym", harness, sha)
    before = git(root, "ls-tree", "-r", "HEAD")
    for _ in range(2):
        result = run(root)
        assert result.returncode == 0, result.stderr
        assert run(root, "--check").returncode == 0
    assert git(root / "third_party/sregym", "rev-parse", "HEAD") == sha
    assert git(root / "third_party/sregym/applications/charts/fixture", "rev-parse", "HEAD") == chart_sha
    assert git(root, "ls-tree", "-r", "HEAD") == before
    assert git(root, "status", "--porcelain") == ""


def test_missing_sha_is_fetched_from_fork_without_moving_pin(tmp_path: Path) -> None:
    configured = repository(tmp_path / "configured")
    fork_base = tmp_path / "forks"
    fork_base.mkdir()
    fork = fork_base / "configured.git"
    git(tmp_path, "clone", "-q", str(configured), str(fork))
    git(fork, "config", "user.email", "fixture@example.com")
    git(fork, "config", "user.name", "Fixture")
    (fork / "fork-only").write_text("fork-only commit\n")
    wanted = commit(fork)
    root = repository(tmp_path / "root")
    pin(root, "third_party/sregym", configured, wanted)
    result = run(root, "--fork-base-url", str(fork_base))
    assert result.returncode == 0, result.stderr
    assert git(root / "third_party/sregym", "rev-parse", "HEAD") == wanted
    assert git(root, "status", "--porcelain") == ""


def test_pytest_stops_once_before_running_prerequisite_dependent_tests(tmp_path: Path) -> None:
    upstream = repository(tmp_path / "upstream")
    root = repository(tmp_path / "root")
    pin(root, "third_party/sregym", upstream, git(upstream, "rev-parse", "HEAD"))
    (root / "test_needs_harness.py").write_text(
        "import pytest\npytestmark = pytest.mark.requires_sregym\n"
        'def test_one(): assert False, "TEST_RAN"\n'
        'def test_two(): assert False, "TEST_RAN"\n'
    )
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "tests.sregym_prerequisites", str(root)],
        cwd=root,
        env={**os.environ, "PYTHONPATH": str(ROOT), "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == pytest.ExitCode.USAGE_ERROR
    assert result.stderr.count(MESSAGE) == 1, result.stdout + result.stderr
    assert "TEST_RAN" not in result.stdout


def test_fetches_exact_missing_sha_from_configured_remote(tmp_path: Path) -> None:
    harness = repository(tmp_path / "harness")
    root = repository(tmp_path / "root")
    pin(root, "third_party/sregym", harness, git(harness, "rev-parse", "HEAD"))
    assert run(root).returncode == 0
    (harness / "next").write_text("new recorded commit\n")
    wanted = commit(harness)
    git(root, "update-index", "--cacheinfo", f"160000,{wanted},third_party/sregym")
    git(root, "commit", "-qm", "advance recorded pin")
    assert run(root, "--check").returncode != 0
    result = run(root)
    assert result.returncode == 0, result.stderr
    assert git(root / "third_party/sregym", "rev-parse", "HEAD") == wanted


def test_preserves_local_edits_when_recorded_pin_changes(tmp_path: Path) -> None:
    harness = repository(tmp_path / "harness")
    root = repository(tmp_path / "root")
    pin(root, "third_party/sregym", harness, git(harness, "rev-parse", "HEAD"))
    assert run(root).returncode == 0
    child = root / "third_party/sregym"
    original = git(child, "rev-parse", "HEAD")
    (child / "file").write_text("local work\n")
    (harness / "next").write_text("next\n")
    wanted = commit(harness)
    git(root, "update-index", "--cacheinfo", f"160000,{wanted},third_party/sregym")
    git(root, "commit", "-qm", "advance recorded pin")
    result = run(root)
    assert result.returncode != 0
    assert "refusing to overwrite local changes" in result.stderr
    assert (child / "file").read_text() == "local work\n"
    assert git(child, "rev-parse", "HEAD") == original


def test_unavailable_clone_uses_fork_at_the_same_recorded_sha(tmp_path: Path) -> None:
    forks = tmp_path / "forks"
    forks.mkdir()
    fork = repository(forks / "harness.git")
    root = repository(tmp_path / "root")
    wanted = git(fork, "rev-parse", "HEAD")
    pin(root, "third_party/sregym", tmp_path / "missing/harness.git", wanted)
    result = run(root, "--fork-base-url", str(forks))
    assert result.returncode == 0, result.stderr
    assert git(root / "third_party/sregym", "rev-parse", "HEAD") == wanted


def test_nested_uninitialized_checkout_is_reported_even_when_harness_is_ready(tmp_path: Path) -> None:
    app = repository(tmp_path / "app")
    harness = repository(tmp_path / "harness")
    pin(harness, "applications", app, git(app, "rev-parse", "HEAD"))
    root = repository(tmp_path / "root")
    pin(root, "third_party/sregym", harness, git(harness, "rev-parse", "HEAD"))
    git(root, "-c", "protocol.file.allow=always", "submodule", "update", "--init", "third_party/sregym")
    result = run(root, "--check")
    assert result.returncode != 0
    assert MESSAGE in result.stderr
    assert "third_party/sregym/applications: uninitialized" in result.stderr
