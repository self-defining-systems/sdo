"""Real Git's ownership check applies to every shared-checkout host boundary."""

import subprocess

import pytest

from benchmarks.sregym.fastloop import parallel_memory, shared_memory
from benchmarks.sregym.fastloop.accepted_source import AcceptedSource
from tests.unit.benchmarks.sregym.fastloop.test_parallel_prepare import _commit, _git


@pytest.mark.parametrize("name", ["accepted", "accepted quote' space"])
@pytest.mark.parametrize("consumer", ["head", "ancestry", "lane-match", "accepted-source", "clone", "replay-clone"])
def test_shared_checkout_uses_scoped_trust(monkeypatch, tmp_path, consumer, name):
    # CI may trust every checkout globally. Exercise real Git ownership checks
    # independently of the runner's system/global and environment config.
    empty_config = tmp_path / "empty.gitconfig"
    empty_config.write_text("")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(empty_config))
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", str(empty_config))
    monkeypatch.setenv("GIT_CONFIG_COUNT", "0")
    monkeypatch.delenv("GIT_CONFIG_PARAMETERS", raising=False)
    accepted = tmp_path / name
    accepted.mkdir()
    _git(accepted, "init", "-q")
    before = _commit(accepted, {"README.md": "synthetic\n"}, "seed")
    after = _commit(accepted, {"README.md": "synthetic learned\n"}, "accepted")
    # Git's own test seam executes its real ownership rejection without root/chown.
    monkeypatch.setenv("GIT_TEST_ASSUME_DIFFERENT_OWNER", "1")
    rejected = subprocess.run(["git", "-C", str(accepted), "rev-parse", "HEAD"], capture_output=True, text=True)
    assert rejected.returncode != 0
    assert "dubious ownership" in rejected.stderr
    if consumer == "head":
        assert parallel_memory.memory_head(accepted) == after
    elif consumer == "ancestry":
        assert parallel_memory.memory_ancestry(accepted, before, after) == [after]
    elif consumer == "lane-match":
        monkeypatch.setattr(shared_memory, "lane_git_command", lambda *args: ("echo", _git_trusted_tree(accepted)))
        assert shared_memory.lane_match(object(), "/lane", accepted, after) == after
    elif consumer == "accepted-source":
        assert AcceptedSource(accepted, "synthetic").revision == after
    elif consumer == "clone":
        shared_memory._clone_accepted(accepted, tmp_path / "copy")
    else:
        from benchmarks.sregym.fastloop.replay_worker import _clone_at

        _clone_at(accepted, tmp_path / "copy", after)
    # No global trust and no wildcard: an unrelated invocation remains rejected.
    assert subprocess.run(["git", "-C", str(accepted), "status"], capture_output=True).returncode != 0


def _git_trusted_tree(repository):
    return subprocess.check_output(
        ["git", "-c", f"safe.directory={repository}", "-C", str(repository), "rev-parse", "HEAD^{tree}"], text=True
    ).strip()


def _unscoped_checkout_commands(source):
    import ast

    tree = ast.parse(source)
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.List) or not node.elts:
            continue
        first = node.elts[0]
        if not isinstance(first, ast.Constant) or first.value != "git":
            continue
        if not any(isinstance(item, ast.Constant) and item.value == "-C" for item in node.elts):
            continue
        if isinstance(parents.get(node), ast.Compare):
            continue  # Inspecting a recorded command produces no Git process.
        violations.append(node.lineno)
    return violations


@pytest.mark.parametrize(
    "source",
    [
        'subprocess.run(["git", "-C", str(repository), "status"])',
        'command = ["git", "-c", "safe.directory=*", "-C", str(repository), *args]',
    ],
)
def test_unscoped_host_git_negative_controls(source):
    assert _unscoped_checkout_commands(source)


def test_all_benchmark_host_checkout_commands_use_the_scoped_policy():
    from pathlib import Path

    root = Path(__file__).resolve().parents[5]
    violations = []
    for package in ("adapter", "fastloop"):
        for path in (root / "benchmarks/sregym" / package).glob("*.py"):
            if path.name == "repository_git.py":
                continue  # The single policy owner is exercised by real Git above.
            violations.extend(
                f"{path.relative_to(root)}:{line}" for line in _unscoped_checkout_commands(path.read_text())
            )
    assert not violations, "Use repository_git.host_git_command for host checkout commands: " + ", ".join(violations)
