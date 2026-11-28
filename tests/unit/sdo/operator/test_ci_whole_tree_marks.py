"""The real repository's whole-tree guard marks (see ``test_ci_selection.py`` for the selector's own rules).

This file scans the real tree, so it is itself a whole-tree guard: a pull request that drops a mark runs it.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

CI_WHOLE_TREE = True

ROOT = Path(__file__).resolve().parents[4]


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


select = load_script("ci_select")


def test_every_tree_walking_test_in_the_repository_is_marked() -> None:
    assert select.unmarked_tree_walkers(ROOT, select.tracked_files(ROOT)) == []


def test_an_operatorcli_only_selection_still_runs_the_safefs_writer_guard() -> None:
    """The merge-queue ejection this class fix closes: PR CI skipped the whole-tree writer guard.

    The plan is built by hand (selecting operatorcli alone) so the test stays fast; the real planner is
    exercised on a synthetic workspace in ``test_ci_selection.py``."""
    config = select.Config.load(ROOT)
    guards = select.go_whole_tree_tests(ROOT, select.tracked_files(ROOT))
    plan = select.Plan(go_packages=["sdo/operator/operatorcli"], go_guards=guards)
    regex, packages = select.shard_go_guards(plan, config, "rest")
    assert "TestCheckoutWritersGoThroughSafefs" in regex
    assert "./sdo/operator/safefs" in packages


def test_the_local_gate_runs_every_go_whole_tree_guard(tmp_path: Path) -> None:
    """Pull request CI always runs the guards, so a local gate that skips them passes code CI then rejects.

    A recording ``go`` stands in for the toolchain: what matters is which tests the gate asks it to run."""
    calls = tmp_path / "go-calls"
    (tmp_path / "go").write_text(f'#!/bin/bash\necho "$@" >> "{calls}"\n')
    (tmp_path / "go").chmod(0o755)
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"}
    env.pop("SDO_CI_PLAN", None)
    env.pop("SDO_GO_TEST_LOG", None)
    subprocess.run(["bash", str(ROOT / "scripts/test_go_ci.sh"), "guards"], env=env, check=True, capture_output=True)

    run = calls.read_text().split()
    selected = run[run.index("-run") + 1]
    for directory, names in select.go_whole_tree_tests(ROOT, select.tracked_files(ROOT)).items():
        assert f"./{directory}" in run
        assert all(name in selected for name in names)
    assert "-count=1" in run, "a guard reads sources outside its module, which the test cache does not track"
    assert 'test_go_ci.sh" guards' in (ROOT / "scripts/check_go_errors.sh").read_text()
