"""A baseline at its install's own health judgment (``benchmarks.fuzzops.judged``)."""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.fuzzops.hooks import Baseline, HookError, write_baseline
from benchmarks.fuzzops.judged import judged_baseline, main

if TYPE_CHECKING:
    from pathlib import Path

JUDGE = "SDO-Actor: health_judge\nSDO-Phase: initial\nSDO-Validation: passed"
UPKEEP = "SDO-Actor: upkeep\nSDO-Phase: initial\nSDO-Validation: passed"
REFLECTION = "SDO-Actor: responder\nSDO-Phase: reflection\nSDO-Validation: passed"


def _git(repo: Path, *args: str) -> str:
    base = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@invalid"]
    return subprocess.run([*base, *args], check=True, capture_output=True, text=True).stdout.strip()


def _commit(repo: Path, path: str, trailers: str = "") -> str:
    file = repo / path
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(file.read_text() + "x\n" if file.exists() else "x\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", f"synthetic {path}\n\n{trailers}".strip())
    return _git(repo, "rev-parse", "HEAD")


def _directory(root: Path, repo: Path, branch: str, render: str = "kind: Service\n") -> Path:
    root.mkdir(parents=True)
    _git(repo, "branch", "-f", branch)
    _git(repo, "bundle", "create", str(root / "memory.bundle"), branch)
    (root / "render.yaml").write_text(render)
    write_baseline(
        root,
        Baseline(
            namespace="ns1",
            bundle="memory.bundle",
            branch=branch,
            commit=_git(repo, "rev-parse", branch),
            render="render.yaml",
            objective="users are served",
            config={"response": {"timeout": "1800s"}},
        ),
    )
    return root


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _commit(repo, "app/main.go")
    _commit(repo, ".sdo/goal.md")
    return repo


def test_the_judged_baseline_is_the_installs_judge_commit(tmp_path: Path, repo: Path) -> None:
    baseline = _directory(tmp_path / "baseline", repo, "baseline")
    judge = _commit(repo, ".sdo/detectors/health/links.go", JUDGE)
    _commit(repo, ".sdo/outcomes/o.json", REFLECTION)
    collected = _directory(tmp_path / "learned", repo, "learned")

    written = judged_baseline(baseline, collected, tmp_path / "judged")

    assert written.commit == judge
    loaded = Baseline.load(tmp_path / "judged")
    assert loaded.commit == judge
    assert loaded.branch == "judged"
    base = Baseline.load(baseline)
    assert (loaded.namespace, loaded.objective, loaded.config) == (base.namespace, base.objective, base.config)
    assert loaded.render_sha256 == base.render_sha256
    heads = _git(repo, "ls-remote", str(tmp_path / "judged" / "memory.bundle"))
    assert heads == f"{judge}\trefs/heads/judged"


def test_an_upkeep_migration_before_the_judge_is_part_of_the_install(tmp_path: Path, repo: Path) -> None:
    baseline = _directory(tmp_path / "baseline", repo, "baseline")
    _commit(repo, ".sdo/schema.json", UPKEEP)
    judge = _commit(repo, ".sdo/detectors/health/links.go", JUDGE)
    collected = _directory(tmp_path / "learned", repo, "learned")
    assert judged_baseline(baseline, collected, tmp_path / "judged").commit == judge


def test_memory_with_no_judge_right_after_the_baseline_is_refused(tmp_path: Path, repo: Path) -> None:
    baseline = _directory(tmp_path / "baseline", repo, "baseline")
    _commit(repo, ".sdo/outcomes/o.json", REFLECTION)
    _commit(repo, ".sdo/detectors/health/links.go", JUDGE)  # a later judge is another run's, not the install's
    collected = _directory(tmp_path / "learned", repo, "learned")
    with pytest.raises(HookError, match="no validated initial health_judge commit"):
        judged_baseline(baseline, collected, tmp_path / "judged")


def test_a_judge_commit_that_changes_application_inputs_is_refused(tmp_path: Path, repo: Path) -> None:
    baseline = _directory(tmp_path / "baseline", repo, "baseline")
    _commit(repo, "app/main.go", JUDGE)
    collected = _directory(tmp_path / "learned", repo, "learned")
    with pytest.raises(HookError, match="changes application inputs"):
        judged_baseline(baseline, collected, tmp_path / "judged")


def test_memory_made_on_another_render_is_refused(tmp_path: Path, repo: Path) -> None:
    baseline = _directory(tmp_path / "baseline", repo, "baseline")
    _commit(repo, ".sdo/detectors/health/links.go", JUDGE)
    collected = _directory(tmp_path / "learned", repo, "learned", render="kind: Deployment\n")
    with pytest.raises(HookError, match="another render"):
        judged_baseline(baseline, collected, tmp_path / "judged")


def test_memory_not_made_from_the_baseline_is_refused(tmp_path: Path, repo: Path) -> None:
    _commit(repo, ".sdo/detectors/health/links.go", JUDGE)
    collected = _directory(tmp_path / "learned", repo, "learned")
    _git(repo, "checkout", "-q", "--orphan", "other")
    _commit(repo, ".sdo/goal.md")
    baseline = _directory(tmp_path / "baseline", repo, "baseline")
    with pytest.raises(HookError, match="not an ancestor"):
        judged_baseline(baseline, collected, tmp_path / "judged")


def test_main_prints_the_judged_commit(tmp_path: Path, repo: Path, capsys: pytest.CaptureFixture[str]) -> None:
    baseline = _directory(tmp_path / "baseline", repo, "baseline")
    judge = _commit(repo, ".sdo/detectors/health/links.go", JUDGE)
    collected = _directory(tmp_path / "learned", repo, "learned")
    out = tmp_path / "judged"
    assert main(["--baseline", str(baseline), "--from", str(collected), "--out", str(out)]) == 0
    receipt = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert receipt == {"baseline": str(out.resolve()), "commit": judge, "judged_on": Baseline.load(baseline).commit}
    assert main(["--baseline", str(baseline), "--from", str(collected), "--out", str(out)]) == 1
