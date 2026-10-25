"""Commit policy tests use real temporary Git history, including hidden raises."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.check_prompt_budgets import BUDGET_PATH, Budget, check_raise, check_range, parse_budgets


def document(limit: int, reason: str = "baseline") -> str:
    return f'[responder.instructions]\nmax_chars = {limit}\nreason = "{reason}"\n'


def git(root: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(root), *args], check=True, text=True, capture_output=True).stdout.strip()


def commit(root: Path, *, limit: int, reason: str, message: str, extra: bool = False) -> str:
    path = root / BUDGET_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(document(limit, reason), encoding="utf-8")
    if extra:
        (root / "other.md").write_text(reason, encoding="utf-8")
    git(root, "add", ".")
    git(root, "commit", "-m", message)
    return git(root, "rev-parse", "HEAD")


@pytest.fixture
def history(tmp_path: Path) -> tuple[Path, str]:
    git(tmp_path, "init", "--initial-branch=main")
    git(tmp_path, "config", "user.name", "Synthetic Test")
    git(tmp_path, "config", "user.email", "test@example.invalid")
    (tmp_path / "README.md").write_text("synthetic", encoding="utf-8")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-m", "initial")
    base = git(tmp_path, "rev-parse", "HEAD")
    commit(tmp_path, limit=100, reason="baseline", message="initial budgets\n\nBudget-Reason: current measured maximum")
    return tmp_path, base


def test_valid_new_and_raised_limits(history: tuple[Path, str]) -> None:
    root, base = history
    commit(root, limit=200, reason="larger synthetic input", message="raise\n\nBudget-Reason: larger input")
    assert check_range(root, base, "HEAD") == []


@pytest.mark.parametrize(
    ("reason", "message", "extra", "expected"),
    [
        ("changed", "raise", False, "Budget-Reason"),
        ("baseline", "raise\n\nBudget-Reason: larger input", False, "updated reason"),
        ("changed", "raise\n\nBudget-Reason: larger input", True, "change only"),
        ("changed", "raise\n\nBudget-Reason: just body text\nThis paragraph is not a trailer.", False, "Budget-Reason"),
    ],
)
def test_invalid_raise(history: tuple[Path, str], reason: str, message: str, extra: bool, expected: str) -> None:
    root, base = history
    commit(root, limit=200, reason=reason, message=message, extra=extra)
    errors = check_range(root, base, "HEAD")
    assert any(expected in error for error in errors)


def test_each_commit_checked_even_when_final_limit_decreases(history: tuple[Path, str]) -> None:
    root, base = history
    raised = commit(root, limit=200, reason="bad raise", message="unexplained")
    commit(root, limit=50, reason="lowered", message="lower", extra=True)
    errors = check_range(root, base, "HEAD")
    assert len(errors) == 1
    assert errors[0].startswith(raised[:12])


def test_lowering_and_reason_updates_need_no_special_commit() -> None:
    before = {"responder.instructions": Budget(200, "before")}
    after = {"responder.instructions": Budget(100, "after")}
    assert check_raise(before, after, files=[BUDGET_PATH, "source.py"], trailers="") == []


def test_new_positive_entry_is_a_raise() -> None:
    errors = check_raise({}, {"new.instructions": Budget(100, "new")}, files=[BUDGET_PATH, "source.py"], trailers="")
    assert len(errors) == 2


@pytest.mark.parametrize(
    "payload",
    [
        '[role.part]\nmax_chars = true\nreason = "why"',
        '[role.part]\nmax_chars = -1\nreason = "why"',
        '[role.part]\nmax_chars = 100\nreason = " "',
        "[role.part]\nmax_chars = 100",
        "role = 3",
    ],
)
def test_manifest_validation(payload: str) -> None:
    with pytest.raises(ValueError, match="requires|must|expected"):
        parse_budgets(payload)


def test_policy_runs_inside_required_pr_ci() -> None:
    import yaml

    root = Path(__file__).resolve().parents[3]
    workflow = yaml.safe_load((root / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    job = workflow["jobs"]["required-pr-ci"]
    assert job["name"] == "Required PR CI"
    steps = job["steps"]
    checkout = next(step for step in steps if step.get("uses", "").startswith("actions/checkout@"))
    assert checkout["with"]["fetch-depth"] == 0
    policy = next(step for step in steps if "scripts/check_prompt_budgets.py" in step.get("run", ""))
    assert "if" not in policy
    assert "continue-on-error" not in policy
    assert "pull_request.base.sha" in policy["env"]["BUDGET_BASE"]
    assert "pull_request.head.sha" in policy["env"]["BUDGET_HEAD"]


def test_merge_inherits_a_raise_already_checked_in_its_own_commit(history: tuple[Path, str]) -> None:
    root, base = history
    git(root, "switch", "-c", "budget-raise")
    commit(root, limit=200, reason="larger", message="raise\n\nBudget-Reason: larger fixture")
    git(root, "switch", "main")
    (root / "README.md").write_text("independent source change", encoding="utf-8")
    git(root, "add", ".")
    git(root, "commit", "-m", "independent source change")
    git(root, "merge", "--no-ff", "--no-edit", "budget-raise")
    assert check_range(root, base, "HEAD") == []


def test_merge_that_introduces_a_new_raise_still_needs_justification(history: tuple[Path, str]) -> None:
    root, base = history
    git(root, "switch", "-c", "budget-raise")
    commit(root, limit=200, reason="larger", message="raise\n\nBudget-Reason: larger fixture")
    git(root, "switch", "main")
    (root / "README.md").write_text("independent source change", encoding="utf-8")
    git(root, "add", ".")
    git(root, "commit", "-m", "independent source change")
    git(root, "merge", "--no-ff", "--no-commit", "budget-raise")
    (root / BUDGET_PATH).write_text(document(300, "new merge raise"), encoding="utf-8")
    git(root, "add", ".")
    git(root, "commit", "-m", "merge with unexplained new raise")
    assert any("Budget-Reason" in error for error in check_range(root, base, "HEAD"))


def test_merge_cannot_hide_an_unjustified_parent_raise(history: tuple[Path, str]) -> None:
    root, base = history
    git(root, "switch", "-c", "bad-budget-raise")
    bad = commit(root, limit=200, reason="larger", message="unexplained raise")
    git(root, "switch", "main")
    (root / "README.md").write_text("independent source change", encoding="utf-8")
    git(root, "add", ".")
    git(root, "commit", "-m", "independent source change")
    git(root, "merge", "--no-ff", "--no-edit", "bad-budget-raise")
    errors = check_range(root, base, "HEAD")
    assert len(errors) == 1
    assert errors[0].startswith(bad[:12])
