"""The local gate (scripts/check_errors.sh) runs every static check that pull-request CI runs.

An agent that passes `check_errors.sh` must not then fail the static job, so each
`scripts/check_*` entry point is either reachable from `check_errors.sh` or listed
here with the reason it cannot run there.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "scripts"
ENTRY = "check_errors.sh"

# Checks that need inputs only a CI job or a live provider has.
NOT_LOCAL = {
    "check_slow_tests.py": "reads the test job's timing log",
    "check_claude_worker.py": "spends provider quota on a real model turn",
    "check_session_context.py": "reads a recorded provider session file",
    "check_responder_image.py": "reads an image file inventory piped by the image job",
    "check_model_cli_versions.py": "inspects model CLIs installed in an image",
    "check_arch.sh": "tach alone; check_errors.sh runs tach and the Go core check itself",
}
# CI static-job steps that are cache plumbing, not checks.
STATIC_JOB_PLUMBING = ("ci_go_cache.sh", "ci_cache_guard.sh", "ci_cache_budget.py", "install_", "init_test_prereqs.py")


def _reachable(script: str, seen: set[str]) -> set[str]:
    """Scripts named, directly or through other scripts, by `script`."""
    if script in seen or not (SCRIPTS / script).is_file():
        return seen
    seen.add(script)
    text = (SCRIPTS / script).read_text()
    for name in re.findall(r"[A-Za-z0-9_]+\.(?:sh|py)", text):
        _reachable(name, seen)
    return seen


def _static_job_commands() -> list[str]:
    steps = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]["checks"]["steps"]
    return [line.strip() for step in steps for line in step.get("run", "").splitlines() if line.strip()]


def test_every_check_script_runs_locally_or_says_why_not() -> None:
    local = _reachable(ENTRY, set())
    unwired = sorted(
        p.name for p in SCRIPTS.glob("check_*") if p.suffix in (".sh", ".py") and p.name not in local | set(NOT_LOCAL)
    )
    assert not unwired, f"wire into scripts/{ENTRY} (or list in NOT_LOCAL with a reason): {unwired}"
    assert set(NOT_LOCAL) <= {p.name for p in SCRIPTS.iterdir()}, "stale NOT_LOCAL entry"
    assert not set(NOT_LOCAL) & local, "NOT_LOCAL names a script the local gate already runs"


def test_the_static_job_runs_only_the_local_gate_beyond_plumbing() -> None:
    commands = _static_job_commands()
    gate = [c for c in commands if ENTRY in c]
    assert len(gate) == 1, "the static job runs check_errors.sh exactly once"
    other = [
        c
        for c in commands
        if c not in gate and "scripts/" in c and not any(p in c for p in (*STATIC_JOB_PLUMBING, "format_code.sh"))
    ]
    assert not other, f"the static job runs checks the local gate does not: {other}"
    assert "format_code.sh --check" in " ".join(commands), "format check is the same ruff check the gate runs"
    assert "ruff format --check" in (SCRIPTS / ENTRY).read_text()


def test_pr_ci_scripts_outside_the_static_job_are_local_or_listed() -> None:
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    local = _reachable(ENTRY, set())
    for name, job in jobs.items():
        for step in job["steps"]:
            for script in re.findall(r"scripts/(check_[A-Za-z0-9_]+\.(?:sh|py))", step.get("run", "")):
                assert script in local or script in NOT_LOCAL, f"{name}: {script} is neither local nor listed"
