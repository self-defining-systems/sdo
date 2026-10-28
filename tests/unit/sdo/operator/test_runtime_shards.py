"""Fixed job counts and shared immutable inventory artifacts are PR contracts."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[4]


def test_fixed_jobs_and_single_required_result():
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    assert set(jobs) == {"prepare-go", "python", "contracts", "required-pr-ci"}
    assert jobs["contracts"]["strategy"]["matrix"] == {"bin": [0, 1]}
    assert len(jobs["python"]["strategy"]["matrix"]["include"]) == 2
    assert jobs["prepare-go"]["timeout-minutes"] == 4
    assert jobs["required-pr-ci"]["timeout-minutes"] == 1
    assert jobs["required-pr-ci"]["name"] == "Required PR CI"
    assert "contracts" in jobs["required-pr-ci"]["needs"]
    preparation = jobs["prepare-go"]["steps"]
    plan = next(step for step in preparation if step.get("id") == "cost-plan")
    assert plan["if"] == "matrix.mode == 'race'"
    assert "--include-dedicated" in plan["run"]
    assert any("scripts/check_ci_budget.py" in step.get("run", "") for step in preparation)


def test_nightly_runs_every_deferred_check_and_image_family():
    jobs = yaml.safe_load((ROOT / ".github/workflows/nightly-fuzz.yml").read_text())["jobs"]
    assert "prepare-go" in jobs["deferred"]["needs"]
    assert any(
        "run_ci_contracts.py" in step.get("run", "") and "--nightly" in step["run"]
        for step in jobs["deferred"]["steps"]
    )
    assert set(jobs["images"]["strategy"]["matrix"]["family"]) == {"controller", "validator", "responder", "benchmark"}
    assert "proto" in jobs
    assert set(jobs["foundation"]["strategy"]["matrix"]["mode"]) == {"stable", "current", "contracts"}
