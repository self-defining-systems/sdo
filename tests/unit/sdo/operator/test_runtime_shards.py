"""CI uses compiled inventories and an automatically sized matrix."""

from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[4]


def test_cost_plan_drives_the_required_controller_matrix():
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    controller = jobs["controller"]
    assert "needs.prepare-go.outputs.test_matrix" in controller["strategy"]["matrix"]
    assert "controller" in jobs["required-pr-ci"]["needs"]
    assert jobs["required-pr-ci"]["name"] == "Required PR CI"
    assert all(job["timeout-minutes"] <= 5 for job in jobs.values())
    filters = yaml.safe_load(jobs["changes"]["steps"][1]["with"]["filters"])
    assert "scripts/go_cost_plan.py" in filters["go"]
    assert "scripts/go_test_timing.py" in filters["go"]


def test_nightly_runs_the_same_planner_overflow():
    jobs = yaml.safe_load((ROOT / ".github/workflows/nightly-fuzz.yml").read_text())["jobs"]
    assert "prepare-go" in jobs["overflow"]["needs"]
    assert any("nightly --plan" in s.get("run", "") for s in jobs["overflow"]["steps"])
