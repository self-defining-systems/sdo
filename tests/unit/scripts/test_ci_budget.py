"""Standing PR limits are enforced rather than remembered."""

import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "ci_budget", Path(__file__).resolve().parents[3] / "scripts/check_ci_budget.py"
)
assert SPEC is not None
assert SPEC.loader is not None
budget = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(budget)


def jobs():
    return {
        "prepare-go": {"timeout-minutes": 4, "strategy": {"matrix": {"mode": ["normal", "race"]}}},
        "python": {"needs": "prepare-go", "timeout-minutes": 5, "strategy": {"matrix": {"part": ["core", "fastloop"]}}},
        "contracts": {"needs": "prepare-go", "timeout-minutes": 5, "strategy": {"matrix": {"bin": [0, 1]}}},
        "required-pr-ci": {"needs": ["python", "contracts"], "timeout-minutes": 1},
    }


def test_four_jobs_and_ten_minute_critical_path():
    assert budget.check(jobs()) == (4, 600)


def test_fifth_job_fails():
    source = jobs()
    source["extra"] = {"needs": "prepare-go", "timeout-minutes": 1}
    with pytest.raises(ValueError, match="4"):
        budget.check(source)


def test_critical_path_over_ten_minutes_fails():
    source = jobs()
    source["prepare-go"]["timeout-minutes"] = 5
    with pytest.raises(ValueError, match="600|10"):
        budget.check(source)


def test_growing_matrix_fails_closed():
    source = jobs()
    source["contracts"]["strategy"]["matrix"] = "${{ fromJSON(needs.prepare-go.outputs.matrix) }}"
    with pytest.raises(ValueError, match="fixed"):
        budget.check(source)
