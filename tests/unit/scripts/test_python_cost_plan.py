"""Integration tests share the same cost/budget owner as Go tests."""

import importlib.util
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "python_cost_plan", Path(__file__).resolve().parents[3] / "scripts/python_cost_plan.py"
)
assert SPEC is not None
assert SPEC.loader is not None
planner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(planner)


def test_new_integration_test_gets_capacity_without_a_file_placement():
    plan = planner.build_plan(["a.py::test_old", "b.py::test_new"], {"a.py::test_old": 140})
    assert len(plan["bins"]) == 2
    assert sorted(n for b in plan["bins"] for n in b) == ["a.py::test_old", "b.py::test_new"]


def test_integration_and_go_share_budget_guard():
    plan = planner.build_plan(["a.py::test_a", "a.py::test_b"], {"a.py::test_a": 150, "a.py::test_b": 150})
    assert len(plan["bins"]) == 2
    assert planner.build_plan(["a.py::test_big"], {"a.py::test_big": 211})["nightly"] == ["a.py::test_big"]


def test_real_pytest_collection_reaches_runner_and_records_every_phase(tmp_path, monkeypatch):
    import json
    import sys

    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "test_contracts.py").write_text("""import pytest
@pytest.mark.go_integration
def test_first(): pass
@pytest.mark.go_integration
def test_second(): pass
def test_excluded(): assert False
""")
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "python_integration_test_costs.json").write_text('{"costs":{}}')
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[3]))
    monkeypatch.setattr(planner, "ROOT", tmp_path)
    path = tmp_path / "plan.json"
    monkeypatch.setattr(sys, "argv", ["plan", "--output", str(path)])
    planner.main()
    data = json.loads(path.read_text())
    assert sorted(n for b in data["bins"] for n in b) == [
        "tests/test_contracts.py::test_first",
        "tests/test_contracts.py::test_second",
    ]
    for i, names in enumerate(data["bins"]):
        output = tmp_path / f"costs-{i}.json"
        monkeypatch.setenv("SDO_PYTEST_COST_OUTPUT", str(output))
        monkeypatch.setattr(sys, "argv", ["plan", "--plan", str(path), "--bin", str(i)])
        planner.main()
        timings = json.loads(output.read_text())
        assert set(timings) == set(names)
        assert all(seconds > 0 for seconds in timings.values())
