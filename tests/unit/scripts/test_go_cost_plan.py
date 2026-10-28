"""Cost planning guards the next expensive test, without placement edits."""

import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "go_cost_plan", Path(__file__).resolve().parents[3] / "scripts/go_cost_plan.py"
)
assert SPEC is not None
assert SPEC.loader is not None
planner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(planner)


def test_new_expensive_test_creates_capacity_without_placement_edits():
    costs = {"TestOld": 140, "TestNewExpensive": 140}
    bins, nightly = planner.pack(list(costs), costs, budget=210)
    assert len(bins) == 2
    assert sorted(n for b in bins for n in b) == sorted(costs)
    assert nightly == []


def test_budget_guard_rejects_overfull_shard():
    with pytest.raises(ValueError, match="budget"):
        planner.check_budget(["TestOld", "TestNewExpensive"], {"TestOld": 140, "TestNewExpensive": 140}, budget=210)


def test_unknown_names_have_conservative_cost_and_deterministic_ownership():
    names = [f"TestNew{i}" for i in range(30)]
    bins, nightly = planner.pack(names, {}, budget=210)
    assert bins == planner.pack(list(reversed(names)), {}, budget=210)[0]
    assert len(bins) == len(names)
    assert sorted(n for b in bins for n in b) == sorted(names)
    assert not nightly


def test_single_test_over_budget_runs_nightly():
    assert planner.pack(["TestHuge"], {"TestHuge": 211}, budget=210) == ([], ["TestHuge"])


@pytest.mark.parametrize("cost", [0, -1, float("nan"), float("inf"), True])
def test_invalid_measurements_fail_closed(cost):
    with pytest.raises(ValueError, match="cost"):
        planner.pack(["TestOne"], {"TestOne": cost}, budget=210)
    with pytest.raises(ValueError, match="cost"):
        planner.check_budget(["TestOne"], {"TestOne": cost}, budget=210)


def test_duplicate_test_names_cannot_receive_two_owners(tmp_path, monkeypatch):
    profiles = tmp_path / "scripts"
    profiles.mkdir()
    (profiles / "operator_test_costs.json").write_text('{"costs":{"pkg/a/TestSame":120,"pkg/b/TestSame":120}}')
    monkeypatch.setattr(planner, "ROOT", tmp_path)
    data = planner.plan("operator", {"pkg/a": ["TestSame"], "pkg/b": ["TestSame"]})
    assert data["nightly"] == ["TestSame"]
    assert data["packages"]["TestSame"] == ["pkg/a", "pkg/b"]


def test_removed_cost_entries_do_not_create_tests(tmp_path, monkeypatch):
    profiles = tmp_path / "scripts"
    profiles.mkdir()
    (profiles / "operator_test_costs.json").write_text('{"costs":{"pkg/a/TestRemoved":200}}')
    monkeypatch.setattr(planner, "ROOT", tmp_path)
    data = planner.plan("operator", {"pkg/a": ["TestNew"]})
    assert data["bins"] == [["TestNew"]]
    assert data["costs"]["TestNew"] == planner.DEFAULT_COST


@pytest.mark.go_integration
def test_real_compiled_inventory_reaches_real_runner(tmp_path, monkeypatch, capsys):
    """The Go producer and selection consumer run together; no fake inventory."""
    import json
    import sys

    module = tmp_path / "sdo/operator"
    module.mkdir(parents=True)
    (module / "go.mod").write_text("module synthetic.example/ci\n\ngo 1.24\n")
    (module / "plan_test.go").write_text("""package ci
import "testing"
func TestFirst(t *testing.T) {}
func FuzzSeed(f *testing.F) { f.Add("seed"); f.Fuzz(func(t *testing.T, s string) {}) }
""")
    (module / "race_only_test.go").write_text(
        '//go:build race\n\npackage ci\nimport "testing"\nfunc TestRaceOnly(t *testing.T) {}\n'
    )
    (module / "normal_only_test.go").write_text(
        """//go:build !race

package ci
import "testing"
func TestNormalOnly(t *testing.T) { t.Fatal("normal-only test cannot enter a race plan") }
"""
    )
    profiles = tmp_path / "scripts"
    profiles.mkdir()
    (profiles / "operator_test_costs.json").write_text('{"costs":{}}')
    monkeypatch.setattr(planner, "ROOT", tmp_path)
    source = planner.inventory("")
    assert source == {"synthetic.example/ci": ["TestFirst", "TestRaceOnly", "FuzzSeed"]}
    data = planner.plan("operator", source)
    assert len(data["bins"]) == 3
    path = tmp_path / "plan.json"
    path.write_text(json.dumps({"operator": data}))
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    import time

    from go_test_timing import run_tests

    restored = json.loads(path.read_text())["operator"]
    members = [name for bucket in restored["bins"] for name in bucket]
    run_tests(planner.go_command(restored, members), started=time.monotonic(), cwd=module)
    events = []
    for line in capsys.readouterr().out.splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    passed = {event["Test"] for event in events if event.get("Action") == "pass" and event.get("Test")}
    assert {name for name in passed if "/" not in name} == set(members)
    assert any(name.startswith("FuzzSeed/") for name in passed)


def test_profiles_fit_work_budget_with_setup_margin():
    import json

    for scope in ("operator", "runtime"):
        profile = json.loads((planner.ROOT / f"scripts/{scope}_test_costs.json").read_text())["costs"]
        bins, nightly = planner.pack(list(profile), profile)
        assert sorted(n for b in bins for n in b) + sorted(nightly) != []
        for members in bins:
            assert sum(profile[n] for n in members) <= planner.JOB_SECONDS - planner.SETUP_MARGIN
            planner.check_budget(members, profile)
        assert all(profile[n] > planner.BUDGET for n in nightly)


def test_go_replay_is_budgeted_without_sharing_a_python_job():
    data = planner.plan("runtime", {"sdo.dev/operator/runtime": ["TestReplayNewBehavior", "TestChaosBounded"]})
    assert data["bins"] == [["TestReplayNewBehavior"]]
    assert "TestChaosBounded" not in data["packages"]


def test_python_only_changes_keep_go_replay_coverage_without_other_go_suites():
    source = {"sdo.dev/operator/runtime": ["TestReplayNewBehavior", "TestReflectionUnchanged", "TestChaosBounded"]}
    data = planner.plan("runtime", source, replay_only=True)
    assert data["bins"] == [["TestReplayNewBehavior"]]
    assert planner.plan("operator", {"sdo.dev/operator/broker": ["TestOther"]}, replay_only=True)["bins"] == []


def test_local_measurements_have_conservative_bounds_until_hosted_costs_exist(tmp_path, monkeypatch):
    import json

    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "runtime_test_costs.json").write_text(
        json.dumps(
            {
                "costs": {"TestHosted": 2},
                "local_costs": {"TestLocal": 0.5, "TestHosted": 10},
            }
        )
    )
    monkeypatch.setattr(planner, "ROOT", tmp_path)
    data = planner.plan("runtime", {"runtime": ["TestLocal", "TestHosted", "TestUnmeasured"]})
    assert data["costs"]["TestLocal"] == 10
    assert data["costs"]["TestHosted"] == 2
    assert data["costs"]["TestUnmeasured"] == planner.BUDGET


@pytest.mark.go_integration
def test_test_compilation_checks_packages_without_test_files(tmp_path):
    import subprocess

    (tmp_path / "go.mod").write_text("module synthetic.example/nontest\n\ngo 1.24\n")
    (tmp_path / "main.go").write_text("package main\nfunc main() { undefinedCompilerControl() }\n")
    result = subprocess.run(
        ["go", "test", "-c", "-o", str(tmp_path / "out"), "./..."],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "undefinedCompilerControl" in result.stderr
