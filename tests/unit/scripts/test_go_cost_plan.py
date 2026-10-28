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
def test_real_compiled_inventory_reaches_real_runner(tmp_path, monkeypatch):
    """The Go producer and selection consumer run together; no fake inventory."""
    import json
    import sys

    module = tmp_path / "sdo/operator"
    module.mkdir(parents=True)
    (module / "go.mod").write_text("module synthetic.example/ci\n\ngo 1.24\n")
    (module / "plan_test.go").write_text("""package ci
import "testing"
func TestFirst(t *testing.T) {}
func TestSecond(t *testing.T) {}
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
    assert source == {"synthetic.example/ci": ["TestFirst", "TestSecond", "TestRaceOnly", "FuzzSeed"]}
    data = planner.plan("operator", source)
    assert len(data["bins"]) == 4
    path = tmp_path / "plan.json"
    path.write_text(json.dumps({"operator": data}))
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    import time

    from go_test_timing import run_tests

    restored = json.loads(path.read_text())["operator"]
    for members in restored["bins"]:
        run_tests(planner.go_command(restored, members), started=time.monotonic(), cwd=module)


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
