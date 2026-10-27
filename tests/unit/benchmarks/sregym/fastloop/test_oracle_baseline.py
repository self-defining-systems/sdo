"""Healthy rehearsal separates invalid fault oracles from repair failures."""

from copy import deepcopy

import pytest

from benchmarks.sregym.fastloop.oracle_baseline import OracleBaselines


def grade(verdicts):
    return {
        "kind": "sregym-mitigation-oracle",
        "success": all(verdicts),
        "details": {
            "success": all(verdicts),
            "accuracy": 100.0 if all(verdicts) else 0.0,
            "collateral_ok": True,
            "traffic_ok": True,
            "oracles": [{"name": f"fault_{i}:Synthetic", "success": ok} for i, ok in enumerate(verdicts)],
            "faults_total": len(verdicts),
            "faults_fixed": sum(verdicts),
            **{f"fault_{i}_verdict": ok for i, ok in enumerate(verdicts)},
        },
    }


@pytest.mark.parametrize("verdicts", [[False, True], [True, False], [False, False], [True, True]])
def test_rehearsal_and_grading_share_exclusions(tmp_path, verdicts):
    path = tmp_path / "oracles.json"
    baseline = OracleBaselines(path, "app-seed-harness")
    raw = grade(verdicts)
    before = deepcopy(raw)
    prepared = baseline.rehearse("problem", lambda: raw)
    assert prepared["success"] is True  # preparation proceeds
    assert raw == before
    reloaded = OracleBaselines(path, "app-seed-harness")
    # Cached once, including green oracles; no live re-evaluation after injection.
    reloaded.rehearse("problem", lambda: pytest.fail("rehearsed twice"))
    final = reloaded.apply("problem", grade([False, True]))
    invalid = sum(not ok for ok in verdicts)
    assert final["details"].get("oracle_invalid", 0) == invalid
    assert final["details"]["faults_total"] == 2 - invalid
    assert final["details"]["faults_fixed"] == int(verdicts[1])
    assert final["success"] is (not verdicts[0])
    if all(verdicts):
        assert prepared == raw


def test_exclusion_never_hides_collateral_or_traffic_failure(tmp_path):
    baseline = OracleBaselines(tmp_path / "oracles.json", "seed")
    baseline.rehearse("p", lambda: grade([False]))
    for field in ("collateral_ok", "traffic_ok"):
        failed = grade([False])
        failed["details"][field] = False
        assert baseline.apply("p", failed)["success"] is False


def test_seed_change_requires_new_rehearsal(tmp_path):
    path = tmp_path / "oracles.json"
    OracleBaselines(path, "old").rehearse("p", lambda: grade([False]))
    new = OracleBaselines(path, "new")
    assert new.rehearse("p", lambda: grade([True]))["details"].get("oracle_invalid", 0) == 0


def test_single_fault_invalid_is_excluded_not_fixed(tmp_path):
    baseline = OracleBaselines(tmp_path / "oracles.json", "seed")
    raw = {"kind": "sregym-mitigation-oracle", "success": False, "details": {"success": False}}
    prepared = baseline.rehearse("single", lambda: raw)
    assert prepared["success"] is True
    assert prepared["details"]["faults_fixed"] == 0
    assert prepared["details"]["faults_total"] == 0
    assert prepared["details"].get("oracle_invalid", 0) == 1


def test_real_worker_grading_consumes_rehearsal(tmp_path):
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop.oracle_baseline import install_oracle_baselines

    class Worker:
        def __init__(self):
            self._problem_factory = lambda problem_id: SimpleNamespace(problem_id=problem_id)
            self.calls = 0

        def _grade(self, problem, oracle=None):
            self.calls += 1
            return grade([False, True])

    worker = Worker()
    rehearse = install_oracle_baselines(worker, OracleBaselines(tmp_path / "baseline.json", "seed"))
    assert rehearse("p")["success"] is True
    assert rehearse("p")["success"] is True
    assert worker.calls == 1
    final = worker._grade(worker._problem_factory("p"))
    assert final["success"] is True
    assert final["details"]["faults_fixed"] == 1
    assert final["details"]["oracle_invalid"] == 1


@pytest.mark.requires_sregym
def test_real_harness_worker_produces_and_consumes_baseline(tmp_path, monkeypatch, request):
    """Exercise the real harness JSON grading path without its model/cluster imports."""
    import importlib
    import sys
    from pathlib import Path
    from types import ModuleType, SimpleNamespace

    from benchmarks.sregym.fastloop.oracle_baseline import install_oracle_baselines

    root = Path(__file__).resolve().parents[5] / "third_party/sregym"
    original_modules = set(sys.modules)

    def remove_imported_harness_modules():
        for name in set(sys.modules) - original_modules:
            if name == "sregym" or name.startswith("sregym."):
                sys.modules.pop(name, None)

    request.addfinalizer(remove_imported_harness_modules)
    # These package initializers import the entire conductor/model stack. The
    # worker itself depends only on stdlib modules; load its real package files.
    for name in ("sregym", "sregym.conductor", "sregym.conductor.problems"):
        package = ModuleType(name)
        package.__path__ = [str(root / name.replace(".", "/"))]
        monkeypatch.setitem(sys.modules, name, package)
    module = importlib.import_module("sregym.fastloop.worker")
    raw = grade([False, True])["details"]

    def factory(problem_id):
        return SimpleNamespace(
            app=SimpleNamespace(namespace="synthetic"),
            mitigation_oracle=SimpleNamespace(evaluate=lambda: deepcopy(raw)),
            inject_fault=lambda: None,
        )

    worker = module.FastloopWorker(
        problem_factory=factory,
        deployer=lambda *_: None,
        health_probe=lambda _: {"healthy": True},
        cluster_factory=lambda *_: ("unused", "unused"),
        cluster_inventory=SimpleNamespace(list_objects=lambda _: frozenset()),
    )
    rehearse = install_oracle_baselines(worker, OracleBaselines(tmp_path / "baselines.json", "seed"))
    assert rehearse("p")["success"] is True
    worker.handle({"op": "inject", "problem_id": "p"})
    result = worker.handle({"op": "oracle"})
    assert result["success"] is True
    assert result["details"]["faults_fixed"] == 1
    assert result["details"]["faults_total"] == 1
    assert result["details"]["oracle_invalid"] == 1


def test_changed_component_identity_cannot_inherit_exclusion(tmp_path):
    baseline = OracleBaselines(tmp_path / "oracles.json", "seed")
    baseline.rehearse("p", lambda: grade([False, True]))
    changed = grade([False, True])
    changed["details"]["oracles"].reverse()
    with pytest.raises(ValueError, match="identities changed"):
        baseline.apply("p", changed)


def test_unhealthy_app_cannot_authorize_oracle_exclusions(tmp_path):
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop.oracle_baseline import install_oracle_baselines

    worker = SimpleNamespace(
        _problem_factory=lambda _: SimpleNamespace(),
        _grade=lambda *_: pytest.fail("must confirm app health before judging oracle validity"),
    )
    baseline = OracleBaselines(tmp_path / "oracles.json", "seed")
    rehearse = install_oracle_baselines(worker, baseline, confirm_healthy=lambda _: False)
    assert rehearse("p")["success"] is False
    assert baseline.grades == {}
    assert not baseline.path.exists()


def test_recovery_uses_fresh_grade_without_reclassifying_invalid_oracles(tmp_path):
    baseline = OracleBaselines(tmp_path / "oracles.json", "seed")
    baseline.rehearse("p", lambda: grade([False, True]))
    recovered = baseline.rehearse("p", lambda: grade([True, False]), fresh=True)
    assert recovered["success"] is False  # current valid fault is now red
    assert recovered["details"]["faults_fixed"] == 0
    assert recovered["details"]["oracle_invalid"] == 1
    assert baseline.grades["p"] == grade([False, True])


def test_current_unhealthy_confirmation_is_returned_without_cached_success(tmp_path):
    baseline = OracleBaselines(tmp_path / "oracles.json", "seed")
    baseline.rehearse("p", lambda: grade([False, True]))
    unhealthy = {"kind": "health-check", "success": False, "details": {"collateral_ok": False}}
    assert baseline.rehearse("p", lambda: unhealthy, fresh=True) == unhealthy
    assert baseline.grades["p"] == grade([False, True])


def test_excluded_single_fault_still_requires_current_application_health(tmp_path):
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop.oracle_baseline import install_oracle_baselines

    worker = SimpleNamespace(
        _problem_factory=lambda _: SimpleNamespace(),
        _grade=lambda *_: {"kind": "sregym-mitigation-oracle", "success": False, "details": {"success": False}},
    )
    health = iter([True, False])
    rehearse = install_oracle_baselines(
        worker, OracleBaselines(tmp_path / "oracles.json", "seed"), confirm_healthy=lambda _: next(health)
    )
    assert rehearse("p")["success"] is True
    assert worker._grade(worker._problem_factory("p"))["success"] is False
