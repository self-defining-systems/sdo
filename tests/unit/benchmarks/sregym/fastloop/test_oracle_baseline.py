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
