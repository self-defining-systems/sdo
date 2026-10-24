from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
from benchmarks.sregym.fastloop.fault_driver import SregymFaultDriver
from benchmarks.sregym.fastloop.loop import run_incidents
from benchmarks.sregym.fastloop.records import OracleVerdict
from tests.unit.benchmarks.sregym.fastloop.test_fastloop_loop import FakeAgent, FakeClock, FakeDriver, _config


@pytest.mark.parametrize("baseline_healthy", [True, False])
def test_seed_recovery_uses_independent_verdict_and_retains_original_grade(tmp_path, baseline_healthy):
    class SeedDriver(FakeDriver):
        last_full_cleanup_verified = True

        def oracle(self):
            self.events.append("mitigation_oracle")
            return OracleVerdict(kind="sregym-mitigation-oracle", success=False, details={"worker_exists": False})

        def recovery_oracle(self):
            self.events.append("seed_oracle")
            return OracleVerdict(kind="health-check", success=baseline_healthy)

    clock = FakeClock()
    driver = SeedDriver(clock)
    records = run_incidents(FakeAgent(clock), driver, _config(tmp_path, 2), monotonic=clock.monotonic)
    assert len(records) == (2 if baseline_healthy else 1)
    assert all(r.oracle.success is False for r in records)
    assert all(r.recovery_oracle.success is baseline_healthy for r in records)
    assert all(("recovery residue" in (r.error or "")) is (not baseline_healthy) for r in records)
    assert driver.events.count("mitigation_oracle") == len(records)
    assert driver.events.count("seed_oracle") == len(records)


def test_driver_requires_completed_seed_cleanup_before_independent_oracle():
    def request(op, **args):
        if op == "baseline_oracle":
            return {"kind": "health-check", "success": True, "details": {}}
        if op == "health":
            return {"healthy": True}
        if op in {"recover", "refresh"}:
            return {"seconds": 0, "protocol_version": 2, "success": True, "recovery_contract": "seed-compatible/v1"}
        raise AssertionError(op)

    worker = SimpleNamespace(request=Mock(side_effect=request))
    driver = SregymFaultDriver(
        worker,
        namespace="sample",
        baseline_problem_id="seed-problem",
        baseline=ApplicationBaseline("sample", []),
        cluster=SimpleNamespace(list_objects=lambda _: []),
    )
    with pytest.raises(RuntimeError, match="seed recovery"):
        driver.recovery_oracle()
    worker.request.assert_not_called()
    driver.recover()
    worker.request.reset_mock()
    assert driver.recovery_oracle().success
    worker.request.assert_called_once_with(
        "baseline_oracle", problem_id="seed-problem", verify_traffic=True, timeout=900.0
    )
