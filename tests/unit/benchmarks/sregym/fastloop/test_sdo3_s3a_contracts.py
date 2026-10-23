"""Hand-written S3A protocol v2 fixtures; no archived run outputs or models."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from benchmarks.sregym.adapter.fault_gate import FaultGateError, request_fault_injection
from benchmarks.sregym.fastloop.fault_driver import FastloopFaultError, SregymFaultDriver


class Worker:
    def __init__(self, reply):
        self.reply = reply
        self.calls = []

    def request(self, op, **kwargs):
        self.calls.append((op, kwargs))
        return self.reply


def test_invalid_worker_injection_has_typed_infrastructure_outcome():
    from benchmarks.sregym.fastloop.fault_driver import InvalidInjectionError

    worker = Worker(
        {
            "protocol_version": 2,
            "started_at": 1,
            "finished_at": 2,
            "fault": 0,
            "faults": [0, 1],
            "injection": {"valid": False, "infra_error": "injection_invalid", "details": {"composite_valid": False}},
        }
    )
    with pytest.raises(InvalidInjectionError) as error:
        SregymFaultDriver(worker, namespace="hotel-reservation").inject("mf__synthetic")
    assert error.value.infra_error == "injection_invalid"
    assert error.value.details == {"composite_valid": False}


def test_deferred_invalid_injection_never_opens_terminal_done_stage():
    with patch(
        "benchmarks.sregym.adapter.fault_gate.request_with_retry",
        return_value=SimpleNamespace(
            json=lambda: {
                "protocol_version": 2,
                "status": "ok",
                "stage": "done",
                "injection": {
                    "valid": False,
                    "infra_error": "injection_invalid",
                    "details": {"composite_valid": False},
                },
            }
        ),
    ):
        with pytest.raises(FaultGateError, match="injection_invalid"):
            request_fault_injection("http://synthetic.invalid")


def test_driver_flattens_each_composites_returned_indices():
    class CompositionWorker(Worker):
        def request(self, op, **kwargs):
            self.calls.append((op, kwargs))
            ids = [0, 1] if not kwargs.get("compose") else [2]
            return {
                "protocol_version": 2,
                "started_at": 1,
                "finished_at": 2,
                "fault": ids[0],
                "faults": ids,
                "injection": {"valid": True, "infra_error": None, "details": {}},
                "components": [
                    {
                        "fault": i,
                        "problem_id": kwargs["problem_id"],
                        "component_index": i,
                        "incident": 0,
                        "injection_order": i,
                        "identity": {"class": "Synthetic"},
                    }
                    for i in ids
                ],
            }

    worker = CompositionWorker({})
    injection = SregymFaultDriver(worker, namespace="hotel-reservation").inject_composite(["mf__nested", "single"])
    assert [fault.index for fault in injection.faults] == [0, 1, 2]
    assert [fault.problem_id for fault in injection.faults] == ["mf__nested", "mf__nested", "single"]
    assert worker.calls[1][1]["compose"] is True


@pytest.mark.parametrize("status", ["partial", "error", "timeout"])
def test_failed_full_cleanup_blocks_healthy_baseline_and_further_incidents(status):
    worker = Worker(
        {
            "protocol_version": 2,
            "success": False,
            "status": status,
            "mode": "full",
            "seconds": 0.1,
            "recovered": [1],
            "pending": [0],
            "errors": [{"error": "synthetic cleanup failure"}],
        }
    )
    with pytest.raises(FastloopFaultError, match="recovery"):
        SregymFaultDriver(
            worker, namespace="hotel-reservation", health_timeout_seconds=0.01, health_poll_seconds=0.001
        ).recover()
    assert worker.calls == [("recover", {"mode": "full", "timeout": 900.0})]


def test_addressed_recovery_explicitly_selects_full_mode():
    worker = Worker(
        {
            "protocol_version": 2,
            "success": True,
            "status": "recovered",
            "mode": "full",
            "seconds": 0.1,
            "recovered": [1],
            "pending": [0],
            "errors": [],
        }
    )
    assert SregymFaultDriver(worker, namespace="hotel-reservation").recover_fault(1) == 0.1
    assert worker.calls[0][1]["mode"] == "full"


def test_component_oracle_metadata_stays_at_benchmark_edge():
    worker = Worker(
        {
            "protocol_version": 2,
            "kind": "sregym-mitigation-oracle",
            "success": False,
            "details": {},
            "components": [{"fault": 1, "component_index": 1, "identity": {"class": "Synthetic"}}],
        }
    )
    verdict = SregymFaultDriver(worker, namespace="hotel-reservation").oracle(fault=1)
    assert verdict.success is False
    assert worker.calls[0][1]["fault"] == 1


def test_invalid_injection_is_terminal_infrastructure_without_response_or_learning(tmp_path):
    from benchmarks.sregym.fastloop.loop import LoopConfig, run_incidents
    from benchmarks.sregym.fastloop.records import summarize

    class InvalidWorker(Worker):
        def request(self, op, **kwargs):
            self.calls.append((op, kwargs))
            if op == "inject":
                return {
                    "protocol_version": 2,
                    "started_at": 1,
                    "finished_at": 2,
                    "injection": {
                        "valid": False,
                        "infra_error": "injection_invalid",
                        "details": {"reason": "not live"},
                    },
                }
            if op == "recover":
                return {"protocol_version": 2, "success": True, "status": "recovered", "seconds": 0.1}
            if op == "health":
                return {"healthy": True}
            pytest.fail("no oracle or further incident work may run for invalid injection")

    class Agent:
        name = "sdo"
        model = "scripted"

        def resolve(self, index, problem_id, inject):
            inject()
            pytest.fail("must not respond after invalid injection")

        def learn(self, outcome):
            pytest.fail("must not learn after invalid injection")

        def close(self):
            pass

    worker = InvalidWorker({})
    records = run_incidents(
        Agent(),
        SregymFaultDriver(worker, namespace="hotel-reservation"),
        LoopConfig("invalid", ("bad",), 1, tmp_path / "incidents.jsonl"),
    )
    assert len(records) == 1
    assert records[0].infra_error == "injection_invalid"
    assert records[0].injection_evidence == {"reason": "not live"}
    assert records[0].oracle is None
    assert records[0].error is None
    assert [op for op, _ in worker.calls] == ["inject", "recover", "health"]
    assert summarize(records)[0].incidents == 0


def test_real_v2_worker_addresses_nested_components_and_reports_failed_cleanup(tmp_path):
    from pathlib import Path

    from benchmarks.sregym.fastloop.worker_client import SregymWorker

    root = Path(__file__).resolve().parents[5]
    command = [
        "uv",
        "run",
        "--directory",
        str(root / "third_party/sregym"),
        "python",
        str(root / "tests/fixtures/sregym/sdo3/worker.py"),
    ]
    with SregymWorker(command, env={}, log_path=tmp_path / "worker.log") as worker:
        driver = SregymFaultDriver(worker, namespace="hotel-reservation")
        injection = driver.inject_composite(["nested", "single"])
        assert [fault.index for fault in injection.faults] == [0, 1, 2]
        assert [part.injection_order for part in driver.last_components] == [1, 0, 2]
        assert driver.oracle(fault=1).success is False
        assert driver.recover_fault(1) >= 0
        assert driver.recover() >= 0
        events = worker.request("events")["events"]
        assert events[-3:] == [["recover", "injected-first"], ["recover", "single"], ["recover", "declared-first"]]
        driver.inject("broken")
        with pytest.raises(FastloopFaultError, match="recovery.*synthetic cleanup failure"):
            driver.recover()
