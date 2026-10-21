from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pytest

from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
from benchmarks.sregym.fastloop.fault_driver import FastloopFaultError, SregymFaultDriver


@dataclass
class FakeWorker:
    health_sequence: list[bool] = field(default_factory=lambda: [True])
    requests: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    def request(self, op: str, *, timeout: float = 0, **args: Any) -> dict[str, Any]:
        self.requests.append((op, args))
        if op == "inject":
            return {"started_at": 1790000000.0, "finished_at": 1790000006.5}
        if op == "oracle":
            return {"kind": "sregym-mitigation-oracle", "success": True, "details": {"success": True}}
        if op == "recover":
            return {"seconds": 4.0}
        if op == "health":
            healthy = self.health_sequence.pop(0) if len(self.health_sequence) > 1 else self.health_sequence[0]
            return {"healthy": healthy, "unready": [] if healthy else ["mongodb-geo"]}
        raise AssertionError(op)


def _driver(worker: FakeWorker, timeout: float = 30.0) -> SregymFaultDriver:
    now = [0.0]

    def sleep(seconds: float) -> None:
        now[0] += seconds

    return SregymFaultDriver(
        worker, namespace="hotel-reservation", health_timeout_seconds=timeout, monotonic=lambda: now[0], sleep=sleep
    )


def test_injection_window_comes_from_the_worker_clock() -> None:
    window = _driver(FakeWorker()).inject("missing_configmap_hotel_reservation")

    assert window.started_at == datetime.fromtimestamp(1790000000.0, tz=timezone.utc)
    assert (window.finished_at - window.started_at).total_seconds() == pytest.approx(6.5)


def test_recovery_waits_until_the_app_is_healthy_again() -> None:
    worker = FakeWorker(health_sequence=[False, False, True])

    seconds = _driver(worker).recover()

    assert [op for op, _ in worker.requests] == ["recover", "health", "health", "health"]
    assert seconds == pytest.approx(2 * 2.0)


def test_recovery_waits_for_rollouts_in_every_application_namespace() -> None:
    class Cluster:
        calls = 0

        def list_objects(self, namespace: str) -> list[dict[str, Any]]:
            self.calls += 1
            ready = self.calls >= 4
            return [
                {
                    "kind": "Deployment" if namespace == "hotel" else "StatefulSet",
                    "metadata": {"namespace": namespace, "name": "app", "generation": 2},
                    "spec": {"replicas": 1},
                    "status": {
                        "observedGeneration": 2 if ready else 1,
                        "updatedReplicas": 1,
                        "readyReplicas": 1 if ready else 0,
                        "replicas": 1,
                    },
                },
                {
                    "kind": "Pod",
                    "metadata": {
                        "namespace": namespace,
                        "name": "old-app",
                        "ownerReferences": [{"name": "app"}],
                        **({"deletionTimestamp": "now"} if not ready else {}),
                    },
                },
            ]

        def delete(self, namespace: str, kind: str, name: str) -> None:
            raise AssertionError(kind)

        def apply(self, namespace: str, value: dict[str, Any]) -> None:
            raise AssertionError(value)

    cluster = Cluster()
    driver = _driver(FakeWorker())
    driver._baselines = tuple(
        ApplicationBaseline(
            namespace,
            [
                {
                    "kind": "Deployment" if namespace == "hotel" else "StatefulSet",
                    "metadata": {"namespace": namespace, "name": "app"},
                    "spec": {"replicas": 1},
                }
            ],
        )
        for namespace in ("hotel", "db")
    )
    driver._cluster = cluster

    assert driver.recover() == pytest.approx(2.0)
    assert cluster.calls == 6


def test_recovery_rollout_timeout_reports_unready_workload() -> None:
    class Cluster:
        def list_objects(self, namespace: str) -> list[dict[str, Any]]:
            return [
                {
                    "kind": "Deployment",
                    "metadata": {"namespace": namespace, "name": "rate", "generation": 2},
                    "spec": {"replicas": 1},
                    "status": {"observedGeneration": 1, "replicas": 1, "updatedReplicas": 1, "readyReplicas": 0},
                }
            ]

        def delete(self, namespace: str, kind: str, name: str) -> None:
            raise AssertionError(kind)

        def apply(self, namespace: str, value: dict[str, Any]) -> None:
            raise AssertionError(value)

    driver = _driver(FakeWorker(), timeout=5.0)
    driver._baselines = (
        ApplicationBaseline(
            "hotel-reservation",
            [
                {
                    "kind": "Deployment",
                    "metadata": {"namespace": "hotel-reservation", "name": "rate"},
                    "spec": {"replicas": 1},
                }
            ],
        ),
    )
    driver._cluster = Cluster()

    with pytest.raises(FastloopFaultError, match="Deployment/rate"):
        driver.recover()


def test_recovery_that_never_becomes_healthy_fails_the_loop() -> None:
    with pytest.raises(FastloopFaultError, match="mongodb-geo"):
        _driver(FakeWorker(health_sequence=[False]), timeout=5.0).recover()


def test_recovery_reconciles_before_waiting_for_health() -> None:
    events: list[str] = []

    class Cluster:
        def list_objects(self, namespace: str) -> list[dict[str, Any]]:
            events.append("list")
            return [{"kind": "Job", "metadata": {"namespace": namespace, "name": "grant-fix"}, "spec": {}}]

        def delete(self, namespace: str, kind: str, name: str) -> None:
            events.append("delete")

        def apply(self, namespace: str, value: dict[str, Any]) -> None:
            events.append("apply")

    class Worker(FakeWorker):
        def request(self, op: str, *, timeout: float = 0, **args: Any) -> dict[str, Any]:
            events.append(op)
            return super().request(op, timeout=timeout, **args)

    driver = SregymFaultDriver(
        Worker(),
        namespace="hotel-reservation",
        baseline=ApplicationBaseline("hotel-reservation", []),
        cluster=Cluster(),
    )

    driver.recover()

    assert events == ["recover", "list", "delete", "health", "list"]
    assert driver.last_cleanup.deleted_counts == {"Job": 1}


def test_recovery_cleans_every_recorded_application_namespace() -> None:
    class Cluster:
        def list_objects(self, namespace: str) -> list[dict[str, Any]]:
            return [{"kind": "Job", "metadata": {"namespace": namespace, "name": "agent-fix"}, "spec": {}}]

        def delete(self, namespace: str, kind: str, name: str) -> None:
            deleted.append((namespace, kind, name))

        def apply(self, namespace: str, value: dict[str, Any]) -> None:
            raise AssertionError(value)

    deleted: list[tuple[str, str, str]] = []
    driver = SregymFaultDriver(
        FakeWorker(),
        namespace="hotel-reservation",
        baseline=(ApplicationBaseline("hotel-reservation", []), ApplicationBaseline("hotel-db", [])),
        cluster=Cluster(),
    )

    driver.recover()

    assert deleted == [("hotel-reservation", "Job", "agent-fix"), ("hotel-db", "Job", "agent-fix")]
    assert driver.last_cleanup.deleted_counts == {"Job": 2}


def test_oracle_verdict_is_typed() -> None:
    verdict = _driver(FakeWorker()).oracle()

    assert verdict.kind == "sregym-mitigation-oracle"
    assert verdict.success


@dataclass
class ComposingWorker(FakeWorker):
    injected: int = 0

    def request(self, op: str, *, timeout: float = 0, **args: Any) -> dict[str, Any]:
        if op == "inject":
            self.requests.append((op, args))
            self.injected = self.injected + 1 if args.get("compose") else 1
            start = 1790000000.0 + 10 * len(self.requests)
            return {"started_at": start, "finished_at": start + 1.0, "fault": self.injected - 1}
        if op == "recover":
            self.requests.append((op, args))
            return {"seconds": 1.5, "recovered": [args["fault"]] if "fault" in args else [1, 0]}
        if op == "oracle":
            self.requests.append((op, args))
            return {"kind": "composition", "success": False, "details": {"faults": []}}
        return super().request(op, timeout=timeout, **args)


def test_a_composite_fault_injects_every_problem_into_one_composition() -> None:
    worker = ComposingWorker()

    composite = _driver(worker).inject_composite(["network_policy_block", "wrong_service_selector_hotel_reservation"])

    assert [(op, args.get("problem_id"), args.get("compose")) for op, args in worker.requests] == [
        ("inject", "network_policy_block", False),
        ("inject", "wrong_service_selector_hotel_reservation", True),
    ]
    assert [fault.index for fault in composite.faults] == [0, 1]
    assert composite.window.started_at == composite.faults[0].window.started_at
    assert composite.window.finished_at == composite.faults[1].window.finished_at


@pytest.mark.parametrize("problems", [[], ["a", "a"], ["a", ""]])
def test_a_composite_fault_needs_distinct_problems(problems: list[str]) -> None:
    with pytest.raises(ValueError, match="composite fault"):
        _driver(ComposingWorker()).inject_composite(problems)


def test_recovering_one_fault_of_a_composition_does_not_wait_for_health() -> None:
    worker = ComposingWorker()
    driver = _driver(worker)
    driver.inject_composite(["a", "b"])

    seconds = driver.recover_fault(1)

    assert worker.requests[-1] == ("recover", {"fault": 1})
    assert seconds == pytest.approx(1.5)
    assert all(op != "health" for op, _ in worker.requests)


def test_recovering_the_composition_reverts_the_rest_and_waits_for_health() -> None:
    worker = ComposingWorker(health_sequence=[False, True])
    driver = _driver(worker)
    driver.inject_composite(["a", "b"])

    driver.recover()

    assert [op for op, _ in worker.requests][-3:] == ["recover", "health", "health"]


def test_a_composition_oracle_is_typed() -> None:
    worker = ComposingWorker()
    driver = _driver(worker)
    driver.inject_composite(["a", "b"])

    assert driver.oracle().kind == "composition"
    driver.oracle(fault=0)
    assert worker.requests[-1] == ("oracle", {"fault": 0})


def test_partial_composite_injection_recovers_the_first_fault() -> None:
    class FailingWorker(ComposingWorker):
        def request(self, op: str, *, timeout: float = 0, **args: Any) -> dict[str, Any]:
            if op == "inject" and args.get("compose"):
                raise RuntimeError("second fault unavailable")
            return super().request(op, timeout=timeout, **args)

    worker = FailingWorker()

    with pytest.raises(FastloopFaultError, match="second fault unavailable"):
        _driver(worker).inject_composite(["a", "b"])
    assert [op for op, _ in worker.requests][-2:] == ["recover", "health"]
