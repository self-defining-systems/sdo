"""Hand-written seed recovery state machine; no cluster or recorded run data."""

from __future__ import annotations

from typing import Any

import pytest

from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
from benchmarks.sregym.fastloop.fault_driver import FastloopFaultError, SregymFaultDriver
from tests.unit.benchmarks.sregym.fastloop.accepted_source_fixture import fixed_source_provider

CONTRACT = "seed-compatible/v1"


class CredentialLane:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.backend = "fake-rotated"
        self.manifest = "fake-seed"
        self.client = "fake-rotated"
        self.contract = CONTRACT
        self.refresh_success = True
        self.reset_success = True
        self.apply_failure = False

    def request(self, op: str, *, timeout: float = 0, **args: Any) -> dict[str, Any]:
        self.events.append(op)
        if op == "recover":
            if self.reset_success:
                self.backend = "fake-seed"
            return {
                "protocol_version": 2,
                "recovery_contract": self.contract,
                "success": self.reset_success,
                "seconds": 0,
            }
        if op == "refresh":
            assert self.backend == self.manifest
            if self.refresh_success:
                self.client = self.manifest
            return {"protocol_version": 2, "recovery_contract": self.contract, "success": self.refresh_success}
        if op == "health":
            return {"healthy": self.backend == self.manifest == self.client}
        raise AssertionError(op)

    def list_objects(self, namespace: str) -> list[dict[str, Any]]:
        return []

    def delete(self, namespace: str, kind: str, name: str) -> None:
        raise AssertionError("no live objects")

    def apply(self, namespace: str, value: dict[str, Any]) -> None:
        self.events.append("seed")
        assert self.backend == "fake-seed"
        if self.apply_failure:
            raise RuntimeError("seed restore unavailable")
        self.manifest = "fake-seed"


def driver(lane: CredentialLane) -> SregymFaultDriver:
    return SregymFaultDriver(
        lane,
        namespace="toy",
        baseline=ApplicationBaseline(
            "toy", [{"kind": "ConfigMap", "metadata": {"name": "toy-init", "namespace": "toy"}}]
        ),
        cluster=lane,
        require_full_cleanup=True,
        health_timeout_seconds=1,
        health_poll_seconds=0.001,
        accepted_source=fixed_source_provider(
            ApplicationBaseline("toy", [{"kind": "ConfigMap", "metadata": {"name": "toy-init", "namespace": "toy"}}]),
            "toy",
        ),
    )


def test_credential_cleanup_resets_then_reapplies_seed_then_refreshes_clients() -> None:
    lane = CredentialLane()
    recovery = driver(lane)
    recovery.recover()
    recovery.recover()
    assert lane.events == ["recover", "seed", "refresh", "health"] * 2
    assert recovery.last_full_cleanup_verified


@pytest.mark.parametrize("failure", ["contract", "reset", "seed", "refresh"])
def test_incomplete_credential_cleanup_never_credits_or_polls_health(failure: str) -> None:
    lane = CredentialLane()
    if failure == "contract":
        lane.contract = "old-contract"
    elif failure == "reset":
        lane.reset_success = False
    elif failure == "seed":
        lane.apply_failure = True
    else:
        lane.refresh_success = False
    recovery = driver(lane)
    with pytest.raises((FastloopFaultError, RuntimeError)):
        recovery.recover()
    assert not recovery.last_full_cleanup_verified
    assert "health" not in lane.events
    if failure in {"contract", "reset"}:
        assert "seed" not in lane.events
    if failure == "seed":
        assert "refresh" not in lane.events


def test_seed_restore_refuses_legacy_harness_even_without_strict_flag() -> None:
    lane = CredentialLane()
    lane.contract = "old-contract"
    recovery = SregymFaultDriver(
        lane,
        namespace="toy",
        baseline=ApplicationBaseline("toy", []),
        cluster=lane,
        health_timeout_seconds=0.01,
        health_poll_seconds=0.001,
        accepted_source=fixed_source_provider(ApplicationBaseline("toy", []), "toy"),
    )
    with pytest.raises(FastloopFaultError, match="seed-compatible"):
        recovery.recover()
    assert lane.events == ["recover"]


@pytest.mark.parametrize("phase", ["recover", "refresh"])
@pytest.mark.parametrize(
    "invalid",
    [
        {"protocol_version": 1},
        {"success": 1},
        {"recovery_contract": "old-contract"},
        {"pending": [0]},
        {"errors": [{"error": "FAKE-CREDENTIAL-CANARY"}]},
    ],
)
def test_incomplete_recovery_reply_cannot_authorize_seed_reuse_or_health(phase: str, invalid: dict[str, Any]) -> None:
    class Worker(CredentialLane):
        def request(self, op: str, *, timeout: float = 0, **args: Any) -> dict[str, Any]:
            reply = super().request(op, timeout=timeout, **args)
            return {**reply, **invalid} if op == phase else reply

    lane = Worker()
    recovery = driver(lane)
    with pytest.raises(FastloopFaultError) as error:
        recovery.recover()
    assert "FAKE-CREDENTIAL-CANARY" not in str(error.value)
    assert not recovery.last_full_cleanup_verified
    assert "health" not in lane.events
    if phase == "recover":
        assert "seed" not in lane.events


@pytest.mark.parametrize("failure", ["seed", "refresh"])
def test_cleanup_retry_finishes_the_same_slot_after_a_recoverable_failure(failure: str) -> None:
    lane = CredentialLane()
    lane.apply_failure = failure == "seed"
    lane.refresh_success = failure != "refresh"
    recovery = driver(lane)
    with pytest.raises(RuntimeError):
        recovery.recover()
    lane.apply_failure = False
    lane.refresh_success = True
    recovery.recover()
    assert recovery.last_full_cleanup_verified
    assert lane.events[-4:] == ["recover", "seed", "refresh", "health"]
