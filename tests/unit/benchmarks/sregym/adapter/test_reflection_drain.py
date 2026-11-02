"""Reflection completion, rather than early ACK, owns receipt collection."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.adapter import persistent
from tests.unit.benchmarks.sregym.adapter.test_persistent import DETECTED, FakeOps, _clock

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.adapter.runtime import RuntimeConfig


def test_acknowledged_incident_waits_for_its_queued_reflection_and_rollout() -> None:
    class ProgressingOps(FakeOps):
        reads = 0

        def runtime_state(self, control_namespace: str) -> dict[str, Any]:
            self.reads += 1
            # Includes reflection execution and subsequent rollout, both owned by
            # the real controller's incident-bound queue projection.
            return {
                "last_acknowledged_incident_id": "incident-1",
                "queued_reflection_incident_ids": ["incident-1"] if self.reads < 3 else [],
            }

    ops = ProgressingOps()
    persistent._wait_for_reflection_drain(ops, "demo-sdo", "incident-1", DETECTED + timedelta(minutes=1), _clock(ops))
    assert ops.reads == 3


@pytest.mark.parametrize("unknown", [False, True])
def test_runtime_drain_does_not_confuse_other_incidents_with_terminal_evidence(unknown: bool) -> None:
    state = {
        "last_acknowledged_incident_id": "incident-1",
        "queued_reflection_incident_ids": ["incident-2"],
        "queued_reflections_unknown": unknown,
    }
    assert persistent._closeout_settled(state, incident_id="incident-1") is (not unknown)
    assert not persistent._closeout_settled(state, incident_id="incident-2")


@pytest.mark.parametrize(
    ("processed", "decision", "commit", "settled"),
    [
        (False, None, "", False),
        (True, None, "", False),
        (False, {"learning_decision": "no_change"}, "", False),
        (True, {"learning_decision": "no_change"}, "", True),
        (True, {"learning_decision": "updated"}, "", False),
        (True, {"learning_decision": "updated"}, "accepted", True),
        (True, {"learning_decision": "failed", "failure_reason": "timeout", "error": "expired"}, "", True),
    ],
)
def test_required_reflection_terminal_contract(
    processed: bool, decision: dict[str, str] | None, commit: str, settled: bool
) -> None:
    receipt = {
        "schema_version": "sdo.production-receipt/v2",
        "incident_id": "incident-1",
        "native_ledger": {
            "incident_id": "incident-1",
            "acknowledged": True,
            "requires_reflection": True,
            "reflection_processed": processed,
            "reflection_decision": decision,
            "reflection_commit": commit,
        },
    }
    assert persistent._closeout_settled(receipt) is settled


@pytest.mark.parametrize("success", [False, True])
def test_reflection_with_detector_changes_waits_for_its_matching_rollout(success: bool) -> None:
    expected = {"incident_id": "incident-1", "reflection_commit": "accepted", "after_detector_fingerprint": "new"}
    receipt = {
        "schema_version": "sdo.production-receipt/v2",
        "incident_id": "incident-1",
        "native_ledger": {
            "incident_id": "incident-1",
            "acknowledged": True,
            "requires_reflection": True,
            "reflection_processed": True,
            "reflection_decision": {"learning_decision": "updated"},
            "reflection_commit": "accepted",
            "rollout_expectation": expected,
            "rollout_receipts": [],
        },
    }
    assert not persistent._closeout_settled(receipt)
    receipt["native_ledger"]["rollout_receipts"] = [
        {"success": success, "expectation": expected, "observed_detector_fingerprint": "new"}
    ]
    assert persistent._closeout_settled(receipt) is success


@pytest.mark.go_integration
@pytest.mark.parametrize("decision", ["no_change", "failed"])
def test_real_native_broker_terminal_reflection_reaches_both_drain_consumers(tmp_path: Path, decision: str) -> None:
    import json
    import subprocess
    from pathlib import Path

    fixture = Path(__file__).with_name("testdata") / "reflection_drain.go"
    root = Path(__file__).resolve().parents[5]
    result = subprocess.run(
        ["go", "run", str(fixture), str(tmp_path), decision],
        cwd=root / "sdo/operator/runtime",
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    before, after = json.loads(result.stdout)
    assert before["receipt"]["native_ledger"]["acknowledged"] is True
    assert before["receipt"]["native_ledger"]["requires_reflection"] is True
    assert after["receipt"]["native_ledger"]["reflection_decision"]["learning_decision"] == decision
    for sample, settled in [(before, False), (after, True)]:
        assert persistent._closeout_settled(sample["receipt"]) is settled
        assert persistent._closeout_settled(sample["state"], incident_id="incident-1") is settled

    class BrokerOps(FakeOps):
        state_reads = 0
        receipt_reads = 0

        def runtime_state(self, control_namespace: str) -> dict[str, Any]:
            self.state_reads += 1
            return before["state"] if self.state_reads == 1 else after["state"]

        def collect_receipt(self, config: RuntimeConfig, incident_id: str, artifacts_dir: Path) -> dict[str, Any]:
            self.receipt_reads += 1
            return before["receipt"] if self.receipt_reads == 1 else after["receipt"]

    ops = BrokerOps()
    clock = _clock(ops)
    assert (
        persistent._wait_for_reflection_drain(ops, "demo-sdo", "incident-1", DETECTED + timedelta(minutes=1), clock)
        == after["state"]
    )
    from tests.unit.benchmarks.sregym.adapter.test_persistent import _inputs

    config = _inputs(tmp_path, "s0").runtime_config
    assert persistent._collect_settled_receipt(ops, config, "incident-1", tmp_path, clock) == after["receipt"]
    assert ops.state_reads == ops.receipt_reads == 2


@pytest.mark.parametrize("field", ["incident_id", "reflection_commit", "after_detector_fingerprint"])
def test_rollout_evidence_for_another_expectation_cannot_settle_reflection(field: str) -> None:
    expected = {"incident_id": "incident-1", "reflection_commit": "accepted", "after_detector_fingerprint": "new"}
    receipt = {
        "schema_version": "sdo.production-receipt/v2",
        "incident_id": "incident-1",
        "native_ledger": {
            "incident_id": "incident-1",
            "acknowledged": True,
            "requires_reflection": True,
            "reflection_processed": True,
            "reflection_decision": {"learning_decision": "updated"},
            "reflection_commit": "accepted",
            "rollout_expectation": expected,
            "rollout_receipts": [
                {
                    "success": True,
                    "expectation": {**expected, field: "other"},
                    "observed_detector_fingerprint": "new",
                }
            ],
        },
    }
    assert not persistent._closeout_settled(receipt)


def test_queued_reflection_still_uses_the_existing_deadline() -> None:
    ops = FakeOps(
        states={
            "demo-sdo": {
                "last_acknowledged_incident_id": "incident-1",
                "queued_reflection_incident_ids": ["incident-1"],
                "busy_until": DETECTED.isoformat(),
            }
        }
    )
    with pytest.raises(persistent.ReflectionDeadlineError):
        persistent._wait_for_reflection_drain(ops, "demo-sdo", "incident-1", DETECTED, _clock(ops))
