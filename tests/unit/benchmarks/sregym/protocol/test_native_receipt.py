"""Strict native Go evidence behind benchmark production receipt v2."""

from __future__ import annotations

from copy import deepcopy

import pytest

from benchmarks.sregym.protocol.native_receipt import native_production_receipt
from benchmarks.sregym.protocol.receipt import ProductionReceiptValidationError, validate_production_receipt

SHA_A = "a" * 40
SHA_B = "b" * 40
SHA_C = "c" * 40


def _ledger() -> dict[str, object]:
    return {
        "incident_id": "incident-1",
        "closure": {
            "request": {"incident_id": "incident-1", "namespace": "demo", "repair_policy": "recorded-actions"},
            "result": {
                "incident_id": "incident-1",
                "status": "completed",
                "repair_actions": [{"action_id": "repair-1", "success": True}],
                "confirmed_root_causes": [{"summary": "unavailable replica"}],
            },
            "final_detector_states": [{"detector_id": "health-objective", "status": "clear", "fingerprints": []}],
            "verified_at": "2026-10-18T03:00:00Z",
        },
        "proposal_commit": SHA_A,
        "outcome_document": {
            "schema_version": 1,
            "incident_id": "incident-1",
            "classification": "success",
            "repair_commit": SHA_A,
            "diagnosis_verification": [{"verdict": "confirmed"}],
        },
        "requires_reflection": True,
        "outcome_commit": SHA_B,
        "reflection_processed": True,
        "reflection_decision": {"learning_decision": "no_change"},
        "reflection_commit": SHA_C,
        "receipt": {
            "incident_id": "incident-1",
            "proposal_commit": SHA_A,
            "outcome_commit": SHA_B,
            "reflection_commit": SHA_C,
            "ack_token": "token",
        },
        "acknowledged": True,
    }


def _receipt(ledger: dict[str, object] | None = None) -> dict[str, object]:
    return native_production_receipt(
        ledger or _ledger(),
        namespace="demo",
        control_namespace="demo-sdo",
        controller_image="controller:go",
        responder_image="responder:python",
        validator_image="validator:go",
        controller_pod_uid="pod-uid",
        responder_job_evidence=[
            {"incident_id": "incident-1", "job_name": "sdo-incident-1-hash", "job_uid": "worker-uid", "succeeded": True}
        ],
    )


def test_native_receipt_verifies_correlated_go_ledger_and_health() -> None:
    receipt = _receipt()
    validate_production_receipt(receipt)
    assert receipt["schema_version"] == "sdo.production-receipt/v2"
    assert receipt["resolution"] == "sdo_mitigated"
    assert receipt["outcome_commit"] == SHA_B


def test_native_receipt_exports_reflection_usage() -> None:
    ledger = _ledger()
    ledger["reflection_decision"]["usage"] = {"input_tokens": 42, "output_tokens": 7}
    assert _receipt(ledger)["reflection_usage"] == {"input_tokens": 42, "output_tokens": 7}


@pytest.mark.parametrize(
    "change",
    [
        lambda ledger: ledger.pop("outcome_document"),
        lambda ledger: ledger.update(acknowledged=False),
        lambda ledger: ledger["receipt"].update(outcome_commit=SHA_C),
        lambda ledger: ledger["closure"]["final_detector_states"][0].update(status="firing"),
        lambda ledger: ledger["outcome_document"].update(incident_id="different"),
        lambda ledger: ledger.update(reflection_commit=""),
    ],
)
def test_native_receipt_rejects_missing_or_conflicting_evidence(change: object) -> None:
    ledger = deepcopy(_ledger())
    change(ledger)  # type: ignore[operator]
    with pytest.raises(ProductionReceiptValidationError):
        _receipt(ledger)


def test_native_receipt_requires_verified_detector_rollout_when_expected() -> None:
    ledger = _ledger()
    ledger["rollout_expectation"] = {
        "incident_id": "incident-1",
        "reflection_commit": SHA_C,
        "before_detector_fingerprint": "old",
        "after_detector_fingerprint": "new",
    }
    with pytest.raises(ProductionReceiptValidationError, match="rollout"):
        _receipt(ledger)
    ledger["rollout_receipts"] = [
        {
            "expectation": ledger["rollout_expectation"],
            "success": True,
            "observed_detector_fingerprint": "new",
            "controller_revision": SHA_C,
            "build_artifact": "sha256:abc",
            "verified_at": "2026-10-18T03:01:00Z",
        }
    ]
    validate_production_receipt(_receipt(ledger))
