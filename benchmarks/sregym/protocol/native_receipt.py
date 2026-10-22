"""Typed transport for the Go-owned standalone native receipt decision."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from google.protobuf import json_format

from benchmarks.sregym.protocol._receipt_checker import ReceiptCheckerError, evaluate_receipt
from benchmarks.sregym.protocol.receipt import ProductionReceiptValidationError
from sdo.contracts import NATIVE_RECEIPT_SCHEMA_VERSION, NativeProductionReceipt, ReceiptFailure


class NativeReceiptValidationError(ProductionReceiptValidationError):
    """Keep the typed incident failure and raw evidence for rejection publication."""

    def __init__(self, message: str, receipt: dict[str, Any], failure: ReceiptFailure | None = None) -> None:
        super().__init__(message)
        self.receipt = receipt
        self.failure = failure or ReceiptFailure(
            incident_id=str(receipt.get("incident_id") or ""),
            code=ReceiptFailure.CODE_REJECTED_EVIDENCE,
            message=message,
        )


def _evaluate(receipt: dict[str, Any], *, build: bool) -> dict[str, Any]:
    try:
        result = evaluate_receipt(json.dumps(receipt), build=build)
    except ReceiptCheckerError as exc:
        failure = ReceiptFailure(
            incident_id=str(receipt.get("incident_id") or ""),
            code=ReceiptFailure.CODE_CHECKER_UNAVAILABLE,
            message=str(exc),
        )
        raise NativeReceiptValidationError(str(exc), receipt, failure) from exc
    document = json_format.MessageToDict(result.receipt, preserving_proto_field_name=True)
    # Keep the original ledger and Job evidence, including malformed values and
    # transport annotations, in diagnostics. Only Go authors the flat summary.
    if build:
        for field in (
            "proposal_commit",
            "outcome_commit",
            "reflection_commit",
            "reflection_usage",
            "reflection_skipped_reason",
            "outcome_classification",
            "resolution",
        ):
            receipt[field] = document.get(field)
    if result.HasField("failure"):
        raise NativeReceiptValidationError(result.failure.message, receipt, result.failure)
    return receipt


def native_receipt_resolution(receipt: dict[str, Any]) -> str:
    """Read the Go-derived classification summary without crediting validation."""

    try:
        result = evaluate_receipt(json.dumps(receipt), build=True)
    except ReceiptCheckerError as exc:
        raise NativeReceiptValidationError(str(exc), receipt) from exc
    if result.failure.code == ReceiptFailure.CODE_INVALID_WIRE:
        raise NativeReceiptValidationError(result.failure.message, receipt, result.failure)
    return result.receipt.resolution


def validate_native_production_receipt(receipt: dict[str, Any]) -> None:
    _evaluate(receipt, build=False)


def native_production_receipt(
    ledger: dict[str, Any],
    *,
    namespace: str,
    control_namespace: str,
    controller_image: str,
    responder_image: str,
    validator_image: str,
    controller_pod_uid: str,
    responder_job_evidence: list[dict[str, Any]],
) -> dict[str, Any]:
    receipt = NativeProductionReceipt(
        schema_version=NATIVE_RECEIPT_SCHEMA_VERSION,
        broker_mode="native",
        worker_mount_mode="proposal-only",
        incident_id=str(ledger.get("incident_id") or ""),
        namespace=namespace,
        control_namespace=control_namespace,
        controller_workload="batch/v1 Job/sdo-controller-run",
        controller_image=controller_image,
        responder_image=responder_image,
        validator_image=validator_image,
        controller_pod_uid=controller_pod_uid,
    )
    receipt.recorded_at.FromDatetime(datetime.now(timezone.utc))
    document = json_format.MessageToDict(receipt, preserving_proto_field_name=True)
    # These are opaque bytes supplied by the Go evidence collector and Kubernetes
    # transport. Their shapes and decisions are checked by the generated Go types.
    document["native_ledger"] = ledger
    document["responder_job_evidence"] = responder_job_evidence
    return _evaluate(document, build=True)
