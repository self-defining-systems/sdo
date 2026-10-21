"""Strict standalone receipt from the durable Go incident broker ledger."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, cast

from benchmarks.sregym.protocol.receipt import (
    CLEARED_WITHOUT_SDO_ACTION,
    EXTERNAL_RECOVERY,
    SDO_MITIGATED,
    ProductionReceiptValidationError,
)

_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_ALLOWED_CLASSIFICATIONS = {"success", "cleared_without_action", "external_recovery", "false_positive"}


def _object(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProductionReceiptValidationError(f"native {name} must be a JSON object")
    candidate = cast("dict[object, object]", value)
    if not all(isinstance(key, str) for key in candidate):
        raise ProductionReceiptValidationError(f"native {name} must be a JSON object")
    return cast("dict[str, Any]", candidate)


def _nonempty(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProductionReceiptValidationError(f"native {name} is missing")
    return value


def _commit(value: object, name: str, *, optional: bool = False) -> str | None:
    if optional and (value is None or value == ""):
        return None
    if not isinstance(value, str) or _COMMIT.fullmatch(value) is None:
        raise ProductionReceiptValidationError(f"native {name} must be a full Git commit")
    return value


def _items(value: object, name: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ProductionReceiptValidationError(f"native {name} must be a list of objects")
    candidate = cast("list[object]", value)
    if not all(isinstance(item, dict) for item in candidate):
        raise ProductionReceiptValidationError(f"native {name} must be a list of objects")
    return cast("list[dict[str, Any]]", candidate)


def _timestamp(value: object, name: str) -> None:
    text = _nonempty(value, name)
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProductionReceiptValidationError(f"native {name} must be an ISO timestamp") from exc


def validate_native_production_receipt(receipt: dict[str, Any]) -> None:
    if receipt.get("schema_version") != "sdo.production-receipt/v2":
        raise ProductionReceiptValidationError("invalid native production receipt schema_version")
    if receipt.get("broker_mode") != "native" or receipt.get("worker_mount_mode") != "proposal-only":
        raise ProductionReceiptValidationError("native receipt requires Go broker and proposal-only worker mounts")
    incident_id = _nonempty(receipt.get("incident_id"), "incident ID")
    _nonempty(receipt.get("namespace"), "namespace")
    _nonempty(receipt.get("control_namespace"), "control namespace")
    for name in ("controller_image", "responder_image", "validator_image", "controller_pod_uid"):
        _nonempty(receipt.get(name), name)
    ledger = _object(receipt.get("native_ledger"), "broker ledger")
    if ledger.get("incident_id") != incident_id or ledger.get("acknowledged") is not True:
        raise ProductionReceiptValidationError("native broker ledger is not acknowledged for this incident")
    closure = _object(ledger.get("closure"), "closure")
    request = _object(closure.get("request"), "closure request")
    result = _object(closure.get("result"), "responder result")
    outcome = _object(ledger.get("outcome_document"), "outcome document")
    broker_receipt = _object(ledger.get("receipt"), "closure receipt")
    if any(item.get("incident_id") != incident_id for item in (request, result, outcome, broker_receipt)):
        raise ProductionReceiptValidationError("native incident identities disagree")
    if request.get("namespace") != receipt["namespace"]:
        raise ProductionReceiptValidationError("native closure namespace disagrees with receipt")
    if request.get("repair_policy") not in ("commit", "recorded-actions"):
        raise ProductionReceiptValidationError("native closure lacks valid repair policy")
    if outcome.get("schema_version") != 1 or outcome.get("classification") not in _ALLOWED_CLASSIFICATIONS:
        raise ProductionReceiptValidationError("native outcome is not a verified closure classification")
    proposal = _commit(ledger.get("proposal_commit"), "proposal commit", optional=True)
    outcome_commit = _commit(ledger.get("outcome_commit"), "outcome commit")
    reflection = _commit(ledger.get("reflection_commit"), "reflection commit", optional=True)
    if _commit(outcome.get("repair_commit"), "outcome repair commit", optional=True) != proposal:
        raise ProductionReceiptValidationError("native outcome repair commit disagrees with broker proposal")
    if request["repair_policy"] == "commit" and outcome["classification"] == "success" and proposal is None:
        raise ProductionReceiptValidationError("native commit repair lacks an accepted proposal")
    for name, expected in (
        ("proposal_commit", proposal),
        ("outcome_commit", outcome_commit),
        ("reflection_commit", reflection),
    ):
        if _commit(broker_receipt.get(name), f"closure receipt {name}", optional=name != "outcome_commit") != expected:
            raise ProductionReceiptValidationError(f"native closure receipt {name} disagrees with broker ledger")
    _nonempty(broker_receipt.get("ack_token"), "acknowledgment token")
    states = _items(closure.get("final_detector_states"), "final detector states")
    if not states or any(state.get("status") != "clear" or state.get("fingerprints") != [] for state in states):
        raise ProductionReceiptValidationError("native closure lacks independently clear health detectors")
    if closure.get("detector_review_required_at"):
        raise ProductionReceiptValidationError("native closure entered detector review")
    _timestamp(closure.get("verified_at"), "verified_at")
    if outcome["classification"] == "success":
        if result.get("status") != "completed":
            raise ProductionReceiptValidationError("native successful outcome lacks completed responder")
        if not _items(result.get("confirmed_root_causes"), "confirmed root causes"):
            raise ProductionReceiptValidationError("native successful outcome lacks confirmed cause")
        verifications = _items(outcome.get("diagnosis_verification"), "diagnosis verification")
        if not any(item.get("verdict") == "confirmed" for item in verifications):
            raise ProductionReceiptValidationError("native successful outcome lacks independently confirmed diagnosis")
        if proposal is None and not any(
            item.get("success") is True for item in _items(result.get("repair_actions"), "repair actions")
        ):
            raise ProductionReceiptValidationError("native successful outcome lacks accepted repair evidence")
    decision = ledger.get("reflection_decision")
    skipped = ledger.get("reflection_skipped_reason")
    if skipped is not None and (
        not isinstance(skipped, str)
        or not skipped.strip()
        or ledger.get("requires_reflection") is True
        or decision is not None
    ):
        raise ProductionReceiptValidationError("native reflection skip conflicts with broker decision")
    if ledger.get("requires_reflection") is True and (
        ledger.get("reflection_processed") is not True
        or not isinstance(decision, dict)
        # A no_change decision commits nothing; only an updating reflection is bound to a commit.
        or (decision.get("learning_decision") == "updated" and reflection is None)
    ):
        raise ProductionReceiptValidationError("native required reflection is incomplete")
    expectation = ledger.get("rollout_expectation")
    if expectation is not None:
        expected = _object(expectation, "rollout expectation")
        if expected.get("incident_id") != incident_id or expected.get("reflection_commit") != reflection:
            raise ProductionReceiptValidationError("native rollout expectation is not bound to the incident")
        before = _nonempty(expected.get("before_detector_fingerprint"), "prior detector fingerprint")
        after = _nonempty(expected.get("after_detector_fingerprint"), "new detector fingerprint")
        if before == after:
            raise ProductionReceiptValidationError("native rollout did not change detector fingerprint")
        verified = [
            item for item in _items(ledger.get("rollout_receipts"), "rollout receipts") if item.get("success") is True
        ]
        if (
            len(verified) != 1
            or verified[0].get("expectation") != expected
            or verified[0].get("observed_detector_fingerprint") != after
        ):
            raise ProductionReceiptValidationError("native rollout lacks one matching verified receipt")
        for name in ("build_artifact", "controller_revision"):
            _nonempty(verified[0].get(name), f"rollout {name}")
        _timestamp(verified[0].get("verified_at"), "rollout verified_at")
    jobs = _items(receipt.get("responder_job_evidence"), "responder Job evidence")
    if outcome.get("resolution_path") == "fast_path":
        programs = outcome.get("fast_path_programs")
        applications = _items(outcome.get("plan_applications"), "fast path plan applications")
        if (
            jobs
            or request.get("repair_policy") != "recorded-actions"
            or result.get("resolution_path") != "fast_path"
            or result.get("fast_path_programs") != programs
            or not isinstance(programs, list)
            or not programs
            or ledger.get("reflection_skipped_reason") != "trusted program fast path"
            or result.get("program_failure_signals")
            or not applications
            or any(
                item.get("applied") is not True
                or item.get("rolled_back") is True
                or item.get("verification", {}).get("result") != "RESULT_NO_MATCH"
                for item in applications
            )
        ):
            raise ProductionReceiptValidationError("native fast path lacks verified program and reflection evidence")
    else:
        if len(jobs) != 1 or jobs[0].get("incident_id") != incident_id or jobs[0].get("succeeded") is not True:
            raise ProductionReceiptValidationError("native receipt requires one correlated successful responder Job")
        _nonempty(jobs[0].get("job_uid"), "responder Job UID")
        _nonempty(jobs[0].get("job_name"), "responder Job name")
    if (
        receipt.get("outcome_commit") != outcome_commit
        or receipt.get("reflection_commit") != reflection
        or receipt.get("proposal_commit") != proposal
    ):
        raise ProductionReceiptValidationError("native flat commit summary disagrees with ledger")
    classification = outcome["classification"]
    resolution = (
        SDO_MITIGATED
        if classification == "success"
        else CLEARED_WITHOUT_SDO_ACTION
        if classification == "cleared_without_action"
        else EXTERNAL_RECOVERY
    )
    if receipt.get("resolution") != resolution:
        raise ProductionReceiptValidationError("native resolution disagrees with independently derived outcome")


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
    outcome = _object(ledger.get("outcome_document"), "outcome document")
    classification = outcome.get("classification")
    resolution = (
        SDO_MITIGATED
        if classification == "success"
        else CLEARED_WITHOUT_SDO_ACTION
        if classification == "cleared_without_action"
        else EXTERNAL_RECOVERY
    )
    receipt: dict[str, Any] = {
        "schema_version": "sdo.production-receipt/v2",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "broker_mode": "native",
        "worker_mount_mode": "proposal-only",
        "incident_id": ledger.get("incident_id"),
        "namespace": namespace,
        "control_namespace": control_namespace,
        "controller_workload": "batch/v1 Job/sdo-controller-run",
        "controller_image": controller_image,
        "responder_image": responder_image,
        "validator_image": validator_image,
        "controller_pod_uid": controller_pod_uid,
        "native_ledger": ledger,
        "responder_job_evidence": responder_job_evidence,
        "proposal_commit": ledger.get("proposal_commit") or None,
        "outcome_commit": ledger.get("outcome_commit"),
        "reflection_commit": ledger.get("reflection_commit") or None,
        "reflection_usage": (ledger.get("reflection_decision") or {}).get("usage"),
        "reflection_skipped_reason": ledger.get("reflection_skipped_reason"),
        "outcome_classification": classification,
        "resolution": resolution,
    }
    validate_native_production_receipt(receipt)
    return receipt
