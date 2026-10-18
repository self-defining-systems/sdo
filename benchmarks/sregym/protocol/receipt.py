"""Canonical validation for standalone SDO production receipts.

This module is deliberately in the dependency-free SREGym integration layer:
both the generic pipeline gate and the application-specific runtime must apply
the exact same artifact contract without introducing a dependency cycle.
"""

from __future__ import annotations

from datetime import datetime  # noqa: TC003 -- Pydantic resolves this annotation at runtime.
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, StrictInt, ValidationError, model_validator


class ProductionReceiptValidationError(ValueError):
    """Raised when an SDO receipt cannot prove production completion."""


#: The controller verified health after a responder repair that backs its causes.
SDO_MITIGATED = "sdo_mitigated"
#: The responder repaired something, but none of it backs its causes: someone else restored health.
EXTERNAL_RECOVERY = "external_recovery"
#: Health cleared with no successful mutation by the responder (a transient that healed on its own).
CLEARED_WITHOUT_SDO_ACTION = "cleared_without_sdo_action"
RECEIPT_RESOLUTIONS = (SDO_MITIGATED, EXTERNAL_RECOVERY, CLEARED_WITHOUT_SDO_ACTION)


def receipt_resolution(receipt: dict[str, Any]) -> str:
    """How the incident closed, derived from the receipt's own evidence.

    Only ``sdo_mitigated`` credits SDO with the recovery and needs the
    responder's completion and same-session reflection. The other two are
    valid audit records of a verified closure that must never be counted as a
    mitigation.
    """

    if receipt.get("schema_version") == "sdo.production-receipt/v2":
        ledger = receipt.get("native_ledger")
        outcome = ledger.get("outcome_document") if isinstance(ledger, dict) else None
        classification = outcome.get("classification") if isinstance(outcome, dict) else None
        if classification == "success":
            return SDO_MITIGATED
        if classification == "cleared_without_action":
            return CLEARED_WITHOUT_SDO_ACTION
        return EXTERNAL_RECOVERY
    actions = receipt.get("repair_actions")
    mutated = bool(receipt.get("proposal_commit")) or any(
        isinstance(action, dict) and cast("dict[str, Any]", action).get("success") is True
        for action in (cast("list[object]", actions) if isinstance(actions, list) else [])
    )
    if not mutated:
        return CLEARED_WITHOUT_SDO_ACTION
    if receipt.get("recovery_attribution") == "external":
        return EXTERNAL_RECOVERY
    return SDO_MITIGATED


def _string_keyed_objects(value: object) -> list[dict[str, object]] | None:
    if not isinstance(value, list):
        return None
    result: list[dict[str, object]] = []
    for item in cast("list[object]", value):
        if not isinstance(item, dict):
            return None
        candidate = cast("dict[object, object]", item)
        if not all(isinstance(key, str) for key in candidate):
            return None
        result.append(cast("dict[str, object]", candidate))
    return result


class _ControllerRolloutRecord(BaseModel):
    """Cross-layer schema for rollout evidence embedded in a receipt."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["sdo.controller-rollout/v1"] = "sdo.controller-rollout/v1"
    incident_id: str = Field(min_length=1)
    reflection_commit: str = Field(min_length=1)
    before_detector_fingerprint: str = Field(min_length=1)
    after_detector_fingerprint: str = Field(min_length=1)
    controller_job: str = Field(min_length=1)
    controller_pod_uid: str = Field(min_length=1)
    started_at: datetime
    completed_at: datetime
    returncode: StrictInt
    success: bool

    @model_validator(mode="after")
    def validate_attempt(self) -> _ControllerRolloutRecord:
        if self.completed_at < self.started_at:
            raise ValueError("completed_at must not precede started_at")
        if self.success != (self.returncode == 0):
            raise ValueError("success must be true exactly when returncode is zero")
        return self


class _RepairActionReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: str = Field(min_length=1)
    kind: str = Field(min_length=1)
    target: str = Field(min_length=1)
    resources: list[dict[str, Any]] = Field(default_factory=lambda: cast("list[dict[str, Any]]", []))
    summary: str = Field(min_length=1)
    details: str = Field(min_length=1)
    started_at: datetime
    completed_at: datetime
    success: bool
    reversible: bool

    @model_validator(mode="after")
    def validate_attempt(self) -> _RepairActionReceipt:
        if self.completed_at < self.started_at:
            raise ValueError("completed_at must not precede started_at")
        return self


def validate_production_receipt(receipt: dict[str, Any], *, allow_test_lifecycle: bool = False) -> None:
    """Validate the strict, standalone SDO production receipt contract."""

    if receipt.get("schema_version") == "sdo.production-receipt/v2":
        from benchmarks.sregym.protocol.native_receipt import validate_native_production_receipt

        validate_native_production_receipt(receipt)
        return
    if receipt.get("schema_version") != "sdo.production-receipt/v1":
        raise ProductionReceiptValidationError("invalid production receipt schema_version")
    if receipt.get("pre_cutover") is not False:
        raise ProductionReceiptValidationError("production receipt must exclude pre-cutover evidence")
    if receipt.get("validator_mode") != "kubernetes-job":
        raise ProductionReceiptValidationError("production receipt requires validator_mode=kubernetes-job")
    resolution = receipt_resolution(receipt)
    declared = receipt.get("resolution")
    if declared is not None and declared != resolution:
        raise ProductionReceiptValidationError(
            f"production receipt resolution={declared!r} contradicts its evidence, which shows {resolution!r}"
        )
    mitigated = resolution == SDO_MITIGATED
    required = ["production_job_dispatch", "acknowledged", "cleaned"]
    if mitigated:
        required.insert(1, "completed")
    for field in required:
        if receipt.get(field) is not True:
            raise ProductionReceiptValidationError(f"production receipt requires {field}=true")
    # The first reflection attempt resumes the responder session unless the
    # run opted into a fresh session; then the receipt must say so. Only a
    # credited mitigation is reflected on, so the others carry no such claim.
    reflection_session_mode = receipt.get("reflection_session_mode")
    if reflection_session_mode not in (None, "resume", "fresh"):
        raise ProductionReceiptValidationError("production receipt has an invalid reflection_session_mode")
    if mitigated:
        expected_same_session = reflection_session_mode != "fresh"
        if receipt.get("same_session_reflection") is not expected_same_session:
            raise ProductionReceiptValidationError(
                f"production receipt requires same_session_reflection={str(expected_same_session).lower()} "
                f"for reflection_session_mode={reflection_session_mode or 'resume'}"
            )
    lifecycle_provenance = receipt.get("lifecycle_provenance")
    if lifecycle_provenance is not True and not (allow_test_lifecycle and lifecycle_provenance is False):
        raise ProductionReceiptValidationError("production receipt requires lifecycle_provenance=true")
    if receipt.get("controller_update_required") is True and receipt.get("controller_update_rollout") is not True:
        raise ProductionReceiptValidationError("production receipt requires controller_update_rollout=true")
    if receipt.get("controller_update_required") is True:
        try:
            rollout = _ControllerRolloutRecord.model_validate(receipt.get("controller_update_rollout_record"))
        except ValidationError as exc:
            raise ProductionReceiptValidationError(
                "production receipt requires a valid durable rollout record"
            ) from exc
        if not rollout.success or rollout.returncode != 0:
            raise ProductionReceiptValidationError("production receipt durable rollout record requires returncode=0")
        if rollout.incident_id != receipt.get("incident_id"):
            raise ProductionReceiptValidationError("production receipt rollout incident_id mismatch")
        if rollout.reflection_commit != receipt.get("reflection_commit"):
            raise ProductionReceiptValidationError("production receipt rollout reflection_commit mismatch")
    repair_policy = receipt.get("repair_policy")
    if repair_policy not in ("commit", "recorded-actions"):
        raise ProductionReceiptValidationError("production receipt requires a valid repair_policy")
    proposal_commit = receipt.get("proposal_commit")
    if proposal_commit is not None and (not isinstance(proposal_commit, str) or not proposal_commit):
        raise ProductionReceiptValidationError("production receipt proposal_commit must be a non-empty string")
    try:
        actions = [_RepairActionReceipt.model_validate(action) for action in receipt.get("repair_actions", [])]
    except (TypeError, ValidationError) as exc:
        raise ProductionReceiptValidationError("production receipt requires valid repair_actions") from exc
    action_ids = [action.action_id for action in actions]
    if len(action_ids) != len(set(action_ids)):
        raise ProductionReceiptValidationError("production receipt repair_actions IDs must be unique")
    if mitigated and repair_policy == "commit" and (not isinstance(proposal_commit, str) or not proposal_commit):
        raise ProductionReceiptValidationError("production receipt requires proposal_commit")
    if (
        mitigated
        and repair_policy == "recorded-actions"
        and not proposal_commit
        and (not actions or not any(action.success for action in actions))
    ):
        raise ProductionReceiptValidationError("production receipt requires successful repair_actions")
    # A non-mitigated closure is never reflected on, so its evidence commit is the outcome commit.
    commit_fields = ("outcome_commit", "reflection_commit") if mitigated else ("outcome_commit",)
    for field in commit_fields:
        if not isinstance(receipt.get(field), str) or not receipt[field]:
            raise ProductionReceiptValidationError(f"production receipt requires {field}")
    reflection_commit = receipt.get("reflection_commit")
    if mitigated or reflection_commit:
        if receipt.get("validator_evidence_commit") != reflection_commit:
            raise ProductionReceiptValidationError(
                "production receipt requires validator_evidence_commit=reflection_commit"
            )
    elif receipt.get("validator_evidence_commit") != receipt["outcome_commit"]:
        raise ProductionReceiptValidationError(
            "production receipt requires validator_evidence_commit=outcome_commit when no reflection was recorded"
        )
    detector_clear = _string_keyed_objects(receipt.get("detector_clear"))
    if not detector_clear or any(
        state.get("status") != "clear" or state.get("fingerprints") != [] for state in detector_clear
    ):
        raise ProductionReceiptValidationError("production receipt requires detector_clear with empty fingerprints")
    if receipt.get("detector_review_required_at"):
        raise ProductionReceiptValidationError(
            "production receipt health cleared only after detector review; the responder did not restore it"
        )
    verification = _string_keyed_objects(receipt.get("independent_verification"))
    if not verification or any(evidence.get("passed") is not True for evidence in verification):
        raise ProductionReceiptValidationError("production receipt requires passing independent_verification")
    validator_execution_required = receipt.get("validator_execution_required", True)
    canaries = _string_keyed_objects(receipt.get("validator_network_policy_canaries"))
    if validator_execution_required is False:
        if canaries != [] or receipt.get("validator_skipped_reason") != "unchanged-diagnostics":
            raise ProductionReceiptValidationError(
                "skipped executable validation requires no canaries and unchanged-diagnostics reason"
            )
        canaries = []
    elif validator_execution_required is not True:
        raise ProductionReceiptValidationError("validator_execution_required must be a boolean")
    if validator_execution_required and (canaries is None or len(canaries) != 2):
        raise ProductionReceiptValidationError("production receipt requires validator_network_policy_canaries")
    if any(canary.get("passed") is not True for canary in canaries or []):
        raise ProductionReceiptValidationError("production receipt requires network-policy canaries passed=true")
    if validator_execution_required and {canary.get("mode") for canary in canaries or []} != {"allow", "deny"}:
        raise ProductionReceiptValidationError(
            "production receipt requires exactly one allow and one deny network-policy canary"
        )
    for canary in canaries or []:
        for field in ("job_name", "observed_at", "details"):
            value = canary.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ProductionReceiptValidationError(f"production receipt network-policy canary requires {field}")
    if receipt.get("remaining_worktrees") != []:
        raise ProductionReceiptValidationError("production receipt requires remaining_worktrees=[]")
    responder_jobs = receipt.get("responder_jobs")
    if not isinstance(responder_jobs, list) or len(cast("list[object]", responder_jobs)) != 1:
        raise ProductionReceiptValidationError("production receipt requires exactly one responder Job")
