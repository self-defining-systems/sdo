"""Dispatch-relative wall clocks shared by live rows and saved-evidence analysis."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path


def timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else None
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.year > 1 else None


@dataclass
class DispatchTiming:
    dispatched_at: datetime | None = None
    first_mitigation_applied_at: datetime | None = None
    time_to_fix_seconds: float | None = None
    time_to_resolution_from_dispatch_seconds: float | None = None
    injection_to_dispatch_seconds: float | None = None
    timing_missing_reasons: dict[str, str] = field(default_factory=dict)

    def json_fields(self) -> dict[str, Any]:
        result = asdict(self)
        for key in ("dispatched_at", "first_mitigation_applied_at"):
            value = result[key]
            result[key] = value.isoformat() if value else None
        return result


def saved_lifecycle(artifacts: Path, incident_id: str | None) -> list[dict[str, Any]]:
    events = []
    for suffix in (".1", ""):
        path = artifacts / "sdo_runtime" / "telemetry" / f"lifecycle-events.jsonl{suffix}"
        if path.is_file():
            for line in path.read_text().splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue  # A torn telemetry tail supplies no timing evidence.
                if isinstance(event, dict) and event.get("incident_id") == incident_id:
                    events.append(event)
    return events


def recover_dispatch_timing(row: dict[str, Any], receipt: dict[str, Any]) -> DispatchTiming:
    """Never infer dispatch from detection, nor first repair from last repair/closure."""
    if receipt and receipt.get("incident_id") != row.get("incident_id"):
        raise ValueError("saved receipt does not belong to row incident")
    ledger = receipt.get("native_ledger") or {}
    if ledger.get("incident_id") and ledger["incident_id"] != row.get("incident_id"):
        raise ValueError("saved ledger does not belong to row incident")
    document = ledger.get("outcome_document") or {}
    closure = ledger.get("closure") or {}
    times = document.get("timestamps") or {}
    # Historical native dispatched_at and responder_started_at copied detected_at.
    # Reject those aliases; the lifecycle event records the actual worker claim.
    detected = timestamp(times.get("detected_at") or closure.get("detected_at") or row.get("detected_at"))
    dispatches = [
        row.get("dispatched_at"),
        times.get("dispatched_at"),
        closure.get("dispatched_at"),
        row.get("responder_started_at"),
        receipt.get("responder_started_at"),
    ]
    dispatches = [value for value in dispatches if timestamp(value) != detected]
    events = receipt.get("lifecycle_events") or []
    dispatches.extend(
        event.get("recorded_at")
        for event in events
        if event.get("incident_id") == row.get("incident_id")
        and event.get("event") in ("responder_dispatched", "deterministic_stage_started")
    )
    stage_start_missing = False
    stages = document.get("prior_stages") or (closure.get("request") or {}).get("prior_stages") or []
    for stage in stages:
        if stage.get("stage") == "deterministic":
            start = timestamp(stage.get("started_at")) or timestamp((stage.get("timing") or {}).get("started_at"))
            dispatches.append(start)
            stage_start_missing = stage_start_missing or (
                start is None
                and not any(
                    event.get("event") == "deterministic_stage_started"
                    and event.get("incident_id") == row.get("incident_id")
                    for event in events
                )
            )
    dispatch = min((at for value in dispatches if (at := timestamp(value)) is not None), default=None)
    if stage_start_missing and not row.get("dispatched_at"):
        dispatch = None
    repairs = [row.get("first_mitigation_applied_at")]
    for owner in (receipt, document, closure.get("result") or {}):
        repairs.extend(
            action.get("completed_at")
            for action in owner.get("repair_actions") or []
            if action.get("success") is True and not action.get("rolled_back")
        )
    # A partial deterministic stage can hand off before settlement. Its repair
    # summary may remain unsuccessful even after the authoritative application
    # is independently settled. Read the final application, never infer success
    # from an applied write or from health/oracle clearance alone.
    applications = list((closure.get("result") or {}).get("plan_applications") or [])
    repairs.extend(
        application.get("completed_at")
        for application in applications
        if application.get("applied") is True
        and application.get("verification_completed") is True
        and application.get("verification_passed") is True
        and application.get("final_equivalence_verified") is True
        and not any(
            application.get(key)
            for key in ("rolled_back", "rollback_error", "error", "rollout_pending", "pending_action", "rolling_back")
        )
        and application.get("settlement_rejection_reason") in (None, "", "SETTLEMENT_REJECTION_REASON_UNSPECIFIED", 0)
    )
    first = min((at for value in repairs if (at := timestamp(value)) is not None), default=None)
    resolution = timestamp(row.get("resolved_at")) or timestamp(times.get("verified_at"))
    if row.get("ablation_arm") == "playbooks-only":
        resolution = timestamp(row.get("responder_finished_at")) or timestamp(closure.get("responder_completed_at"))
    resolution = resolution or timestamp(closure.get("verified_at"))
    injection = row.get("injection") or {}
    injected = timestamp(injection.get("started_at")) if injection.get("state") == "injected" else None
    result = DispatchTiming(dispatched_at=dispatch, first_mitigation_applied_at=first)
    if dispatch is None:
        result.timing_missing_reasons["dispatched_at"] = (
            "deterministic stage start not recorded"
            if stage_start_missing
            else "no recorded stage start or responder dispatch"
        )
    for key, start, end, missing in (
        ("time_to_fix_seconds", dispatch, first, "no successful repair completion"),
        (
            "time_to_resolution_from_dispatch_seconds",
            dispatch,
            resolution,
            "no verified closure or responder completion",
        ),
        ("injection_to_dispatch_seconds", injected, dispatch, "no injection start"),
    ):
        if dispatch is None:
            result.timing_missing_reasons[key] = "no recorded dispatch"
        elif start is None or end is None:
            result.timing_missing_reasons[key] = missing
        elif end < start:
            result.timing_missing_reasons[key] = (
                "endpoint precedes dispatch" if start == dispatch else "dispatch precedes injection"
            )
        else:
            setattr(result, key, (end - start).total_seconds())
    return result
