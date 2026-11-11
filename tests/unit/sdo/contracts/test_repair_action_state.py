"""A repair-action receipt reads FAILED only with a failure_reason; older records fall back to success."""

from __future__ import annotations

import pytest

from sdo.contracts import PlanApplication, RepairActionReceipt, repair_action_state


def _clean(application_id: str = "plan-s-1", **fields: object) -> PlanApplication:
    application = PlanApplication(application_id=application_id, applied=True)
    for name, value in fields.items():
        if name == "rollout_pending":
            application.rollout_pending.SetInParent()
        else:
            setattr(application, name, value)
    return application


_UNVERIFIED = RepairActionReceipt(action_id="sdo-program-plan-s-1", kind="program-plan")


@pytest.mark.parametrize(
    ("action", "applications", "state"),
    [
        (RepairActionReceipt(action_id="x", kind="program-plan", failure_reason="rolled_back"), [], "failed"),
        (RepairActionReceipt(action_id="x", kind="mitigation-menu", success=True), [], "verified"),
        (_UNVERIFIED, [_clean()], "applied"),
        (RepairActionReceipt(action_id="sdo-program-plan-s-1-2", kind="program-plan"), [_clean()], "applied"),
        (RepairActionReceipt(action_id="sdo-program-plan-s-1-script-2", kind="run-script"), [_clean()], "applied"),
        (
            RepairActionReceipt(action_id="sdo-program-plan-s-1-2", kind="program-plan"),
            [_clean(), _clean("plan-s-1-2", rolled_back=True)],
            "failed",
        ),
        (_UNVERIFIED, [_clean(rolled_back=True)], "failed"),
        (_UNVERIFIED, [_clean(rollback_error="refused")], "failed"),
        (_UNVERIFIED, [_clean(error="lost")], "failed"),
        (_UNVERIFIED, [_clean(rollout_pending=True)], "failed"),
        (_UNVERIFIED, [_clean(verification_completed=True)], "failed"),
        (_UNVERIFIED, [_clean(settlement_rejection_reason=1)], "failed"),
        (_UNVERIFIED, [_clean(applied=False)], "failed"),
        (_UNVERIFIED, [], "failed"),
        (RepairActionReceipt(action_id="plan-s-1-a1-r1-1", kind="mitigation-menu"), [_clean()], "failed"),
    ],
    ids=[
        "reason",
        "success",
        "applied-not-verified",
        "deduplicated-id",
        "run-script",
        "longest-application-id",
        "older-rollback",
        "older-rollback-error",
        "older-application-error",
        "older-rollout-pending",
        "older-verification-ran",
        "older-settlement-refusal",
        "older-not-applied",
        "no-application",
        "older-menu-rollback",
    ],
)
def test_repair_action_state(action: RepairActionReceipt, applications: list[PlanApplication], state: str) -> None:
    assert repair_action_state(action, applications) == state


def test_reflection_brief_shows_failed_only_with_a_reason() -> None:
    from sdo.agents.responder.reflection_brief import _result_section
    from sdo.contracts import IncidentResult

    result = IncidentResult(incident_id="i", status="completed")
    result.plan_applications.append(_clean())
    result.repair_actions.append(
        RepairActionReceipt(action_id="sdo-program-plan-s-1", kind="program-plan", target="Deployment/api", summary="a")
    )
    result.repair_actions.append(
        RepairActionReceipt(
            action_id="sdo-program-plan-s-2",
            kind="program-plan",
            target="Deployment/db",
            summary="b",
            failure_reason="rolled_back",
        )
    )
    lines = [line for line in _result_section(result, "").splitlines() if "Deployment/" in line]
    assert len(lines) == 2
    assert "applied" in lines[0]
    assert "FAILED" not in lines[0]
    assert "FAILED" in lines[1]
