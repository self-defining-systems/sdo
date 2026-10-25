"""Resumed learning receives controller facts, preserving the responder transcript."""

from __future__ import annotations

from sdo.agents.responder.reflection_delta import reflection_delta_values
from sdo.contracts import (
    ConfirmedRootCause,
    IncidentClosure,
    IncidentResult,
    ObjectRef,
    OutcomeRecord,
    PlanApplication,
    ReflectionCauseWork,
    ReflectionEligibility,
    ReflectionWorkItem,
    RepairActionReceipt,
)


def test_delta_keeps_independent_verdict_and_only_confirmed_fixed_faults() -> None:
    fixed = ObjectRef(kind="Service", namespace="toy", name="fixed")
    broken = ObjectRef(kind="Service", namespace="toy", name="broken")
    outcome = OutcomeRecord(
        incident_id="incident",
        classification="failed",
        confirmed_root_causes=[
            ConfirmedRootCause(summary="fixed port", resources=[fixed]),
            ConfirmedRootCause(summary="unproven port", resources=[broken]),
        ],
        reflection_eligibility=ReflectionEligibility(
            version=1,
            causes=[ReflectionCauseWork(cause_index=0, resources=[fixed])],
            unassigned=[ReflectionWorkItem(reason="evidence_gap", disposition="question", resources=[broken])],
        ),
    )
    before = outcome.SerializeToString()
    delta = reflection_delta_values(outcome, IncidentClosure(), outcome_commit="accepted")

    assert delta["verdict"] == "failed"
    assert delta["outcome_commit"] == "accepted"
    assert [item["cause_index"] for item in delta["confirmed_fixed"]] == [0]
    assert delta["confirmed_fixed"][0]["summary"] == "fixed port"
    assert delta["unassigned_work"][0]["reason"] == "evidence_gap"
    assert "memory_snapshot" not in delta
    assert "history" not in delta
    assert "findings" not in delta
    assert "detector_history" not in delta
    assert outcome.SerializeToString() == before


def test_delta_sends_only_receipts_not_already_in_the_responder_result() -> None:
    seen = RepairActionReceipt(action_id="seen", details="already observed")
    new = RepairActionReceipt(action_id="controller", details="independent controller observation")
    seen_plan = PlanApplication(application_id="seen-plan", applied=True)
    changed_plan = PlanApplication(application_id="seen-plan", applied=True, verification_passed=True)
    calls = IncidentResult()
    seen_call = calls.program_tool_calls.add(command="diagnose", response_json='{"result":"Match"}')
    new_call = calls.program_tool_calls.add(command="verify", response_json='{"healthy":false}')
    closure = IncidentClosure(
        result=IncidentResult(repair_actions=[seen], plan_applications=[seen_plan], program_tool_calls=[seen_call])
    )
    outcome = OutcomeRecord(
        classification="failed",
        repair_actions=[seen, new],
        plan_applications=[changed_plan],
        program_tool_calls=[seen_call, new_call],
    )

    receipts = reflection_delta_values(outcome, closure, outcome_commit="accepted")["unseen_receipts"]

    assert [item["action_id"] for item in receipts["repair_actions"]] == ["controller"]
    assert receipts["repair_actions"][0]["details"] == new.details
    assert receipts["plan_applications"][0]["verification_passed"] is True
    assert [item["command"] for item in receipts["program_tool_calls"]] == ["verify"]


def test_failed_historical_outcome_never_promotes_unproven_causes() -> None:
    outcome = OutcomeRecord(classification="failed", confirmed_root_causes=[ConfirmedRootCause(summary="unproven")])
    assert reflection_delta_values(outcome, IncidentClosure(), outcome_commit="accepted")["confirmed_fixed"] == []


def test_explicit_cause_work_preserves_materialized_questions() -> None:
    question = ReflectionWorkItem(disposition="question", reason="program_refinement", program_id="port")
    work = ReflectionCauseWork(cause_index=2, items=[question])
    delta = reflection_delta_values(
        OutcomeRecord(classification="success"), IncidentClosure(), outcome_commit="accepted", cause_work=[work]
    )
    assert delta["cause_work"][0]["cause_index"] == 2
    assert delta["cause_work"][0]["items"][0]["reason"] == "program_refinement"
