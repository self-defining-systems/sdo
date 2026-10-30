"""Reflection consumes the complete typed responder result, including failed applies."""

import sys
from pathlib import Path

import pytest

from sdo.agents.responder import recorded_actions
from sdo.agents.responder.reflection import SessionReflector
from sdo.agents.responder.reflection_brief import incident_brief
from sdo.contracts import IncidentResult, MemoryDiagnostic
from tests.context_helpers import expand_context
from tests.unit.sdo.agents.prompts.cases import ReflectionCapture, live_shaped_reflection


@pytest.mark.parametrize("mode", ["resume", "fresh"])
@pytest.mark.parametrize("retry", [False, True])
@pytest.mark.parametrize("detail_field", ["application_error", "action_details", "verification_details"])
def test_reflection_variants_render_unbounded_producer_details(tmp_path, monkeypatch, mode, retry, detail_field):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    live = live_shaped_reflection()
    result = IncidentResult(
        schema_version="sdo.dev/v1alpha1",
        incident_id=live.closure.request.incident_id,
        status="completed",
        responder_session_id="toy",
    )
    result.usage.SetInParent()
    result.timing.started_at.FromJsonString("2026-01-01T00:00:00Z")
    result.timing.completed_at.FromJsonString("2026-01-01T00:00:01Z")
    # Copy controller outcome records into the same typed fields the responder publishes.
    result.plan_applications.extend(live.outcome.plan_applications)
    result.repair_actions.extend(live.outcome.repair_actions)
    detail = "Synthetic failed apply/readback evidence. " * 250
    if detail_field == "application_error":
        result.plan_applications[0].error = detail
    elif detail_field == "action_details":
        result.repair_actions.clear()
        recorded_actions.run_and_record(
            [sys.executable, "-c", "print('Synthetic readback evidence. ' * 250)"],
            [{"api_version": "v1", "kind": "ConfigMap", "namespace": "toy", "name": "toy-config"}],
            summary="Synthetic recorded repair",
        )
        recorded_actions.merge_into(result)
        detail = result.repair_actions[0].details
    else:
        result.verification_evidence.add(name="readback", passed=False, details=detail)
    live.closure.result.CopyFrom(result)
    brief = incident_brief(live.closure, worktree=Path("/application"))
    assert brief
    capture = ReflectionCapture()
    SessionReflector(capture).resume(
        session_id="toy",
        incident_id=result.incident_id,
        worktree=Path("/application"),
        outcome=live.outcome,
        history=[],
        outcome_commit="d" * 40,
        closure=live.closure,
        session_mode=mode,
        diagnostics=[MemoryDiagnostic(rule="synthetic", message="review")] if retry else (),
    )
    assert capture.prompt
    # Full evidence remains readable, including the bytes beyond the inline budget.
    assert detail in expand_context(brief)
