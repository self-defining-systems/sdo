"""Responder guidance for follow-up incidents dispatched for residual findings."""

from __future__ import annotations

import json
from pathlib import Path

from sdo.agents.responder.codex import _responder_prompt
from sdo.agents.views import (
    FollowUpContext,
    IncidentRequest,
)

_FIXTURE = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts" / "incident_request.json"


def _request(**update: object) -> IncidentRequest:
    raw = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    raw["memory_snapshot"] = {"accepted_commit": raw["repository_base_commit"]}
    request = IncidentRequest.model_validate(raw)
    return request.model_copy(update=update) if update else request


def test_prompt_without_follow_up_context_has_no_follow_up_guidance() -> None:
    assert "follow-up" not in _responder_prompt(_request()).lower()


def test_follow_up_prompt_states_residual_scope_and_prior_summary() -> None:
    follow_up = FollowUpContext(
        original_incident_id="inc-1",
        parent_incident_id="inc-2",
        attempt=1,
        max_follow_ups=3,
        prior_responder_summary="Earlier responder repaired the first fault.",
    )

    prompt = _responder_prompt(_request(follow_up=follow_up))

    assert "follow-up 1 of at most 3" in prompt
    assert "Earlier responder repaired the first fault." in prompt
    assert "already-repaired" in prompt
