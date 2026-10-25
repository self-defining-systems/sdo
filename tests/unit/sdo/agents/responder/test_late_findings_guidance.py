"""Responder guidance for pulling late findings, behind ``--late-findings``."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from google.protobuf import json_format

from sdo.agents.responder.codex import late_findings_guidance
from sdo.contracts import IncidentRequest
from tests.proto_messages import evolve
from tests.unit.sdo.agents.prompts.cases import responder_prompt as _responder_prompt

_FIXTURE = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts" / "incident_request.json"


def _request(**update: object) -> IncidentRequest:
    raw = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    raw["memory_snapshot"] = {"accepted_commit": raw["repository_base_commit"]}
    request = json_format.ParseDict(raw, IncidentRequest())
    return evolve(request, **update) if update else request


def test_prompt_is_unchanged_when_the_flag_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SDO_LATE_FINDINGS", raising=False)
    baseline = _responder_prompt(_request())
    monkeypatch.setenv("SDO_LATE_FINDINGS", "off")

    assert _responder_prompt(_request()) == baseline
    assert "late-findings" not in baseline
    assert late_findings_guidance(_request()) == ""


@pytest.mark.parametrize("policy", ["commit", "recorded-actions"])
def test_pull_mode_adds_the_guidance_in_both_repair_modes(monkeypatch: pytest.MonkeyPatch, policy: str) -> None:
    monkeypatch.delenv("SDO_LATE_FINDINGS", raising=False)
    baseline = _responder_prompt(_request(repair_policy=policy))
    monkeypatch.setenv("SDO_LATE_FINDINGS", "pull")
    request = _request(repair_policy=policy)

    prompt = _responder_prompt(request)

    assert late_findings_guidance(request) in prompt
    assert f"--incident-id {request.incident_id}" in prompt
    assert "sdo-operator late-findings" in prompt
    assert "sdo.operational_memory" not in prompt
    assert prompt.replace(late_findings_guidance(request), "") != ""
    assert len(prompt) > len(baseline)
    assert f"Repair evidence mode: {policy}" in prompt


def test_guidance_says_when_to_pull_and_to_treat_playbooks_as_hypotheses() -> None:
    text = late_findings_guidance(_request(), mode="pull")

    assert "now" in text
    assert "before your first mutation" in text
    assert "hypothes" in text
    assert "Live-check" in text


def test_guidance_is_problem_agnostic() -> None:
    text = late_findings_guidance(_request(), mode="pull")

    for private_fact in ("hotel", "geo-config", "SREGym"):
        assert private_fact not in text


def test_unknown_mode_is_rejected() -> None:
    with pytest.raises(ValueError, match="late"):
        late_findings_guidance(_request(), mode="push")
