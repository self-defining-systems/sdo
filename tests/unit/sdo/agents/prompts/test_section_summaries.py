"""Ratchet: a spilled prompt section never leaves a placeholder in the prompt."""

from __future__ import annotations

import json

import pytest

from sdo.agents import prompt_templates
from sdo.agents.context_output import cap_text
from tests.unit.sdo.agents.prompts.cases import rendered_cases

# Live incidents carried up to ~50 open findings with ~150-character drift fingerprints.
REAL_SCALE = 20


def test_no_registered_section_degrades_on_real_scale_fixtures(monkeypatch, tmp_path) -> None:
    """Sections that overflow spill to a file and keep a real summary inline.

    h9.18 shipped a responder first message whose findings were a field skeleton
    (17 of 485 findings visible); any section rendered that way fails here.
    """
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    capped = prompt_templates.cap_section
    degraded: dict[str, str] = {}

    def record(name: str, text: str) -> str:
        result = capped(name, text)
        if getattr(result, "degraded", False):
            degraded[name] = result[:300]
        return result

    monkeypatch.setattr(prompt_templates, "cap_section", record)
    for scale in (1, REAL_SCALE):
        rendered_cases(scale=scale)
    assert not degraded, json.dumps(degraded, ensure_ascii=False, indent=2)


@pytest.mark.parametrize(
    ("text", "limit"),
    [
        (json.dumps({"findings": [{"summary": "s" * 400, "evidence": "e" * 400}] * 50}), 300),
        ("instructions " * 60, 120),
    ],
    ids=["field-skeleton", "no-room-for-content"],
)
def test_placeholder_summaries_are_flagged(monkeypatch, tmp_path, text: str, limit: int) -> None:
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    assert cap_text(text, limit).degraded


def test_real_summaries_are_not_flagged(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    document = json.dumps({"findings": [{"summary": "route unavailable", "evidence": "e" * 400}] * 50})
    assert not cap_text(document, 2000).degraded
    assert not cap_text("Findings:\n" + "- route unavailable\n" * 400, 2000).degraded
    # Prose with one small embedded JSON document keeps the prose, not the fragment.
    mixed = cap_text("Findings:\n" + "- route unavailable\n" * 400 + '{"changes": []}\n', 2000)
    assert "- route unavailable" in mixed
    assert not mixed.degraded
