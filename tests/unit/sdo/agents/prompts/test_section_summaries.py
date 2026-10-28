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
        (json.dumps({"findings": [{"summary": "s" * 400, "evidence": "e" * 400}] * 50}), 120),
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


def _ref(kind: str, name: str) -> dict[str, str]:
    return {"api_version": "v1", "kind": kind, "namespace": "toy", "name": name}


def _deep_document() -> dict[str, object]:
    """Toy outcome view shaped like a live one: few lists, each item a deep record."""
    plan = {"actions": [{"reconcile_from_source": {"ref": _ref("ConfigMap", "toy-config")}}]}
    return {
        "incident_id": "toy-incident-" + "0" * 60,
        "classification": "success",
        "findings": [
            {"fingerprint": f"drift/toy/ConfigMap/toy-config-{index}/" + "f" * 64, "summary": "toy config drifted"}
            for index in range(10)
        ],
        "repair_actions": [
            {
                "action_id": f"toy-mitigation-{index}-r1",
                "kind": "program-plan",
                "resources": [_ref("ConfigMap", f"toy-config-{index}")],
                "summary": "applied program plan action:ReconcileFromSource",
                "details": "controller confirmed 1 applied resource effects " * 3,
                "success": True,
            }
            for index in range(6)
        ],
        "plan_applications": [
            {
                "plan": plan,
                "applied": True,
                "application_id": f"toy-mitigation-{index}-r1",
                "program_ids": ["action:ReconcileFromSource"],
                "actions_applied": plan["actions"],
                "action_reports": [{"status": "applied", "source_reconcile": {"ref": _ref("ConfigMap", "toy")}}],
            }
            for index in range(3)
        ],
    }


@pytest.mark.parametrize("limit", [1200, 3600])
def test_every_capped_list_keeps_items_before_any_is_counted(monkeypatch, tmp_path, limit: int) -> None:
    """A capped list shows its first items within the budget; only the rest is counted.

    h9.19 reflection rendered every list of its outcome, memory and per-cause
    sections empty with an omitted count, so reflection read every full file.
    """
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    document = _deep_document()
    result = cap_text(json.dumps(document, indent=2), limit)
    assert len(result) <= limit
    assert not result.degraded
    summary = json.loads(result.split("Summary (partial):\n", 1)[1])
    for key in ("findings", "repair_actions", "plan_applications"):
        assert summary[key], key
        assert len(summary[key]) + summary.get(key + "_omitted", 0) == len(document[key])
    assert summary["classification"] == "success"


def test_per_cause_questions_come_first(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    question = "A repaired object in this confirmed cause had no matched program binding. What relation holds?"
    item = {"disposition": "question", "question": question, "reason": "unbound_write"}
    work = {
        "causes": [{"resources": [_ref("ConfigMap", f"toy-{index}")], "items": [item]} for index in range(2)],
        "unassigned": [{**item, "resources": [_ref("Service", "toy")]}],
    }
    result = cap_text(json.dumps(work, indent=2), 700)
    summary = json.loads(result.split("Summary (partial):\n", 1)[1])
    assert summary["causes"][0]["items"][0]["question"] == question
    assert summary["unassigned"]
