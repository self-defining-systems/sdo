"""Required prompt content must survive rendering or fail with a budget diagnostic."""

from __future__ import annotations

import json

import pytest

from sdo.agents.context_output import budget_for
from sdo.agents.prompt_templates import named_prompt, render_prompt


def test_over_budget_instructions_fail_instead_of_becoming_a_summary(monkeypatch, tmp_path):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    limit = budget_for("reflection.instructions")
    with pytest.raises(ValueError, match=rf"reflection.instructions.*required.*{limit + 1}.*{limit}"):
        named_prompt("reflection.instructions", "x" * limit + "!")


def test_aggregate_required_content_fails_before_dropping_later_instructions(monkeypatch, tmp_path):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    limit = budget_for("reflection.instructions")
    first = named_prompt("reflection.instructions", "x" * (limit - 10))
    with pytest.raises(ValueError, match="reflection.instructions.*required"):
        first + named_prompt("reflection.instructions", "The incident outcome must be reviewed.")


def test_wrapping_a_required_prompt_cannot_make_it_droppable(monkeypatch, tmp_path):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    original = named_prompt("reflection.instructions", "x" * (budget_for("reflection.instructions") - 100))
    with pytest.raises(ValueError, match="reflection.instructions.*required"):
        render_prompt("responder", "reflection_fresh", part="reflection.instructions", brief="", request=original)


@pytest.mark.parametrize(
    ("section", "key"), [("reflection.cause_work", "causes"), ("reflection.outcome", "repair_actions")]
)
def test_required_records_are_never_removed_or_shortened(monkeypatch, tmp_path, section, key):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    records = [
        {"question": f"Question {index}: " + "q" * 150, "summary": "repair " * 20}
        for index in range(budget_for(section) // 200 + 10)
    ]
    text = json.dumps({"incident_id": "toy", "classification": "success", key: records})
    with pytest.raises(ValueError, match=section + ".*required"):
        named_prompt(section, text)


def test_optional_content_cannot_displace_required_tail(monkeypatch, tmp_path):
    from sdo.agents.prompt_templates import PromptPart, RenderedPrompt

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    required = "Required incident outcome: failed; repair was not verified."
    prompt = RenderedPrompt(
        (
            PromptPart("reflection.memory", "optional evidence " * 1000, required=False),
            PromptPart("reflection.memory", required),
        )
    )
    assert required in prompt
    assert len(prompt) <= budget_for("reflection.memory")
    assert tuple(part.text for part in prompt.parts if part.required) == (required,)


def test_optional_content_is_ranked_and_omissions_counted(monkeypatch, tmp_path):
    from sdo.agents.prompt_templates import PromptPart, RenderedPrompt

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    lower = "low priority " * 1000
    higher = "current context " * 1000
    prompt = RenderedPrompt(
        (
            PromptPart("reflection.memory", lower, required=False),
            PromptPart("reflection.memory", higher, required=False, priority=1),
        )
    )
    assert "current context" in prompt
    assert f"omitted: {len(lower)} characters" in prompt
    assert len(prompt) <= budget_for("reflection.memory")


def test_wrapping_cannot_downgrade_required_content_or_erase_its_budget():
    required = named_prompt("reflection.instructions", "must review the outcome")
    wrapped = named_prompt("reflection.memory", required, required=False)
    assert wrapped.parts == required.parts


@pytest.mark.parametrize("guidance", ["go", "shell"])
@pytest.mark.parametrize("mode", ["resume", "fresh", "retry", "fresh_retry"])
def test_recorded_scale_questions_and_repairs_reach_the_real_session_prompt(monkeypatch, tmp_path, guidance, mode):
    from pathlib import Path

    from google.protobuf import json_format

    from sdo.agents.responder.reflection import SessionReflector
    from sdo.contracts import MemoryDiagnostic
    from tests.unit.sdo.agents.prompts.cases import ReflectionCapture, recorded_scale_reflection

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    monkeypatch.setenv("SDO_AGENT_GUIDANCE", guidance)
    live = recorded_scale_reflection()
    capture = ReflectionCapture()
    SessionReflector(capture).resume(
        session_id="synthetic-session",
        incident_id=live.outcome.incident_id,
        worktree=Path("/synthetic-application"),
        outcome=live.outcome,
        history=[],
        outcome_commit="d" * 40,
        closure=live.closure,
        cause_work=live.cause_work,
        unassigned_work=live.unassigned_work,
        session_mode="fresh" if mode in {"fresh", "fresh_retry"} else "resume",
        diagnostics=[MemoryDiagnostic(rule="synthetic-rule", message="Synthetic correction " * 10)]
        if mode in {"retry", "fresh_retry"}
        else (),
    )
    prompt = capture.prompt
    # Resumed retries intentionally use the same session's already-present request.
    if mode == "retry":
        assert "Synthetic correction" in prompt
        return
    questions = [item.question for cause in live.cause_work for item in cause.items]
    assert len(questions) == 91
    assert all(question in prompt for question in questions)
    required = next(
        part.text for part in prompt.parts if part.name == "reflection.outcome" and part.text.startswith("{")
    )
    view = json.loads(required)
    assert view["classification"] == live.outcome.classification
    assert view["incident_id"] == live.outcome.incident_id
    assert view["repair_actions"] == [
        {**json_format.MessageToDict(action, preserving_proto_field_name=True), "success": action.success}
        for action in live.outcome.repair_actions
    ]
    assert len(view["repair_actions"]) == 24
    assert len(prompt) <= budget_for("session_context.reflection_first")


def test_every_agent_render_preserves_all_required_parts(monkeypatch, tmp_path):
    from sdo.agents.prompt_templates import RenderedPrompt
    from tests.unit.sdo.agents.prompts.cases import rendered_cases

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    original = RenderedPrompt.__new__
    renders = 0

    def checked(cls, parts):
        nonlocal renders
        parts = tuple(parts)
        result = original(cls, parts)
        before = tuple(part for part in parts if part.required)
        after = tuple(part for part in result.parts if part.required)
        assert before == after, "Required prompt parts were changed or removed"
        renders += 1
        return result

    monkeypatch.setattr(RenderedPrompt, "__new__", staticmethod(checked))
    for scale in (1, 20):
        rendered_cases(scale=scale)
    assert renders > 0
