"""Required prompt content must survive rendering or fail with a budget diagnostic."""

from __future__ import annotations


import pytest

from sdo.agents.context_output import budget_for
from sdo.agents.prompt_templates import named_prompt


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


def test_optional_content_cannot_displace_required_tail(monkeypatch, tmp_path):
    from sdo.agents.prompt_templates import PromptPart, RenderedPrompt

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    required = "Required incident outcome: failed; repair was not verified."
    prompt = RenderedPrompt(
        (
            PromptPart("reflection.leftovers", "optional evidence " * 1000, required=False),
            PromptPart("reflection.leftovers", required),
        )
    )
    assert required in prompt
    assert len(prompt) <= budget_for("reflection.leftovers")
    assert tuple(part.text for part in prompt.parts if part.required) == (required,)


def test_optional_content_is_ranked_and_omissions_counted(monkeypatch, tmp_path):
    from sdo.agents.prompt_templates import PromptPart, RenderedPrompt

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    lower = "low priority " * 1000
    higher = "current context " * 1000
    prompt = RenderedPrompt(
        (
            PromptPart("reflection.leftovers", lower, required=False),
            PromptPart("reflection.leftovers", higher, required=False, priority=1),
        )
    )
    assert "current context" in prompt
    assert f"omitted: {len(lower)} characters" in prompt
    assert len(prompt) <= budget_for("reflection.leftovers")


def test_wrapping_cannot_downgrade_required_content_or_erase_its_budget():
    required = named_prompt("reflection.instructions", "must review the outcome")
    wrapped = named_prompt("reflection.leftovers", required, required=False)
    assert wrapped.parts == required.parts


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
