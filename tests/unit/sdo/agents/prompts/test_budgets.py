"""Budgets count the actual production rendering, including every separator."""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

import pytest
import yaml

from sdo.agents.prompt_templates import (
    PromptPart,
    RenderedPrompt,
    count_chars,
    join_prompts,
    named_prompt,
    render_prompt,
)
from sdo.agents.responder.continuation import continuation_prompt
from sdo.agents.responder.incident_context import prepared_incident_data
from tests.unit.sdo.agents.prompts.cases import incident, rendered_cases, responder_prompt
from tests.unit.sdo.agents.responder.continuation_cases import ten_receipt_continuation
from tests.unit.sdo.agents.responder.tool_output_cases import rendered_stdout_cases

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[5]


def measured_parts() -> dict[str, int]:
    maxima: dict[str, int] = defaultdict(int)
    prompts = rendered_cases()
    prompts["ten_receipt_continuation"] = continuation_prompt(json.dumps(ten_receipt_continuation()))
    assert len(prompts["ten_receipt_continuation"]) <= 1500
    boundary = incident(5, oversized=True)
    boundary.memory_snapshot.goal_markdown = "目" * 1000
    boundary.memory_snapshot.architecture_markdown = "界" * 2000
    prompts["responder_boundaries"] = responder_prompt(boundary)
    for name, prompt in prompts.items():
        assert isinstance(prompt, RenderedPrompt), name
        assert "".join(part.text for part in prompt.parts) == prompt.final == prompt, name
        totals: dict[str, int] = defaultdict(int)
        for part in prompt.parts:
            totals[part.name] += count_chars(part.text)
        for part, count in totals.items():
            maxima[part] = max(maxima[part], count)
    for skill in sorted((ROOT / "sdo/skills").glob("*/SKILL.md")):
        frontmatter = yaml.safe_load(skill.read_text(encoding="utf-8").split("---", 2)[1])
        description = frontmatter["description"]
        assert isinstance(description, str)
        maxima[f"skills.{skill.parent.name}"] = count_chars(description)
    for name, output in rendered_stdout_cases().items():
        command = name.split(".", 1)[0]
        maxima[f"tool_stdout.{command}"] = max(maxima[f"tool_stdout.{command}"], count_chars(output))
    for request in (incident(3, warm=True), incident(5, oversized=True), incident(100, oversized=True)):
        with prepared_incident_data(request) as summary:
            maxima["tool_stdout.incident"] = max(maxima["tool_stdout.incident"], count_chars(summary))
    maxima["tool_stdout.incident"] = max(maxima["tool_stdout.incident"], maxima["responder.incident_data"])
    return dict(sorted(maxima.items()))


def test_rendered_prompt_budgets() -> None:
    budgets = tomllib.loads((ROOT / "sdo/agents/prompt_budgets.toml").read_text(encoding="utf-8"))
    measured = measured_parts()
    configured = {f"{role}.{part}": entry for role, parts in budgets.items() for part, entry in parts.items()}
    assert set(measured) <= set(configured), "Every shipped part/skill must have an exercised, explicit budget"
    for name, count in measured.items():
        assert count <= configured[name]["max_chars"], f"{name}: {count} > {configured[name]['max_chars']}"
        assert configured[name]["reason"].strip(), name


@pytest.mark.parametrize(("text", "expected"), [("", 0), ("abc", 3), ("é界🙂", 3), ("a\r\nb", 4), ("e\u0301", 2)])
def test_count_unicode_code_points(text: str, expected: int) -> None:
    assert count_chars(text) == expected


def test_strict_template_requires_every_value() -> None:
    with pytest.raises(KeyError, match="incident_id"):
        render_prompt("responder", "incident_response_instructions", part="responder.instructions")


def test_parts_survive_composition() -> None:
    left = named_prompt("responder.instructions", "é界")
    right = named_prompt("responder.incident_data", "\r\n🙂")
    prompt = join_prompts("|", [left, right], part="reflection_brief.separators")
    assert prompt.final == "é界|\r\n🙂"
    assert prompt.parts == (
        PromptPart("responder.instructions", "é界"),
        PromptPart("reflection_brief.separators", "|"),
        PromptPart("responder.incident_data", "\r\n🙂"),
    )
    assert (left + right).parts == (*left.parts, *right.parts)


def test_incident_data_cap_preserves_overflow_instead_of_inline_memory_bodies() -> None:
    request = incident(5, oversized=True)
    with prepared_incident_data(request) as rendered:
        assert len(rendered) <= 12000
        assert '"details_file"' in rendered
        assert "手" * 100 not in rendered
        assert "検" * 100 not in rendered
