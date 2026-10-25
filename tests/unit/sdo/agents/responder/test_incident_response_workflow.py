"""The shipped workflow and shortened prompts are loadable without a model call."""

from __future__ import annotations

from pathlib import Path
from string import Template

import pytest
import yaml

from sdo.agents.responder.credentials import SKILLS_ROOT, prepare_claude_home, prepare_codex_home

PROMPTS = Path(__file__).resolve().parents[5] / "sdo/agents/responder/prompts"
WORKFLOW = "sdo-incident-response"


@pytest.mark.parametrize("provider", ["codex", "claude"])
def test_incident_workflow_installs_from_the_shipped_skill_root(tmp_path: Path, provider: str) -> None:
    destination = tmp_path / provider
    if provider == "codex":
        prepare_codex_home(credentials_root=tmp_path / "absent", codex_home=destination, skills=(WORKFLOW,))
    else:
        prepare_claude_home(credentials_root=tmp_path / "absent", claude_home=destination, skills=(WORKFLOW,))

    installed = destination / "skills" / WORKFLOW / "SKILL.md"
    document = installed.read_text(encoding="utf-8")
    assert document == (SKILLS_ROOT / WORKFLOW / "SKILL.md").read_text(encoding="utf-8")
    _, frontmatter, body = document.split("---", 2)
    metadata = yaml.safe_load(frontmatter)
    assert metadata["name"] == WORKFLOW
    assert 0 < len(metadata["description"]) <= 200
    assert len(document) <= 3000
    assert "sdo detect" in body
    assert "sdo verify" in body
    assert "still_broken" in body


@pytest.mark.parametrize(
    ("name", "values", "limit"),
    [
        (
            "incident_response_instructions",
            {
                "incident_id": "inc-1",
                "skills_root": "/opt/sdo/sdo/skills",
                "repair_policy": "strict",
                "branch_instruction": "Return incident_result and leave reflection null.",
            },
            1500,
        ),
        (
            "reflection_learning_instructions",
            {
                "skills_root": "/opt/sdo/sdo/skills",
                "branch_instruction": "Return reflection and leave incident_result null.",
            },
            2000,
        ),
        (
            "reflection_delta",
            {
                "outcome_commit": "accepted-outcome",
                "verdict": "partial",
                "confirmed_fixed": "[]",
                "unseen_receipts": "[]",
                "cause_work": "[]",
                "unassigned_work": "[]",
            },
            4000,
        ),
    ],
)
def test_short_workflow_templates_render_strictly(name: str, values: dict[str, str], limit: int) -> None:
    template = Template((PROMPTS / f"{name}.md").read_text(encoding="utf-8"))
    rendered = template.substitute(values)
    assert "$" not in rendered
    assert len(rendered) <= limit
    for key in values:
        missing = {field: value for field, value in values.items() if field != key}
        with pytest.raises(KeyError, match=key):
            template.substitute(missing)
