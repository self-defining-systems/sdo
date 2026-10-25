"""The shipped workflow and shortened prompts are loadable without a model call."""

from __future__ import annotations

from pathlib import Path
from string import Template

import pytest
import yaml

from sdo.agents.responder.credentials import AGENT_SKILLS, SKILLS_ROOT, prepare_claude_home, prepare_codex_home

PROMPTS = Path(__file__).resolve().parents[5] / "sdo/agents/responder/prompts"
WORKFLOW = "sdo-incident-response"


@pytest.mark.parametrize("provider", ["codex", "claude"])
def test_incident_workflow_installs_from_the_shipped_skill_root(tmp_path: Path, provider: str) -> None:
    destination = tmp_path / provider
    if provider == "codex":
        prepare_codex_home(
            credentials_root=tmp_path / "absent", codex_home=destination, skills=AGENT_SKILLS["responder"]
        )
    else:
        prepare_claude_home(
            credentials_root=tmp_path / "absent", claude_home=destination, skills=AGENT_SKILLS["responder"]
        )

    assert {path.name for path in (destination / "skills").iterdir()} == set(AGENT_SKILLS["responder"])
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


def test_responder_role_installs_and_lists_the_workflow() -> None:
    from sdo.agents.responder.codex import _responder_prompt
    from sdo.contracts import IncidentRequest, OperationalMemorySnapshot

    assert WORKFLOW in AGENT_SKILLS["responder"]
    request = IncidentRequest(
        incident_id="incident", memory_snapshot=OperationalMemorySnapshot(accepted_commit="accepted")
    )
    from sdo.agents.responder.incident_context import prepared_incident_data

    with prepared_incident_data(request) as data:
        prompt = _responder_prompt(request, incident_data=data)
    assert str(SKILLS_ROOT / WORKFLOW / "SKILL.md") in prompt
    assert "playbook_coverage" in prompt
    assert len(prompt.split("Incident request:", 1)[0]) <= 1500


def test_optional_pull_guidance_fits_with_the_actual_skill_root(monkeypatch) -> None:
    from sdo.agents.responder.codex import _responder_prompt
    from sdo.agents.responder.incident_context import prepared_incident_data
    from sdo.contracts import IncidentRequest, OperationalMemorySnapshot

    monkeypatch.setenv("SDO_LATE_FINDINGS", "pull")
    request = IncidentRequest(
        incident_id="incident-with-a-normal-identifier",
        repair_policy="recorded-actions",
        memory_snapshot=OperationalMemorySnapshot(accepted_commit="accepted"),
    )
    with prepared_incident_data(request) as data:
        prompt = _responder_prompt(request, incident_data=data)
    assert len(prompt.split("Incident request:", 1)[0]) <= 1500


@pytest.mark.parametrize("provider", ["codex", "claude"])
def test_workflow_is_visible_in_the_real_provider_turn(tmp_path: Path, monkeypatch, provider: str) -> None:
    import json

    from google.protobuf import json_format

    from sdo.agents.responder.codex import execute_incident
    from sdo.contracts import IncidentRequest
    from tests.structured_turns import ScriptedAgent, reply

    destination = tmp_path / provider
    variable = "CODEX_HOME" if provider == "codex" else "CLAUDE_CONFIG_DIR"
    monkeypatch.setenv(variable, str(destination))
    if provider == "codex":
        prepare_codex_home(
            credentials_root=tmp_path / "absent", codex_home=destination, skills=AGENT_SKILLS["responder"]
        )
    else:
        prepare_claude_home(
            credentials_root=tmp_path / "absent", claude_home=destination, skills=AGENT_SKILLS["responder"]
        )
    fixtures = PROMPTS.parents[3] / "tests/fixtures/sdo/contracts"
    request = json_format.Parse((fixtures / "incident_request.json").read_text(encoding="utf-8"), IncidentRequest())
    request.memory_snapshot.accepted_commit = request.repository_base_commit
    request.repository_worktree = str(tmp_path)
    output = json.loads((fixtures / "incident_result.json").read_text(encoding="utf-8"))

    def respond(command):
        installed = Path(command.env[variable]) / "skills" / WORKFLOW / "SKILL.md"
        assert installed.read_text(encoding="utf-8") == (SKILLS_ROOT / WORKFLOW / "SKILL.md").read_text(
            encoding="utf-8"
        )
        assert str(SKILLS_ROOT / WORKFLOW / "SKILL.md") in command.stdin
        return reply(provider, output, session_id="workflow-session", branch="incident_result")

    agent = ScriptedAgent(respond)
    result = execute_incident(request, provider=provider, model="synthetic", executor=agent.executor)
    assert result.responder_session_id
    assert len(agent.requests) == 1
