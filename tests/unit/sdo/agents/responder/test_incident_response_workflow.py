"""The shipped skills and shortened prompt are loadable without a model call."""

from __future__ import annotations

from pathlib import Path
from string import Template

import pytest
import yaml

from sdo.agents.responder.credentials import AGENT_SKILLS, SKILLS_ROOT, prepare_claude_home, prepare_codex_home
from tests.skill_sizes import SKILL_BYTE_CAPS

PROMPTS = Path(__file__).resolve().parents[5] / "sdo/agents/responder/prompts"
WORKFLOW = "incident-response"


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
    assert len(document.encode()) <= SKILL_BYTE_CAPS[WORKFLOW]


def test_the_first_message_template_renders_strictly() -> None:
    values = {
        "incident_id": "inc-1",
        "namespace": "ns",
        "symptom": "Failing user tests since 10:00:00 UTC (first failing sample):",
        "recent_changes": "Candidates: `sdo detect`; every change: `sdo changes`; signals over time: `sdo timeline`.",
        "skills_root": "/opt/sdo/sdo/skills",
        "branch_instruction": "Return incident_result and leave reflection null.",
    }
    template = Template((PROMPTS / "incident_prompt.md").read_text(encoding="utf-8"))
    rendered = template.substitute(values)
    assert "$" not in rendered
    assert len(rendered.encode()) <= 520
    for key in values:
        missing = {field: value for field, value in values.items() if field != key}
        with pytest.raises(KeyError, match=key):
            template.substitute(missing)


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
        assert "SKILL.md" not in command.stdin
        return reply(provider, output, session_id="workflow-session", branch="incident_result")

    agent = ScriptedAgent(respond)
    result = execute_incident(request, provider=provider, model="synthetic", executor=agent.executor)
    assert result.responder_session_id
    assert len(agent.requests) == 1


def _responder_instructions() -> str:
    """Everything the responder is told on its incident turn: the first message, both skills and the result field."""

    from sdo.agents.responder.session_turn import session_output_schema

    schema = session_output_schema()
    texts = [
        (PROMPTS / "incident_prompt.md").read_text(encoding="utf-8"),
        (SKILLS_ROOT / WORKFLOW / "SKILL.md").read_text(encoding="utf-8"),
        (SKILLS_ROOT / "sdo-operating" / "SKILL.md").read_text(encoding="utf-8"),
        str(schema["properties"]["incident_result"]["description"]),  # type: ignore[index]
    ]
    return "\n".join(texts).lower()


def test_the_responder_works_the_incident_itself_and_nothing_prescribes_subagents() -> None:
    text = _responder_instructions()

    for forced in ("you are the orchestrator", "do not investigate yourself", "one subagent per explanation"):
        assert forced not in text
    assert "before you act on the lead" not in text
    assert "subagent" not in text
    assert "sub-agent" not in text


def test_nothing_tells_the_responder_to_end_a_turn_to_wait() -> None:
    """The structured-result contract turns every ended turn into a submitted result (an early "failed")."""

    text = _responder_instructions()

    assert "end your turn to wait" not in text
    assert "between turns" not in text
    assert "return only to give up" in text
    assert "use failed when giving up" in text


def test_a_result_submitted_while_background_work_runs_is_told_to_be_interim() -> None:
    """Whatever ends a turn mid-incident (a background command, a monitor, a subagent), the wake-up follows.

    The transport already returns the last result once the CLI is idle (see the background Bash and subagent
    drain tests in ``test_structured``); the agent must not treat the earlier one as final.
    """

    text = _responder_instructions()

    assert "interim" in text
    assert "keep working" in text
    assert "the last result stands" in text
