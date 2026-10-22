"""The responder turn and its resumed reflection turn share one prompt-cache prefix."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from agentshim.testing import FakeRun, installed_mcp_servers
from google.protobuf import json_format

from sdo.agents.responder import CodexSessionBackend, execute_incident, session_output_schema, unwrap_branch
from sdo.agents.responder.codex import ResponderExecutionError
from sdo.agents.responder.reflection import ClaudeSessionBackend
from sdo.contracts import IncidentRequest, IncidentResult
from tests.proto_messages import evolve
from tests.structured_turns import ScriptedAgent, reply
from tests.unit.sdo.agents.responder.test_codex_responder import _fixture

_REFLECTION = {"summary": "covered", "learning_decision": "no_change", "no_change_reason": "ok", "playbook_ids": []}


def _raw_schema(request: object) -> str:
    argv = list(request.argv)  # type: ignore[attr-defined]
    if "--output-schema" in argv:
        return Path(argv[argv.index("--output-schema") + 1]).read_text(encoding="utf-8")
    return argv[argv.index("--json-schema") + 1]


class _Recorder:
    """A scripted agent that keeps each turn's schema bytes and installed MCP server definitions."""

    def __init__(self, provider: str, workspace: Path, output: dict[str, object], branch: str) -> None:
        self.provider = provider
        self.workspace = workspace
        self.schemas: list[str] = []
        self.servers: list[dict[str, object]] = []
        self._output = output
        self._branch = branch
        self.agent = ScriptedAgent(self._respond)

    def _respond(self, request: object) -> FakeRun:
        self.schemas.append(_raw_schema(request))
        self.servers.append(json.loads(json.dumps(installed_mcp_servers(self.provider, request, self.workspace))))
        return reply(self.provider, self._output, session_id="session-1", branch=self._branch)


@pytest.mark.parametrize(
    ("provider", "backend_type"), [("codex", CodexSessionBackend), ("claude", ClaudeSessionBackend)]
)
def test_responder_and_reflection_turns_send_identical_schema_and_mcp_servers(
    tmp_path: Path, provider: str, backend_type: type[CodexSessionBackend]
) -> None:
    request = evolve(
        json_format.Parse(_fixture("incident_request.json"), IncidentRequest()), repository_worktree=str(tmp_path)
    )
    result = json_format.Parse(_fixture("incident_result.json"), IncidentResult())
    responder = _Recorder(
        provider,
        tmp_path,
        json_format.MessageToDict(evolve(result, responder_session_id=None), preserving_proto_field_name=True),
        "incident_result",
    )
    reflection = _Recorder(provider, tmp_path, _REFLECTION, "reflection")

    execute_incident(request, provider=provider, executor=responder.agent.executor)
    backend_type(executor=reflection.agent.executor).resume(
        session_id="session-1", worktree=tmp_path, prompt="reflect", idempotency_key="reflection:test"
    )

    assert responder.schemas == reflection.schemas
    assert json.loads(responder.schemas[0]) == session_output_schema()
    assert responder.servers == reflection.servers
    assert "sdo_reflection" in responder.servers[0]


def test_session_schema_is_a_strict_wrapper_of_both_turn_results() -> None:
    schema = session_output_schema()

    assert schema["additionalProperties"] is False
    assert schema["required"] == ["incident_result", "reflection"]
    properties = schema["properties"]
    assert isinstance(properties, dict)
    for name, branch in properties.items():
        assert branch["anyOf"][1] == {"type": "null"}, name
        assert branch["anyOf"][0]["additionalProperties"] is False
    # A ``$ref`` inside a branch resolves against the root, so definitions live there.
    assert "SdkGap" in schema["$defs"]
    assert "$defs" not in properties["reflection"]["anyOf"][0]


@pytest.mark.parametrize(
    "output",
    [
        {"incident_result": None, "reflection": None},
        {"incident_result": {"summary": "x"}, "reflection": None},
        {"incident_result": {"summary": "x"}},
        [],
    ],
)
def test_a_null_or_wrong_branch_is_a_validation_error(output: object) -> None:
    with pytest.raises(ValueError, match=r"turn output"):
        unwrap_branch(json.dumps(output), "reflection")


def test_the_responder_rejects_a_reflection_branch(tmp_path: Path) -> None:
    request = evolve(
        json_format.Parse(_fixture("incident_request.json"), IncidentRequest()), repository_worktree=str(tmp_path)
    )
    agent = ScriptedAgent(lambda _request: reply("codex", _REFLECTION, session_id="session-1", branch="reflection"))

    with pytest.raises(ResponderExecutionError, match="no incident_result object; it filled reflection"):
        execute_incident(request, executor=agent.executor)
