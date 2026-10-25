"""The responder turn and its resumed reflection turn share one prompt-cache prefix."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from agentshim.testing import FakeRun, installed_mcp_servers
from google.protobuf import json_format

from sdo.agents.responder import CodexSessionBackend, execute_incident, session_output_schema, unwrap_branch
from sdo.agents.responder.codex import ResponderExecutionError, _responder_prompt
from sdo.agents.responder.reflection import ClaudeSessionBackend
from sdo.agents.responder.reflection_validation import validation_server
from sdo.agents.responder.session_turn import WrongBranchError
from sdo.contracts import IncidentRequest, IncidentResult
from tests.proto_messages import evolve
from tests.structured_turns import ScriptedAgent, reply
from tests.unit.sdo.agents.responder.test_codex_responder import _fixture

_REFLECTION = {
    "summary": "covered",
    "learning_decision": "no_change",
    "no_change_reason": "ok",
    "proposed_artifact_ids": [],
}


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
        self.servers: list[dict[str, dict[str, object]]] = []
        self.tool_timeouts: list[str | None] = []
        self._output = output
        self._branch = branch
        self.agent = ScriptedAgent(self._respond)

    def _respond(self, request: object) -> FakeRun:
        self.schemas.append(_raw_schema(request))
        self.servers.append(json.loads(json.dumps(installed_mcp_servers(self.provider, request, self.workspace))))
        self.tool_timeouts.append(request.env.get("MCP_TOOL_TIMEOUT"))  # type: ignore[attr-defined]
        return reply(self.provider, self._output, session_id="session-1", branch=self._branch)


def _model_visible(servers: dict[str, dict[str, object]]) -> dict[str, object]:
    """What a server definition shows the model: its name and timeout (the tools are listed separately)."""
    return {name: entry.get("tool_timeout_s") for name, entry in servers.items()}


def _tool_definitions(root: Path) -> list[str]:
    return [tool.model_dump_json() for tool in asyncio.run(validation_server(root).list_tools())]


@pytest.mark.parametrize(
    ("provider", "backend_type"), [("codex", CodexSessionBackend), ("claude", ClaudeSessionBackend)]
)
def test_responder_and_reflection_turns_send_the_same_model_visible_schema_and_mcp_servers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provider: str,
    backend_type: type[CodexSessionBackend],
    prepare_reflection_memory,
) -> None:
    # The turns deliberately run in different roots with different environments: the reflection turn
    # works in a scratch clone and the broker pins SDO_* values per phase. ``--root`` and the server
    # environment are process launch details the model never sees, so only the prompt-cache prefix
    # (schema bytes, server name, tool definitions, tool timeout) must match.
    responder_root, reflection_root = tmp_path / "responder", tmp_path / "reflection"
    responder_root.mkdir()
    reflection_root.mkdir()
    request = evolve(
        json_format.Parse(_fixture("incident_request.json"), IncidentRequest()),
        repository_worktree=str(responder_root),
    )
    result = json_format.Parse(_fixture("incident_result.json"), IncidentResult())
    responder = _Recorder(
        provider,
        responder_root,
        json_format.MessageToDict(evolve(result, responder_session_id=None), preserving_proto_field_name=True),
        "incident_result",
    )
    reflection = _Recorder(provider, reflection_root, _REFLECTION, "reflection")

    monkeypatch.setenv("SDO_PHASE", "responder")
    execute_incident(request, provider=provider, executor=responder.agent.executor)
    prepare_reflection_memory(reflection_root)
    backend_type(executor=reflection.agent.executor).resume(
        session_id="session-1", worktree=reflection_root, prompt="reflect", idempotency_key="reflection:test"
    )

    assert responder.schemas == reflection.schemas
    assert json.loads(responder.schemas[0]) == session_output_schema()
    (responder_servers,), (reflection_servers,) = responder.servers, reflection.servers
    assert set(responder_servers) == {"sdo_reflection"}
    assert _model_visible(responder_servers) == _model_visible(reflection_servers)
    assert responder.tool_timeouts == reflection.tool_timeouts
    assert _tool_definitions(responder_root) == _tool_definitions(reflection_root)
    # The launch details really do differ, so the equality above is not vacuous.
    assert responder_servers != reflection_servers


def test_session_schema_is_a_strict_wrapper_of_both_turn_results() -> None:
    schema = session_output_schema()

    assert schema["additionalProperties"] is False
    assert schema["required"] == ["incident_result", "reflection"]
    properties = schema["properties"]
    assert isinstance(properties, dict)
    for name, branch in properties.items():
        assert branch["anyOf"][1] == {"type": "null"}, name
        assert branch["anyOf"][0]["additionalProperties"] is False
    # Generated schemas are inlined: no reference needs a root definition.
    assert "$ref" not in json.dumps(schema)
    assert "$defs" not in schema


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


@pytest.mark.parametrize("output", [{"incident_result": None, "reflection": None}, {"incident_result": {"a": 1}}])
def test_a_null_or_misplaced_branch_is_a_typed_wrong_branch_error(output: object) -> None:
    with pytest.raises(WrongBranchError):
        unwrap_branch(json.dumps(output), "reflection")


def _responder_request(tmp_path: Path) -> IncidentRequest:
    return evolve(
        json_format.Parse(_fixture("incident_request.json"), IncidentRequest()), repository_worktree=str(tmp_path)
    )


def _result_output() -> dict[str, object]:
    result = json_format.Parse(_fixture("incident_result.json"), IncidentResult())
    return json_format.MessageToDict(evolve(result, responder_session_id=None), preserving_proto_field_name=True)


_WRONG_BRANCH_REPLIES = {
    "reflection": lambda: reply("codex", _REFLECTION, session_id="session-1", branch="reflection"),
    "null": lambda: reply("codex", {"incident_result": None, "reflection": None}, session_id="session-1"),
}


@pytest.mark.parametrize("wrong", _WRONG_BRANCH_REPLIES)
def test_the_responder_resumes_once_when_it_answers_in_the_wrong_branch(tmp_path: Path, wrong: str) -> None:
    replies = [
        _WRONG_BRANCH_REPLIES[wrong](),
        reply("codex", _result_output(), session_id="session-1", branch="incident_result"),
    ]
    agent = ScriptedAgent(lambda _request: replies.pop(0))

    result = execute_incident(_responder_request(tmp_path), executor=agent.executor)

    assert result.incident_id == _responder_request(tmp_path).incident_id
    first, second = agent.argvs
    assert "resume" not in first
    assert "resume" in second
    assert "session-1" in second
    assert "`incident_result`" in agent.prompts[1]
    assert "null" in agent.prompts[1]


def test_the_responder_fails_with_the_typed_error_when_the_correction_is_also_wrong(tmp_path: Path) -> None:
    agent = ScriptedAgent(lambda _request: reply("codex", _REFLECTION, session_id="session-1", branch="reflection"))

    with pytest.raises(ResponderExecutionError, match="no incident_result object; it filled reflection"):
        execute_incident(_responder_request(tmp_path), executor=agent.executor)

    assert len(agent.requests) == 2


@pytest.mark.parametrize(
    ("provider", "backend_type"), [("codex", CodexSessionBackend), ("claude", ClaudeSessionBackend)]
)
def test_reflection_resumes_once_when_it_answers_in_the_wrong_branch(
    tmp_path: Path, provider: str, backend_type: type[CodexSessionBackend], prepare_reflection_memory
) -> None:
    prepare_reflection_memory(tmp_path)
    replies = [
        reply(provider, _result_output(), session_id="session-1", branch="incident_result"),
        reply(provider, _REFLECTION, session_id="session-1", branch="reflection"),
    ]
    agent = ScriptedAgent(lambda _request: replies.pop(0))

    turn = backend_type(executor=agent.executor).resume(
        session_id="session-1", worktree=tmp_path, prompt="reflect", idempotency_key="reflection:test"
    )

    assert turn.learning_decision == "no_change"
    assert len(agent.requests) == 2
    assert "`reflection`" in agent.prompts[1]
    assert "Idempotency key" not in agent.prompts[1]


def test_reflection_raises_the_typed_error_when_the_correction_is_also_wrong(tmp_path: Path) -> None:
    agent = ScriptedAgent(
        lambda _request: reply("codex", _result_output(), session_id="session-1", branch="incident_result")
    )

    with pytest.raises(WrongBranchError, match="no reflection object; it filled incident_result"):
        CodexSessionBackend(executor=agent.executor).resume(
            session_id="session-1", worktree=tmp_path, prompt="reflect", idempotency_key="reflection:test"
        )

    assert len(agent.requests) == 2


def test_the_responder_prompt_names_its_branch() -> None:
    prompt = _responder_prompt(json_format.Parse(_fixture("incident_request.json"), IncidentRequest()))

    assert "in the `incident_result` field and leave `reflection` null" in prompt
