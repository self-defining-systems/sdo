from __future__ import annotations

import json
from pathlib import Path

import pytest
from agentshim.providers.codex import CodexSandboxConfig, parse_sandbox
from agentshim.testing import FakeRun
from google.protobuf import json_format

from sdo.agents.responder.codex import (
    ResponderExecutionError,
    _incident_result_schema,
    execute_incident,
)
from sdo.agents.responder.codex import (
    _responder_prompt as _build_responder_prompt,
)
from sdo.contracts import (
    IncidentRequest,
    IncidentResult,
)
from tests.proto_messages import evolve
from tests.structured_turns import ScriptedAgent, failure


def _fixture(name: str) -> str:
    root = Path(__file__).resolve().parents[5]
    source = (root / "tests" / "fixtures" / "sdo" / "contracts" / name).read_text(encoding="utf-8")
    if name not in {"incident_request.json", "incident_request_state_changes.json"}:
        return source
    request = json.loads(source)
    request["memory_snapshot"] = {
        "accepted_commit": request["repository_base_commit"],
        "goal_markdown": "Stay healthy.",
        "architecture_markdown": "# Architecture\nSource-grounded system.",
    }
    return json.dumps(request)


def _responder_prompt(request: IncidentRequest) -> str:
    return str(_build_responder_prompt(request))


def _request(name: str = "incident_request.json") -> IncidentRequest:
    return json_format.Parse(_fixture(name), IncidentRequest())


def _result() -> IncidentResult:
    return json_format.Parse(_fixture("incident_result.json"), IncidentResult())


def _reply(result: IncidentResult) -> dict[str, object]:
    return {
        "incident_result": json_format.MessageToDict(result, preserving_proto_field_name=True),
        "reflection": None,
    }


def test_codex_responder_captures_resumable_session_id() -> None:
    request = _request()
    result = evolve(_result(), responder_session_id=None)
    message = {"type": "agent_message", "id": "msg", "text": json.dumps(_reply(result))}
    agent = ScriptedAgent(
        lambda _request: FakeRun(
            stdout=[
                '{"type":"thread.started","thread_id":"session-from-codex"}\n',
                json.dumps({"type": "item.completed", "item": message}) + "\n",
                '{"type":"turn.completed","usage":{"input_tokens":321,"cached_input_tokens":123,'
                '"cache_write_input_tokens":0,"output_tokens":45,"reasoning_output_tokens":12}}\n',
            ]
        )
    )

    completed = execute_incident(request, model="gpt-5.5", executor=agent.executor)

    assert completed.responder_session_id == "session-from-codex"
    assert completed.usage.llm_calls == 1
    assert completed.usage.input_tokens == 321
    assert completed.usage.cached_input_tokens == 123
    assert completed.usage.cache_read_input_tokens == 123
    assert completed.usage.uncached_input_tokens == 198
    assert completed.usage.output_tokens == 45
    assert completed.usage.reasoning_output_tokens == 12
    (argv,) = agent.argvs
    assert argv[argv.index("--model") + 1] == "gpt-5.5"
    assert parse_sandbox(argv) == CodexSandboxConfig(mode="danger-full-access")
    assert agent.requests[0].cwd == str(Path(request.repository_worktree).resolve())


def test_claude_responder_captures_resumable_session_id(tmp_path: Path) -> None:
    request = evolve(_request(), repository_worktree=str(tmp_path))
    result = evolve(_result(), responder_session_id=None)
    event = {
        "type": "result",
        "session_id": "session-from-claude",
        "is_error": False,
        "structured_output": _reply(result),
        "num_turns": 4,
        "total_cost_usd": 0.25,
        "usage": {
            "input_tokens": 500,
            "cache_creation_input_tokens": 100,
            "cache_read_input_tokens": 200,
            "output_tokens": 75,
        },
    }
    init = {"type": "system", "subtype": "init", "session_id": "session-from-claude"}
    agent = ScriptedAgent(lambda _request: FakeRun(stdout=[json.dumps(init) + "\n", json.dumps(event) + "\n"]))

    completed = execute_incident(request, provider="claude", model="haiku", executor=agent.executor)

    assert completed.responder_session_id == "session-from-claude"
    assert completed.usage.llm_calls == 4
    assert completed.usage.input_tokens == 800
    assert completed.usage.cached_input_tokens == 200
    assert completed.usage.cache_read_input_tokens == 200
    assert completed.usage.cache_write_input_tokens == 100
    assert completed.usage.uncached_input_tokens == 500
    assert completed.usage.model_requests == 4
    assert completed.usage.output_tokens == 75
    assert completed.usage.total_cost_usd == 0.25
    (argv,) = agent.argvs
    assert argv[argv.index("--model") + 1] == "haiku"
    assert "--settings" not in argv


def test_responder_cannot_reflect_before_controller_verification(monkeypatch) -> None:
    request = _request()

    prompt = _responder_prompt(request)

    assert "After recovery, reflect into `.sdo`" not in prompt

    assert "SREGym" not in prompt
    assert "benchmarks.sregym" not in prompt

    monkeypatch.setenv(
        "SDO_RESPONDER_EXTRA_INSTRUCTIONS",
        "Use the configured external incident transport, but never treat its response as health evidence.",
    )
    extended_prompt = _responder_prompt(request)
    assert "Use the configured external incident transport" in extended_prompt
    assert "never treat its response as health evidence" in extended_prompt


def test_codex_failure_reports_structured_events_and_stderr() -> None:
    request = _request()

    agent = ScriptedAgent(
        lambda _request: failure(stdout='{"type":"error","message":"schema rejected"}\n', stderr="PATH warning\n")
    )

    with pytest.raises(ResponderExecutionError) as captured:
        execute_incident(request, executor=agent.executor)

    assert "schema rejected" in str(captured.value)
    assert "PATH warning" in str(captured.value)


def test_codex_output_schema_is_strict_and_requires_defaulted_fields() -> None:
    schema = _incident_result_schema()

    assert schema["required"] == list(schema["properties"])
    assert schema["additionalProperties"] is False
    applied = schema["properties"]["applied_playbooks"]["items"]
    assert applied["required"] == ["playbook_id"]
    assert "parameter_bindings" not in applied["properties"]
    assert "responder_session_id" not in schema["properties"]
    assert "usage" not in schema["properties"]
    assert "repair_actions" not in schema["properties"]
    cause = schema["properties"]["confirmed_root_causes"]["items"]
    assert cause["required"] == list(cause["properties"])
    assert cause["additionalProperties"] is False
    evidence = cause["properties"]["evidence"]["items"]
    assert evidence["required"] == list(evidence["properties"])
    assert "static-artifact" not in evidence["properties"]["kind"]["enum"]
    assert set(evidence["properties"]["kind"]["enum"]) == {
        "detector-finding",
        "synthetic-traffic",
        "state-change",
        "live-observation",
    }


INCIDENT_PLAYBOOK = "missing-configmap"


def _scripted_reply(result: IncidentResult, session: str) -> FakeRun:
    message = {"type": "agent_message", "id": "msg", "text": json.dumps(_reply(result))}
    return FakeRun(
        stdout=[
            json.dumps({"type": "thread.started", "thread_id": session}) + "\n",
            json.dumps({"type": "item.completed", "item": message}) + "\n",
            '{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":0,'
            '"cache_write_input_tokens":0,"output_tokens":5,"reasoning_output_tokens":1}}\n',
        ]
    )


def test_responder_checkpoint_binds_worker_task_and_phase(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from agentshim import normalized_usage
    from agentshim.testing import scripted_turn

    directory = tmp_path / "usage"
    monkeypatch.setenv("SDO_RESPONDER_USAGE_DIR", str(directory))
    request = evolve(_request(), repository_worktree=str(tmp_path))
    task_id = request.incident_id + "-continue-1"
    monkeypatch.setenv("SDO_RESPONDER_TASK_ID", task_id)
    agent = ScriptedAgent(
        lambda _: scripted_turn(
            "codex",
            session_id="synthetic-usage-session",
            structured_output=_reply(_result()),
            usage=normalized_usage(input_tokens=120, output_tokens=10, turns=1),
        )
    )
    result = execute_incident(request, provider="codex", executor=agent.executor)
    documents = [json.loads(path.read_text()) for path in directory.glob("*.json")]
    assert len(documents) == 1
    assert documents[0]["incident_id"] == request.incident_id
    assert documents[0]["task_id"] == task_id
    assert documents[0]["phase"] == "responder"
    assert documents[0]["complete"] is True
    assert documents[0]["usage"]["input_tokens"] == result.usage.input_tokens == 120


@pytest.mark.parametrize("provider", ["codex", "claude"])
def test_worker_ignores_malformed_controller_fields(provider: str, tmp_path: Path) -> None:
    from tests.structured_turns import reply

    payload = json_format.MessageToDict(_result(), preserving_proto_field_name=True)
    payload["drift_reprompt"] = {"findings_before": 0, "findings_after": 0}
    payload["unresolvedFindings"] = [{"invalid": True}]
    agent = ScriptedAgent(lambda _: reply(provider, payload, session_id="worker-session", branch="incident_result"))
    result = execute_incident(
        evolve(_request(), repository_worktree=str(tmp_path)), provider=provider, executor=agent.executor
    )
    assert not result.HasField("drift_reprompt")
    assert not result.unresolved_findings
    assert len(agent.requests) == 1


@pytest.mark.parametrize("provider", ["codex", "claude"])
@pytest.mark.parametrize("corrected", [True, False])
def test_worker_schema_violation_gets_one_same_session_correction(
    provider: str,
    corrected: bool,
    tmp_path: Path,
) -> None:
    from tests.structured_turns import reply

    valid = json_format.MessageToDict(_result(), preserving_proto_field_name=True)
    invalid = {**valid, "status": "not-a-status"}
    outputs = iter([invalid, valid if corrected else invalid])
    agent = ScriptedAgent(
        lambda _: reply(provider, next(outputs), session_id="worker-session", branch="incident_result")
    )
    request = evolve(_request(), repository_worktree=str(tmp_path))
    if corrected:
        result = execute_incident(request, provider=provider, executor=agent.executor)
        assert result.status == "completed"
        assert result.usage.llm_calls == 2
    else:
        with pytest.raises(ResponderExecutionError, match="status"):
            execute_incident(request, provider=provider, executor=agent.executor)
    assert len(agent.requests) == 2
    assert "contract validation (IncidentResultValidationError)" in agent.prompts[1]
    assert "worker-session" in agent.argvs[1]


def test_long_result_validation_error_spills_instead_of_crashing_the_worker(monkeypatch, tmp_path) -> None:
    from sdo.agents.prompt_templates import render_prompt

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    diagnostic = "1 validation error for IncidentResult " + "x" * 670
    prompt = render_prompt(
        "responder",
        "result_correction",
        part="session.instructions",
        result_type="ValidationError",
        diagnostic=diagnostic,
    )
    assert "ValidationError" in prompt
    assert prompt.context_files
    assert diagnostic in prompt.context_files[0].read_text()


def test_the_agent_gets_one_incident_turn_whatever_the_controller_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SDO ends the work turn itself, from Go; the worker never prompts or interrupts the session."""
    (tmp_path / "incident-verified.json").write_text("{}")
    monkeypatch.setenv("SDO_API_HISTORY_DIR", str(tmp_path))
    monkeypatch.setenv("SDO_RESPONSE_CONTINUATION", '{"healthy":false,"attempt":1}')
    agent = ScriptedAgent(lambda _request: _scripted_reply(_result(), "session-1"))
    execute_incident(_request(), provider="codex", executor=agent.executor)
    assert len(agent.requests) == 1
    assert not agent.executor.handles[0].terminated
