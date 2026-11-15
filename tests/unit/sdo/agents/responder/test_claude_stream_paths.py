"""Claude responder paths on agentshim's stream transport: the incident turn, then the closing turn on its session."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from agentshim.testing import FakeExecutor, FakeRun
from agentshim.testing.claude_stream import ClaudeFramePeer

from libs.agent_cli.structured import run_structured_turn
from sdo.agents.responder.reflection import ClaudeSessionBackend, SessionReflector
from sdo.agents.responder.reflection_worker import run_reflection_task
from sdo.contracts import ReflectionResult
from tests.structured_turns import ScriptedAgent, reply
from tests.unit.sdo.agents.responder.test_closing_turn import (
    INCIDENT,
    RESPONDER_SESSION,
    Publisher,
    _learning_request,
    _passing,
    _task,
)

if TYPE_CHECKING:
    from pathlib import Path

SCHEMA = {"type": "object", "properties": {"answer": {"type": "string"}}, "required": ["answer"]}


def _frames(session: str, *answers: str) -> list[dict[str, object]]:
    """A turn that ends while background subagents run; the CLI's follow-on turn answers, then it is idle."""

    def result(answer: str) -> dict[str, object]:
        output = {"answer": answer}
        return {
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "result": json.dumps(output),
            "structured_output": output,
            "session_id": session,
            "num_turns": 1,
            "duration_ms": 1,
            "total_cost_usd": 0,
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "permission_denials": [],
        }

    init = {"type": "system", "subtype": "init", "session_id": session}
    state = {"type": "system", "subtype": "session_state_changed"}
    frames: list[dict[str, object]] = [{**state, "state": "running"}, init]
    for answer in answers:
        frames += [result(answer), init]
    return [*frames[:-1], {**state, "state": "idle"}]


def test_incident_turn_waits_for_its_subagents_then_the_closing_turn_resumes_the_same_session(
    tmp_path: Path, prepare_reflection_memory
) -> None:
    incident = FakeExecutor(
        FakeRun(), peers=lambda _spawn: ClaudeFramePeer([_frames(RESPONDER_SESSION, "failed: pending", "recovered")])
    )
    turn = run_structured_turn(
        "claude", "respond", output_schema=SCHEMA, cwd=tmp_path, access="danger-full-access", executor=incident
    )
    assert json.loads(turn.output_json) == {"answer": "recovered"}
    assert turn.session_id == RESPONDER_SESSION

    prepare_reflection_memory(tmp_path, INCIDENT)
    closing = ScriptedAgent(
        lambda _: reply("claude", {"note": "restarts it"}, session_id=RESPONDER_SESSION, branch="reflection")
    )
    result = run_reflection_task(
        _task(tmp_path, _learning_request(tmp_path, session=turn.session_id)),
        "result",
        Publisher(),
        reflector=SessionReflector(
            ClaudeSessionBackend(reasoning_effort="medium", executor=closing.executor, memory_checker=_passing)
        ),
    )

    assert result.status == "completed"
    spawn = closing.executor.spawns[-1]
    argv = list(spawn.argv)
    assert f"--resume={RESPONDER_SESSION}" in argv
    assert "-p" not in argv
    assert argv[argv.index("--input-format") + 1] == "stream-json"
    decision = ReflectionResult()
    assert result.response.Unpack(decision)
    assert decision.no_change_reason == "restarts it"


def test_a_claude_session_that_cannot_resume_is_a_typed_failure_not_a_new_session(
    tmp_path: Path, prepare_reflection_memory
) -> None:
    from agentshim.testing import scripted_resume_failure
    from agentshim.testing.claude_stream import ClaudeRecordedPeer

    prepare_reflection_memory(tmp_path, INCIDENT)
    refused = scripted_resume_failure("claude")
    error = {
        "type": "result",
        "subtype": "error_during_execution",
        "is_error": True,
        "num_turns": 0,
        "errors": [f"No conversation found with session ID: {RESPONDER_SESSION}"],
    }
    # The CLI writes its error and exits before it answers ``initialize``.
    executor = FakeExecutor(
        FakeRun(), peers=lambda _spawn: ClaudeRecordedPeer(json.dumps(error) + "\n", "".join(refused.stderr))
    )
    result = run_reflection_task(
        _task(tmp_path, _learning_request(tmp_path)),
        "result",
        Publisher(),
        reflector=SessionReflector(
            ClaudeSessionBackend(reasoning_effort="medium", executor=executor, memory_checker=_passing)
        ),
    )
    assert (result.status, result.failure_reason) == ("failed", "session_unavailable")
