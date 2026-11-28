"""Go owns the done verdict: the responder's finish is asked, answered and resumed in the same session."""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING

import pytest
from agentshim.testing import FakeRun

from sdo.agents.responder import codex, finish
from sdo.agents.responder.session_recovery import SessionRecovery
from tests.proto_messages import evolve
from tests.structured_turns import ScriptedAgent
from tests.unit.sdo.agents.responder.test_codex_responder import _reply, _request, _result
from tests.unit.sdo.agents.responder.test_session_recovery import controller_session, transcript

if TYPE_CHECKING:
    from pathlib import Path


def _codex_turn(session: str) -> FakeRun:
    message = {
        "type": "agent_message",
        "id": "msg",
        "text": json.dumps(_reply(evolve(_result(), responder_session_id=None))),
    }
    return FakeRun(
        stdout=[
            json.dumps({"type": "thread.started", "thread_id": session}) + "\n",
            json.dumps({"type": "item.completed", "item": message}) + "\n",
            '{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":0,'
            '"cache_write_input_tokens":0,"output_tokens":5,"reasoning_output_tokens":0}}\n',
        ]
    )


def _answers(monkeypatch: pytest.MonkeyPatch, *answers: finish.FinishAnswer) -> list[dict[str, object]]:
    asked: list[dict[str, object]] = []
    pending = list(answers)

    def ask(**request: object) -> finish.FinishAnswer:
        asked.append(request)
        return pending.pop(0) if pending else finish.FinishAnswer("end")

    monkeypatch.setattr(finish, "enabled", lambda: True)
    monkeypatch.setattr(finish, "ask", ask)
    return asked


def test_a_return_before_recovered_resumes_the_same_session_with_gos_message(monkeypatch: pytest.MonkeyPatch) -> None:
    asked = _answers(
        monkeypatch,
        finish.FinishAnswer("wait", wait_seconds=0.0),
        finish.FinishAnswer("resume", "SDO's verdict: not recovered.\n\nNot recovered. Consider why before acting."),
    )
    monkeypatch.setattr(finish.time, "sleep", lambda _seconds: None)
    agent = ScriptedAgent(lambda _request: _codex_turn("session-1"))

    result = codex.execute_incident(_request(), model="gpt-5.5", executor=agent.executor)

    assert len(agent.argvs) == 2, "the session was not resumed"
    assert "session-1" in agent.argvs[1], "the resume did not continue the same provider session"
    assert "Not recovered. Consider why before acting." in str(agent.requests[1].stdin)
    assert [request["in_turn"] for request in asked] == [False, False, False]
    assert all(request["resumable"] for request in asked)
    assert result.responder_session_id == "session-1"
    assert result.usage.llm_calls == 2


def test_a_resume_that_fails_is_reported_unresumable_and_keeps_the_last_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asked = _answers(monkeypatch, finish.FinishAnswer("resume", "SDO's verdict: not recovered."))
    turns = iter(
        [_codex_turn("session-1"), FakeRun(stdout=['{"type":"error","message":"context overflow"}\n'], returncode=1)]
    )
    agent = ScriptedAgent(lambda _request: next(turns))

    result = codex.execute_incident(_request(), model="gpt-5.5", executor=agent.executor)

    assert result.responder_session_id == "session-1"
    assert asked[-1]["resumable"] is False
    assert "resume failed" in str(asked[-1]["reason"])


def test_without_sdo_tools_the_turn_result_stands(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SDO_RESPONDER_SESSION", raising=False)
    monkeypatch.setattr(finish, "ask", lambda **_: pytest.fail("asked without SDO tools"))
    agent = ScriptedAgent(lambda _request: _codex_turn("session-1"))

    codex.execute_incident(_request(), model="gpt-5.5", executor=agent.executor)

    assert len(agent.argvs) == 1


def test_the_claude_turn_carries_the_in_turn_finish_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    (hook,) = finish.claude_hooks()
    assert hook.event == "PreToolUse"
    assert hook.matcher == "StructuredOutput"
    assert list(hook.command)[-2:] == ["-m", "sdo.agents.responder.finish"]


@pytest.mark.parametrize(
    ("answer", "denied"),
    [(finish.FinishAnswer("deny", "SDO's verdict: not recovered."), True), (finish.FinishAnswer("end"), False)],
)
def test_the_hook_refuses_a_finish_only_when_sdo_denies_it(
    monkeypatch: pytest.MonkeyPatch, answer: finish.FinishAnswer, denied: bool
) -> None:
    asked: list[dict[str, object]] = []
    monkeypatch.setattr(finish, "ask", lambda **request: asked.append(request) or answer)
    event = {
        "tool_name": "StructuredOutput",
        "tool_input": {"incident_result": {"status": "completed"}, "reflection": None},
    }
    out = io.StringIO()

    assert finish.hook_main(io.StringIO(json.dumps(event)), out) == 0

    assert asked == [{"in_turn": True, "status": "completed"}]
    if denied:
        decision = json.loads(out.getvalue())["hookSpecificOutput"]
        assert decision["permissionDecision"] == "deny"
        assert decision["permissionDecisionReason"] == answer.message
    else:
        assert out.getvalue() == ""


def test_the_hook_never_judges_the_closing_turns_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(finish, "ask", lambda **_: pytest.fail("judged a reflection answer"))
    event = {"tool_name": "StructuredOutput", "tool_input": {"incident_result": None, "reflection": {}}}
    out = io.StringIO()
    assert finish.hook_main(io.StringIO(json.dumps(event)), out) == 0
    assert out.getvalue() == ""


def test_an_unavailable_controller_ends_the_finish(monkeypatch: pytest.MonkeyPatch) -> None:
    def unavailable(*_args: object) -> dict[str, object]:
        raise OSError("SDO tools are unavailable")

    monkeypatch.setattr("sdo.agents.responder.tools_cli._exchange", unavailable)
    assert finish.ask(in_turn=True, status="completed") == finish.FinishAnswer("end")


def test_a_fresh_session_sets_aside_the_earlier_sessions_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    transcript(tmp_path, monkeypatch, "codex")
    earlier = SessionRecovery(controller_session(), "codex", tmp_path / "scratch", lambda _: None)
    earlier.checkpoint("provider-session")
    session = controller_session()
    session.session_id = "fresh-logical-session"
    fresh = SessionRecovery(session, "codex", tmp_path / "scratch", lambda _: None)

    assert fresh.resume_session_id() is None
    assert (tmp_path / "scratch/provider-checkpoint-logical-session.json").is_file()


def test_a_resume_message_over_its_budget_keeps_the_last_result(monkeypatch: pytest.MonkeyPatch) -> None:
    asked = _answers(monkeypatch, finish.FinishAnswer("resume", "x" * 20_000))
    agent = ScriptedAgent(lambda _request: _codex_turn("session-1"))

    result = codex.execute_incident(_request(), model="gpt-5.5", executor=agent.executor)

    assert len(agent.argvs) == 1
    assert result.responder_session_id == "session-1"
    assert asked[-1]["resumable"] is False
