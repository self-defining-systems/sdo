"""Once SDO verifies recovery, the responder session ends with one short final turn."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING

import pytest
from agentshim.testing import FakeRun

from sdo.agents.responder import codex
from sdo.agents.responder.codex import execute_incident
from tests.proto_messages import evolve
from tests.structured_turns import ScriptedAgent
from tests.unit.sdo.agents.responder.test_codex_responder import _request, _result, _scripted_reply

if TYPE_CHECKING:
    from pathlib import Path

    from agentshim import CommandRequest

SESSION = "session-verified"


def _exploring_until_stopped(agent: ScriptedAgent, on_start) -> FakeRun:
    """A turn still running tools when it is stopped; it never answers on its own."""
    on_start()
    handle = agent.executor.handles[-1]
    deadline = time.monotonic() + 10
    while not handle.terminated and time.monotonic() < deadline:
        time.sleep(0.01)
    assert handle.terminated, "the running turn was not stopped after verification"
    return FakeRun(stdout=[json.dumps({"type": "thread.started", "thread_id": SESSION}) + "\n"], returncode=-15)


@pytest.fixture
def history(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("SDO_API_HISTORY_DIR", str(tmp_path))
    monkeypatch.setattr(codex, "VERIFIED_SETTLE_SECONDS", 0.0)
    return tmp_path


def _verify(history: Path) -> None:
    (history / "incident-verified.json").write_text(
        json.dumps({"verified_at": "2026-10-28T07:14:17Z", "message": "SDO verified this incident"})
    )


def test_verified_recovery_stops_exploration_and_asks_once_for_the_result(history: Path) -> None:
    prompts: list[str] = []
    agent: ScriptedAgent

    def respond(request: CommandRequest) -> FakeRun:
        prompts.append(request.stdin or "")
        if len(prompts) == 1:
            return _exploring_until_stopped(agent, lambda: _verify(history))
        return _scripted_reply(evolve(_result(), responder_session_id=None), SESSION)

    agent = ScriptedAgent(respond)
    completed = execute_incident(_request(), provider="codex", executor=agent.executor)

    assert len(prompts) == 2
    final = prompts[1]
    assert "verified" in final
    assert "incident_result" in final
    assert len(final) <= 200
    assert SESSION in agent.argvs[1]
    assert completed.responder_session_id == SESSION


def test_unverified_turn_runs_to_its_own_answer(history: Path) -> None:
    agent = ScriptedAgent(lambda _request: _scripted_reply(_result(), SESSION))
    execute_incident(_request(), provider="codex", executor=agent.executor)
    assert len(agent.requests) == 1
    assert not agent.executor.handles[0].terminated


def test_a_turn_that_answers_inside_the_settle_window_is_not_interrupted(
    history: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(codex, "VERIFIED_SETTLE_SECONDS", 60.0)

    def respond(_request: CommandRequest) -> FakeRun:
        _verify(history)
        time.sleep(0.5)
        return _scripted_reply(_result(), SESSION)

    agent = ScriptedAgent(respond)
    execute_incident(_request(), provider="codex", executor=agent.executor)
    assert len(agent.requests) == 1
    assert not agent.executor.handles[0].terminated
