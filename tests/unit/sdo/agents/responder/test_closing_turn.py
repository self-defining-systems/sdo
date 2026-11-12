"""The closing turn: one short message into the responder's own session after Recovered.

Go decides whether it runs and what it carries; the worker sends it, the agent replies with one line, and the memory
check runs on the checkout. These tests pin the contract at the worker boundary with a scripted provider.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import TYPE_CHECKING

from google.protobuf import json_format

from sdo.agents.responder.memory_check import ValidationResult
from sdo.agents.responder.reflection import (
    CodexSessionBackend,
    SessionReflector,
    changed_playbooks,
    cleanup_prompt,
    closing_prompt,
)
from sdo.agents.responder.reflection_worker import run_reflection_task
from sdo.contracts import (
    AgentTask,
    IncidentClosure,
    OutcomeRecord,
    ReflectionRequest,
    ReflectionResult,
    to_canonical_json,
)
from tests.context_helpers import expand_context
from tests.structured_turns import ScriptedAgent, failure, reply

if TYPE_CHECKING:
    from collections.abc import Callable

    from agentshim import CommandRequest
    from agentshim.testing import FakeRun

INCIDENT = "incident"
RESPONDER_SESSION = "responder-session-0001"
CHANGES = ["1. 03:04:05 api-exec Pod/db-1", "2. 03:04:06 api-delete Pod/api-1"]


class Publisher:
    def __init__(self) -> None:
        self.payload = ""

    def publish_payload(self, name: str, key: str, payload: str) -> None:
        self.payload = payload


def _closure(session: str = RESPONDER_SESSION) -> IncidentClosure:
    closure = IncidentClosure()
    closure.request.incident_id = INCIDENT
    closure.result.responder_session_id = session
    return closure


def _task(root: Path, request: ReflectionRequest) -> Path:
    base = os.environ["SDO_BASE_COMMIT"]
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="closing-task",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        workspace={"proposal_id": "proposal", "checkout_path": str(root), "base_commit": base},
    )
    task.request.Pack(request)
    path = root.with_suffix(".task.json")
    path.write_text(to_canonical_json(task))
    return path


def _learning_request(root: Path, *, session: str = RESPONDER_SESSION) -> ReflectionRequest:
    base = os.environ["SDO_BASE_COMMIT"]
    outcome_path = root / ".sdo/outcomes" / f"{hashlib.sha256(INCIDENT.encode()).hexdigest()}.json"
    outcome = json_format.Parse(outcome_path.read_text(), OutcomeRecord())
    request = ReflectionRequest(closure=_closure(session), outcome=outcome, outcome_commit=base, changes=CHANGES)
    request.memory_snapshot.accepted_commit = base
    return request


def _note(note: str = "restarts the stale process") -> Callable[[CommandRequest], FakeRun]:
    return lambda _: reply("codex", {"note": note}, session_id=RESPONDER_SESSION, branch="reflection")


def _passing(*_: object) -> ValidationResult:
    return ValidationResult(0, False, "", "")


def _reflector(agent: ScriptedAgent, checker: Callable[..., ValidationResult] = _passing) -> SessionReflector:
    return SessionReflector(
        CodexSessionBackend(reasoning_effort="medium", executor=agent.executor, memory_checker=checker)
    )


def test_the_learning_turn_resumes_the_responder_session_and_carries_the_changes(
    tmp_path: Path, prepare_reflection_memory
) -> None:
    prepare_reflection_memory(tmp_path, INCIDENT)
    agent = ScriptedAgent(_note())
    publisher = Publisher()
    result = run_reflection_task(
        _task(tmp_path, _learning_request(tmp_path)), "result", publisher, reflector=_reflector(agent)
    )
    assert result.status == "completed"
    assert agent.requests, "the closing turn never invoked the agent"
    for request in agent.requests:
        assert list(request.argv)[1:4] == ["exec", "resume", RESPONDER_SESSION]
    (prompt,) = agent.prompts
    for line in CHANGES:
        assert line in prompt
    assert "playbook-writing/SKILL.md" in prompt
    decision = ReflectionResult()
    assert result.response.Unpack(decision)
    # An empty diff is no_change; the agent's line is the reason.
    assert (decision.learning_decision, decision.no_change_reason) == ("no_change", "restarts the stale process")


def test_a_changed_playbook_derives_an_update_naming_exactly_that_playbook(
    tmp_path: Path, prepare_reflection_memory
) -> None:
    prepare_reflection_memory(tmp_path, INCIDENT)

    def write_playbook(_: CommandRequest) -> FakeRun:
        playbook = tmp_path / ".sdo/playbooks/stale-process/diagnose.go"
        playbook.parent.mkdir(parents=True)
        playbook.write_text("package staleprocess\n")
        return reply("codex", {"note": "restarts it"}, session_id=RESPONDER_SESSION, branch="reflection")

    result = run_reflection_task(
        _task(tmp_path, _learning_request(tmp_path)),
        "result",
        Publisher(),
        reflector=_reflector(ScriptedAgent(write_playbook)),
    )
    decision = ReflectionResult()
    assert result.response.Unpack(decision)
    assert (decision.learning_decision, list(decision.proposed_artifact_ids)) == ("updated", ["stale-process"])
    assert changed_playbooks(tmp_path) == ["stale-process"]


def test_the_cleanup_turn_resumes_the_same_session_with_the_leftovers_and_no_outcome(
    tmp_path: Path, prepare_reflection_memory
) -> None:
    prepare_reflection_memory(tmp_path, INCIDENT)
    request = ReflectionRequest(closure=_closure(), cleanup_turn=2, leftovers=["app/main.go", "app/debug.txt"])
    agent = ScriptedAgent(_note("removed the debug file"))
    result = run_reflection_task(_task(tmp_path, request), "result", Publisher(), reflector=_reflector(agent))
    assert result.status == "completed"
    assert list(agent.requests[0].argv)[1:4] == ["exec", "resume", RESPONDER_SESSION]
    (prompt,) = agent.prompts
    assert "app/main.go" in prompt and "app/debug.txt" in prompt and "Cleanup turn 2" in prompt
    decision = ReflectionResult()
    assert result.response.Unpack(decision)
    assert decision.learning_decision == "no_change"


def test_a_missing_session_never_becomes_a_fresh_one(tmp_path: Path, prepare_reflection_memory) -> None:
    prepare_reflection_memory(tmp_path, INCIDENT)
    agent = ScriptedAgent(lambda _request: failure())
    result = run_reflection_task(
        _task(tmp_path, _learning_request(tmp_path, session="")), "result", Publisher(), reflector=_reflector(agent)
    )
    assert (result.status, result.failure_reason) == ("failed", "session_unavailable")
    assert agent.requests == []


def test_a_provider_that_cannot_resume_is_a_typed_failure_not_a_new_session(
    tmp_path: Path, prepare_reflection_memory
) -> None:
    from agentshim.testing import scripted_resume_failure

    prepare_reflection_memory(tmp_path, INCIDENT)
    agent = ScriptedAgent(lambda _request: scripted_resume_failure("codex"))
    result = run_reflection_task(
        _task(tmp_path, _learning_request(tmp_path)), "result", Publisher(), reflector=_reflector(agent)
    )
    assert (result.status, result.failure_reason) == ("failed", "session_unavailable")
    assert len(agent.requests) == 1


def test_a_failing_check_gets_exactly_one_correction_turn(tmp_path: Path, prepare_reflection_memory) -> None:
    prepare_reflection_memory(tmp_path, INCIDENT)
    failing = ValidationResult(1, False, '{"diagnostics": [{"rule": "x.rule", "path": "p", "message": "m"}]}', "")
    agent = ScriptedAgent(_note())
    result = run_reflection_task(
        _task(tmp_path, _learning_request(tmp_path)),
        "result",
        Publisher(),
        reflector=_reflector(agent, checker=lambda *_: failing),
    )
    assert len(agent.requests) == 2, "one turn plus one correction, then the failure is reported"
    assert result.status == "failed"
    assert "x.rule" in expand_context(agent.prompts[1])


def test_the_learning_message_is_about_600_bytes_plus_the_change_list() -> None:
    prompt = expand_context(closing_prompt(incident_id="0123456789abcdef", changes=CHANGES))
    message = prompt.replace("\n".join(CHANGES), "")
    assert len(message.encode()) <= 760, len(message.encode())
    assert "\n".join(CHANGES) in prompt
    cleanup = expand_context(cleanup_prompt(leftovers=["a.go"], turn=1))
    assert len(cleanup.encode()) <= 560, len(cleanup.encode())
