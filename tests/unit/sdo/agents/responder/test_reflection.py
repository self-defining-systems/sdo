"""Native reflection sessions consume logical memory and return typed learning decisions."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from agentshim.providers.codex import CodexSandboxConfig, parse_sandbox
from google.protobuf import json_format

from sdo.agents.responder.reflection import (
    ClaudeSessionBackend,
    CodexSessionBackend,
    ReflectionTurn,
    SessionReflector,
    reflection_output_schema,
)
from sdo.contracts import (
    IncidentClosure,
    IncidentRequest,
    IncidentResult,
    OperationalMemorySnapshot,
    OutcomeClassification,
    OutcomeRecord,
    datetime_timestamp,
)
from tests.proto_messages import evolve
from tests.structured_turns import ScriptedAgent, reply

if TYPE_CHECKING:
    from agentshim import CommandRequest
    from agentshim.testing import FakeRun


class _CapturingBackend:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def resume(self, *, session_id: str, worktree: Path, prompt: str, idempotency_key: str) -> ReflectionTurn:
        self.calls.append({"mode": "resume", "session_id": session_id, "prompt": prompt, "key": idempotency_key})
        return ReflectionTurn(
            summary="Already covered", learning_decision="no_change", no_change_reason="No gap", proposed_changes=[]
        )

    def fresh(self, *, worktree: Path, prompt: str, idempotency_key: str) -> ReflectionTurn:
        self.calls.append({"mode": "fresh", "prompt": prompt, "key": idempotency_key})
        return ReflectionTurn(
            summary="Already covered", learning_decision="no_change", no_change_reason="No gap", proposed_changes=[]
        )


def _outcome() -> OutcomeRecord:
    return OutcomeRecord(
        schema_version=1,
        incident_id="inc-1",
        source_commit="source",
        deployed_commit="deployed",
        classification="success",
        responder_backend="codex",
        responder_model="gpt-5",
    )


@pytest.mark.parametrize(
    ("classification", "expected"),
    [
        (OutcomeClassification.SUCCESS, True),
        (OutcomeClassification.FALSE_POSITIVE, True),
        (OutcomeClassification.FALSE_NEGATIVE, True),
        (OutcomeClassification.PARTIAL, False),
        (OutcomeClassification.EXTERNAL_RECOVERY, False),
        (OutcomeClassification.CLEARED_WITHOUT_ACTION, False),
        (OutcomeClassification.FAILED, False),
        (OutcomeClassification.CANCELLED, False),
    ],
)
def test_reflection_gate_uses_typed_classification(classification: str, expected: bool) -> None:
    outcome = _outcome()
    outcome.classification = classification
    reflected = SessionReflector(_CapturingBackend()).should_reflect(
        outcome, health_verified=True, session_id="session"
    )
    assert reflected is expected


def test_reflection_gate_rejects_unknown_classification() -> None:
    outcome = _outcome()
    outcome.classification = "sucess"
    with pytest.raises(ValueError, match="classification"):
        SessionReflector(_CapturingBackend()).should_reflect(outcome, health_verified=True, session_id="session")


def _fresh_closure(worktree: Path, *, evidence: str = "Deployment unavailable") -> IncidentClosure:
    root = Path(__file__).resolve().parents[5] / "tests" / "fixtures" / "sdo" / "contracts"
    request = json_format.Parse((root / "incident_request.json").read_text(encoding="utf-8"), IncidentRequest())
    finding = evolve(request.findings[0], evidence=evidence)
    assert request.repository_base_commit
    request = evolve(
        request,
        findings=[finding],
        repository_worktree=str(worktree),
        memory_snapshot=OperationalMemorySnapshot(
            accepted_commit=request.repository_base_commit,
            goal_markdown="The system remains healthy.",
            architecture_markdown="Accepted architecture summary.",
        ),
    )
    result = json_format.Parse((root / "incident_result.json").read_text(encoding="utf-8"), IncidentResult())
    at = datetime_timestamp(datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc))
    return IncidentClosure(
        request=request,
        result=result,
        final_detector_states=result.final_detector_states,
        detected_at=at,
        dispatched_at=at,
        responder_completed_at=at,
        verified_at=at,
    )


def test_codex_reflection_resumes_structured_session_in_incident_worktree(tmp_path: Path) -> None:
    def respond(request: CommandRequest) -> FakeRun:
        return reply(
            "codex",
            {
                "summary": "already covered",
                "learning_decision": "no_change",
                "no_change_reason": "existing detector covers it",
                "proposed_changes": [],
            },
            session_id="session-1",
        )

    agent = ScriptedAgent(respond)
    backend = CodexSessionBackend(
        model="gpt-test", reasoning_effort="high", timeout_seconds=456, executor=agent.executor
    )
    result = backend.resume(
        session_id="session-1",
        worktree=tmp_path,
        prompt="reflect on verified recovery",
        idempotency_key="reflection:incident:commit",
    )
    assert result.learning_decision == "no_change"
    (request,) = agent.requests
    argv = list(request.argv)
    assert argv[1:5] == ["exec", "resume", "session-1", "-"]
    assert parse_sandbox(argv) == CodexSandboxConfig(mode="danger-full-access")
    assert argv[argv.index("--model") + 1] == "gpt-test"
    assert request.cwd == str(tmp_path.resolve())
    assert request.timeout == 456


def test_claude_reflection_resumes_structured_session(tmp_path: Path) -> None:
    agent = ScriptedAgent(
        lambda _request: reply(
            "claude",
            {
                "summary": "already covered",
                "learning_decision": "no_change",
                "no_change_reason": "existing detector covers it",
                "proposed_changes": [],
            },
            session_id="session-1",
        )
    )
    result = ClaudeSessionBackend(model="haiku", executor=agent.executor).resume(
        session_id="session-1", worktree=tmp_path, prompt="reflect", idempotency_key="reflection:incident:commit"
    )
    assert result.learning_decision == "no_change"
    (command,) = agent.argvs
    assert command[command.index("--resume") + 1] == "session-1"


def test_reflection_requires_logical_snapshot(tmp_path: Path) -> None:
    backend = _CapturingBackend()
    with pytest.raises(ValueError, match="logical memory snapshot"):
        SessionReflector(backend).resume(
            session_id="session-1",
            incident_id="inc-1",
            worktree=tmp_path,
            outcome=_outcome(),
            history=[],
            outcome_commit="outcome-sha",
        )
    assert backend.calls == []


def test_reflection_prompt_uses_logical_artifacts_and_native_draft_validation(tmp_path: Path) -> None:
    backend = _CapturingBackend()
    closure = _fresh_closure(tmp_path)
    SessionReflector(backend).resume(
        session_id="session-1",
        incident_id="inc-1",
        worktree=tmp_path,
        outcome=_outcome(),
        history=[],
        outcome_commit="outcome-sha",
        session_mode="fresh",
        closure=closure,
    )
    (call,) = backend.calls
    assert call["mode"] == "fresh"
    prompt = str(call["prompt"])
    assert "OperationalArtifactProposal" in prompt
    assert "sdo-operator memory check" in prompt
    assert "draft-test" not in prompt
    assert "Accepted architecture summary" in prompt
    assert ".sdo/" not in prompt
    assert "sdo.operational_memory" not in prompt
    assert json.dumps({"incident_id": "inc-1"}) not in prompt  # full outcome context is richer


def test_reflection_output_schema_is_strict_and_artifact_typed() -> None:
    schema = reflection_output_schema()
    assert schema["additionalProperties"] is False
    assert "artifact_proposal" in schema["properties"]
    assert schema["required"] == list(schema["properties"])
