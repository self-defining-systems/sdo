"""Native reflection sessions consume logical memory and return typed learning decisions."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from agentshim.providers.codex import CodexSandboxConfig, parse_sandbox
from agentshim.testing import scripted_resume_failure
from google.protobuf import json_format

from sdo.agents.responder.reflection import (
    ClaudeSessionBackend,
    CodexSessionBackend,
    ReflectionTurn,
    ResponderSessionUnavailable,
    SessionReflector,
    reflection_output_schema,
)
from sdo.contracts import (
    IncidentClosure,
    IncidentRequest,
    IncidentResult,
    MemoryDiagnostic,
    OperationalMemorySnapshot,
    OutcomeRecord,
    StateChanges,
    datetime_timestamp,
)
from tests.proto_messages import evolve, from_dict
from tests.structured_turns import ScriptedAgent, failure, reply

if TYPE_CHECKING:
    from agentshim import CommandRequest
    from agentshim.testing import FakeRun


class _CapturingBackend:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def resume(self, *, session_id: str, worktree: Path, prompt: str, idempotency_key: str) -> ReflectionTurn:
        self.calls.append({"mode": "resume", "session_id": session_id, "prompt": prompt, "key": idempotency_key})
        return ReflectionTurn(
            summary="Already covered", learning_decision="no_change", no_change_reason="No gap", playbook_ids=[]
        )

    def fresh(self, *, worktree: Path, prompt: str, idempotency_key: str) -> ReflectionTurn:
        self.calls.append({"mode": "fresh", "prompt": prompt, "key": idempotency_key})
        return ReflectionTurn(
            summary="Already covered", learning_decision="no_change", no_change_reason="No gap", playbook_ids=[]
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
                "proposed_artifact_ids": [],
            },
            session_id="session-1",
            branch="reflection",
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
                "proposed_artifact_ids": [],
            },
            session_id="session-1",
            branch="reflection",
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


def test_reflection_prompt_requires_direct_edits_and_check_until_green(tmp_path: Path) -> None:
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
    assert "Accepted source and developer changes express intent." in prompt
    assert "Operational memory is fallible advice." in prompt
    assert "Recovery requires evidence from the intended application behavior." in prompt
    assert "artifact_proposal" not in prompt
    assert "broker independently validates" in prompt
    assert "draft-test" not in prompt
    assert "sdo-memory/SKILL.md" in prompt
    assert "DetectionContext exposes" not in prompt  # the SDK reference lives in the skill, not the prompt
    assert "Accepted architecture summary" in prompt
    assert "Edit final operational-memory files only" in prompt
    assert "in the `reflection` field and leave `incident_result` null" in prompt
    assert "sdo.operational_memory" not in prompt
    assert json.dumps({"incident_id": "inc-1"}) not in prompt


def test_reflection_retry_prompt_carries_typed_broker_diagnostics(tmp_path: Path) -> None:
    backend = _CapturingBackend()
    SessionReflector(backend).resume(
        session_id="session-1",
        incident_id="inc-1",
        worktree=tmp_path,
        outcome=_outcome(),
        history=[],
        outcome_commit="outcome-sha",
        session_mode="resume",
        closure=_fresh_closure(tmp_path),
        validation_attempt=2,
        diagnostics=[MemoryDiagnostic(rule="detector.manifest", path="m.yaml", line=4, message="bad id", hint="kebab")],
    )
    (call,) = backend.calls
    assert call["mode"] == "resume"
    assert "[detector.manifest] m.yaml:4: bad id (hint: kebab)" in str(call["prompt"])


def test_reflection_first_attempt_prompt_has_no_rejection_text(tmp_path: Path) -> None:
    backend = _CapturingBackend()
    SessionReflector(backend).resume(
        session_id="session-1",
        incident_id="inc-1",
        worktree=tmp_path,
        outcome=_outcome(),
        history=[],
        outcome_commit="outcome-sha",
        session_mode="fresh",
        closure=_fresh_closure(tmp_path),
    )
    assert "rejected" not in str(backend.calls[0]["prompt"]).lower().split("reflection request:")[0]


def test_reflection_output_schema_is_strict_and_carries_only_decision_and_ids() -> None:
    schema = reflection_output_schema()
    assert schema["additionalProperties"] is False
    assert "artifact_proposal" not in schema["properties"]
    assert "proposed_changes" not in schema["properties"]
    assert "proposed_artifact_ids" in schema["properties"]
    assert schema["required"] == list(schema["properties"])


def test_broker_diagnostic_correction_keeps_the_incident_session(tmp_path: Path) -> None:
    backend = _CapturingBackend()
    SessionReflector(backend).resume(
        session_id="prior-session",
        incident_id="inc-1",
        worktree=tmp_path,
        outcome=_outcome(),
        history=[],
        outcome_commit="outcome-sha",
        closure=_fresh_closure(tmp_path),
        validation_attempt=2,
        diagnostics=[MemoryDiagnostic(rule="document.parse", path="README.md", line=1, message="missing front matter")],
    )
    (call,) = backend.calls
    assert call["mode"] == "resume"
    assert "document.parse" in str(call["prompt"])
    assert "Author corrected final files" in str(call["prompt"])
    assert "artifact_proposal" not in str(call["prompt"])


def test_reflection_prompt_routes_to_one_shipped_guide_without_protocol_recipes(tmp_path: Path) -> None:
    from sdo.agents.responder.credentials import SKILLS_ROOT

    backend = _CapturingBackend()
    SessionReflector(backend).resume(
        session_id="session-1",
        incident_id="inc-1",
        worktree=tmp_path,
        outcome=_outcome(),
        history=[],
        outcome_commit="outcome-sha",
        closure=_fresh_closure(tmp_path),
    )
    prompt = str(backend.calls[0]["prompt"])
    assert str(SKILLS_ROOT / "sdo-memory/SKILL.md") in prompt
    for tool in ("authoring_reference", "create_corpus", "check_memory"):
        assert tool not in prompt
    assert "--" not in prompt
    assert "open_at" not in prompt
    assert "open@settle" not in prompt
    assert "open@pre-apply" not in prompt
    assert "Unknown" in prompt


def test_fresh_reflection_brief_keeps_observed_state_for_corpus(tmp_path: Path) -> None:
    from sdo.agents.responder.reflection_brief import incident_brief

    closure = _fresh_closure(tmp_path)
    observed = datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc)
    closure.request.state_changes.CopyFrom(
        from_dict(
            StateChanges,
            {
                "baseline_at": observed,
                "observed_at": observed,
                "changes": [
                    {
                        "kind": "Service",
                        "name": "api",
                        "change": "modified",
                        "fields": [{"field": "targetPort", "after": "9999"}],
                    }
                ],
            },
        )
    )
    closure.final_state_changes.CopyFrom(
        from_dict(StateChanges, {"baseline_at": observed, "observed_at": observed, "changes": []})
    )
    brief = incident_brief(closure, worktree=tmp_path)
    assert "State changes at dispatch" in brief
    assert "targetPort" in brief
    assert "State changes at verification" in brief


def test_fresh_reflection_brief_summarizes_program_diagnosis_and_application(tmp_path: Path) -> None:
    from sdo.agents.responder.reflection_brief import incident_brief

    closure = _fresh_closure(tmp_path)
    assert closure.result is not None
    payload = json_format.MessageToDict(closure.result, preserving_proto_field_name=True)
    payload["program_runs"] = [
        {"program_id": "targetport", "verdict": {"result": "RESULT_MATCH"}, "duration": "0.1s"},
        {"program_id": "other", "verdict": {"result": "RESULT_NO_MATCH"}, "duration": "0.1s"},
    ]
    payload["plan_applications"] = [
        {
            "application_id": "application-1",
            "program_ids": ["targetport"],
            "plan": {"actions": []},
            "applied": True,
            "verification": {"result": "RESULT_NO_MATCH"},
        }
    ]
    closure.result.CopyFrom(from_dict(IncidentResult, payload))
    brief = incident_brief(closure, worktree=tmp_path)
    assert "targetport: Match" in brief
    assert "other: NoMatch" in brief
    assert "application-1" in brief
    assert "verification NoMatch" in brief


def test_missing_provider_session_restarts_reflection_fresh(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    class MissingSession(_CapturingBackend):
        def resume(self, *, session_id: str, worktree: Path, prompt: str, idempotency_key: str) -> ReflectionTurn:
            raise ResponderSessionUnavailable("session not found")

    backend = MissingSession()
    turn = SessionReflector(backend).resume(
        session_id="missing",
        incident_id="inc-1",
        worktree=tmp_path,
        outcome=_outcome(),
        history=[],
        outcome_commit="outcome-sha",
        closure=_fresh_closure(tmp_path),
    )
    assert turn.learning_decision == "no_change"
    assert backend.calls[0]["mode"] == "fresh"
    assert "session not found" in capsys.readouterr().err


def test_generic_resume_failure_propagates_without_fresh_retry(tmp_path: Path) -> None:
    error = RuntimeError("model unavailable")

    class FailingBackend(_CapturingBackend):
        def resume(self, *, session_id: str, worktree: Path, prompt: str, idempotency_key: str) -> ReflectionTurn:
            self.calls.append({"mode": "resume"})
            raise error

    backend = FailingBackend()
    with pytest.raises(RuntimeError) as caught:
        SessionReflector(backend).resume(
            session_id="session-1",
            incident_id="inc-1",
            worktree=tmp_path,
            outcome=_outcome(),
            history=[],
            outcome_commit="outcome-sha",
            closure=_fresh_closure(tmp_path),
        )
    assert caught.value is error
    assert backend.calls == [{"mode": "resume"}]


def test_claude_model_failure_is_not_classified_as_missing_session(tmp_path: Path) -> None:
    backend = ClaudeSessionBackend(
        executor=ScriptedAgent(lambda _request: failure(stderr="model unavailable")).executor
    )
    with pytest.raises(RuntimeError, match="model unavailable"):
        backend.resume(session_id="session-1", worktree=tmp_path, prompt="reflect", idempotency_key="key")


@pytest.mark.parametrize(
    ("provider", "backend_type"), [("codex", CodexSessionBackend), ("claude", ClaudeSessionBackend)]
)
def test_provider_missing_session_raises_typed_error(provider: str, backend_type: type, tmp_path: Path) -> None:
    backend = backend_type(executor=ScriptedAgent(lambda _request: scripted_resume_failure(provider)).executor)
    with pytest.raises(ResponderSessionUnavailable):
        backend.resume(session_id="missing", worktree=tmp_path, prompt="reflect", idempotency_key="key")


def _verified_repeat_closure(worktree: Path) -> IncidentClosure:
    closure = _fresh_closure(worktree)
    closure.request.memory_snapshot.playbooks.add(id="service-targetport-mismatch", markdown="# programs")
    closure.request.memory_snapshot.playbooks.add(id="image-pull-source-drift", markdown="# programs")
    return closure


def _verified_repeat_outcome() -> OutcomeRecord:
    return from_dict(
        OutcomeRecord,
        {
            "schema_version": 1,
            "incident_id": "inc-1",
            "classification": "success",
            "confirmed_root_causes": [{"summary": "Service targetPort does not match the container port"}],
            "repair_actions": [
                {
                    "action_id": "a1",
                    "kind": "program-plan",
                    "target": "Service/reservation",
                    "summary": "set targetPort",
                }
            ],
            "plan_applications": [
                {
                    "program_ids": ["service-targetport-mismatch"],
                    "applied": True,
                    "verification": {"result": "RESULT_NO_MATCH"},
                }
            ],
        },
    )


def test_typed_evidence_gap_decline_has_a_canonical_reason() -> None:
    from sdo.agents.responder.reflection import ReflectionTurn

    turn = ReflectionTurn.from_model_output(
        {
            "summary": "The Go-flagged fixed observation is absent.",
            "learning_decision": "no_change",
            "no_change_reason": "evidence gap",
            "decline_reason": "evidence_gap",
        }
    )
    assert turn.decline_reason == "evidence_gap"
    with pytest.raises(ValueError, match="canonical"):
        ReflectionTurn(
            summary="Missing observation",
            learning_decision="no_change",
            no_change_reason="some inferred absence",
            decline_reason="evidence_gap",
        )


def test_large_evidence_principle_is_stated_once(tmp_path: Path) -> None:
    backend = _CapturingBackend()
    SessionReflector(backend).resume(
        session_id="session-1",
        incident_id="inc-1",
        worktree=tmp_path,
        outcome=_outcome(),
        history=[],
        outcome_commit="outcome-sha",
        closure=_fresh_closure(tmp_path),
    )
    prompt = str(backend.calls[0]["prompt"])
    assert prompt.count("Preserve potentially large evidence output") == 1
    assert "temporary file" in prompt
    assert "search or extract" in prompt
    assert "never infer missing evidence from a truncated prefix" in prompt
