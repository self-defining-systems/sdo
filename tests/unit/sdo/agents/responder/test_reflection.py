"""Native reflection sessions consume logical memory and return typed learning decisions."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from agentshim.providers.codex import CodexSandboxConfig, parse_sandbox
from agentshim.testing import scripted_resume_failure

from sdo.agents.responder.reflection import (
    ClaudeSessionBackend,
    CodexSessionBackend,
    ReflectionTurn,
    ResponderSessionUnavailable,
    SessionReflector,
    reflection_output_schema,
)
from sdo.agents.views import (
    BrokerClosure,
    IncidentRequest,
    IncidentResult,
    OperationalMemorySnapshot,
    StateChanges,
)
from sdo.contracts import OutcomeClassification, OutcomeRecord
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
def test_reflection_gate_uses_typed_classification(classification: OutcomeClassification, expected: bool) -> None:
    outcome = _outcome()
    outcome.classification = classification.value
    reflected = SessionReflector(_CapturingBackend()).should_reflect(
        outcome, health_verified=True, session_id="session"
    )
    assert reflected is expected


def test_reflection_gate_rejects_unknown_classification() -> None:
    outcome = _outcome()
    outcome.classification = "sucess"
    with pytest.raises(ValueError, match="sucess"):
        SessionReflector(_CapturingBackend()).should_reflect(outcome, health_verified=True, session_id="session")


def _fresh_closure(worktree: Path, *, evidence: str = "Deployment unavailable") -> BrokerClosure:
    root = Path(__file__).resolve().parents[5] / "tests" / "fixtures" / "sdo" / "contracts"
    request = IncidentRequest.model_validate_json((root / "incident_request.json").read_text(encoding="utf-8"))
    finding = request.findings[0].model_copy(update={"evidence": evidence})
    assert request.repository_base_commit
    request = request.model_copy(
        update={
            "findings": [finding],
            "repository_worktree": str(worktree),
            "memory_snapshot": OperationalMemorySnapshot(
                accepted_commit=request.repository_base_commit,
                goal_markdown="The system remains healthy.",
                architecture_markdown="Accepted architecture summary.",
            ),
        }
    )
    result = IncidentResult.model_validate_json((root / "incident_result.json").read_text(encoding="utf-8"))
    at = datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc)
    return BrokerClosure(
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
    assert "sdo-operator program scaffold" in prompt
    assert "sdo-operator program propose" in prompt
    assert "Accepted architecture summary" in prompt
    assert ".sdo/outcomes/" not in prompt
    assert "sdo.operational_memory" not in prompt
    assert json.dumps({"incident_id": "inc-1"}) not in prompt  # full outcome context is richer


def test_reflection_prompt_requires_one_program_decision_per_fault_and_retries_draft_test(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SDO_PLAYBOOK_PROGRAM_EXAMPLE_DIR", raising=False)
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
    for required in (
        "for each confirmed fault",
        "diagnose.go",
        "mitigate.go",
        "open, fixed, and healthy",
        "recordings",
        "labels",
        "--corpus-from-incident <id>",
        "--bind name=Kind/namespace/name",
        "--peer-bind name=Kind/namespace/name",
        "ReconcileFromSource or Rollback",
        "container status reasons",
        "sdo.dev/operator/sdk/program",
        ".sdo/lib",
        "diagnostics module path",
        "Params",
        "derive names, ports and images from bindings and the ReadView",
        "every name, port and image renamed",
        "allowed imports",
        "/opt/sdo/examples/playbook-programs/playbooks/service-targetport-mismatch/diagnose.go",
        "/opt/sdo/examples/playbook-programs/corpus/targetport-mismatch/",
        "sdk_gap",
        "Record each reused program ID",
        "NoMatch",
        "no shell commands",
        "rerun",
    ):
        assert required in prompt


def test_reflection_prompt_uses_configured_example_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_PLAYBOOK_PROGRAM_EXAMPLE_DIR", "/custom/examples")
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
    assert "/custom/examples/playbooks/service-targetport-mismatch/diagnose.go" in str(backend.calls[0]["prompt"])


def test_draft_test_rejection_is_shown_to_reflection_retry(tmp_path: Path) -> None:
    backend = _CapturingBackend()
    SessionReflector(backend).resume(
        session_id="session-1",
        incident_id="inc-1",
        worktree=tmp_path,
        outcome=_outcome(),
        history=[],
        outcome_commit="outcome-sha",
        validation_feedback="draft-test: healthy peer returned Match",
        rejected_proposal_diff="+func Diagnose() {}",
        closure=_fresh_closure(tmp_path),
    )
    assert backend.calls[0]["mode"] == "fresh"
    assert "draft-test: healthy peer returned Match" in str(backend.calls[0]["prompt"])
    assert "+func Diagnose() {}" in str(backend.calls[0]["prompt"])


def test_markdown_rejection_explains_proposal_shape_with_example(tmp_path: Path) -> None:
    backend = _CapturingBackend()
    SessionReflector(backend).resume(
        session_id="session-1",
        incident_id="inc-1",
        worktree=tmp_path,
        outcome=_outcome(),
        history=[],
        outcome_commit="outcome-sha",
        validation_feedback=(
            'playbook "service-targetport-mismatch": markdown: expected nonempty body without front matter'
        ),
        closure=_fresh_closure(tmp_path),
    )
    prompt = str(backend.calls[0]["prompt"])
    assert 'markdown: "# Service targetPort mismatch\\n\\nCheck <SERVICE>."' in prompt
    assert "full README" in prompt
    assert "fault_class" in prompt


def test_fresh_reflection_brief_keeps_observed_state_for_corpus(tmp_path: Path) -> None:
    from sdo.agents.responder.reflection_brief import incident_brief

    closure = _fresh_closure(tmp_path)
    observed = datetime(2026, 7, 9, 18, 0, tzinfo=timezone.utc)
    closure.request.state_changes = StateChanges.model_validate(
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
        }
    )
    closure.final_state_changes = StateChanges.model_validate(
        {
            "baseline_at": observed,
            "observed_at": observed,
            "changes": [],
        }
    )
    brief = incident_brief(closure, worktree=tmp_path)
    assert "State changes at dispatch" in brief
    assert "targetPort" in brief
    assert "State changes at verification" in brief


def test_fresh_reflection_brief_summarizes_program_diagnosis_and_application(tmp_path: Path) -> None:
    from sdo.agents.responder.reflection_brief import incident_brief

    closure = _fresh_closure(tmp_path)
    assert closure.result is not None
    payload = closure.result.model_dump(mode="json")
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
    closure.result = IncidentResult.model_validate(payload)

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


def test_reflection_output_schema_is_strict_and_artifact_typed() -> None:
    schema = reflection_output_schema()
    assert schema["additionalProperties"] is False
    assert "artifact_proposal" in schema["properties"]
    assert schema["required"] == list(schema["properties"])
