from pathlib import Path

import pytest
from google.protobuf import json_format
from google.protobuf.any_pb2 import Any

from libs.agent_cli.session_ownership import SessionHandoffError, SessionHandoffTimeout
from sdo.agents.responder.reflection import ReflectionTurn
from sdo.agents.responder.reflection_worker import run_reflection_task
from sdo.contracts import (
    AgentResult,
    AgentTask,
    IncidentClosure,
    MemoryDiagnostic,
    OperationalMemorySnapshot,
    ReflectionDecision,
    ReflectionRequest,
    ReflectionResult,
    parse_json,
    to_canonical_json,
    validate,
)
from tests.unit.sdo.agents.responder.test_reflection_outcomes import _outcome

FIXTURE = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts" / "go" / "closure_repaired.json"


def _closure() -> IncidentClosure:
    closure = json_format.Parse(FIXTURE.read_text(encoding="utf-8"), IncidentClosure())
    closure.request.memory_snapshot.CopyFrom(
        OperationalMemorySnapshot(
            accepted_commit=closure.request.repository_base_commit,
            goal_markdown="Health objective",
            architecture_markdown="Architecture",
        )
    )
    return closure


class Publisher:
    def __init__(self) -> None:
        self.payload: str | None = None

    def publish_payload(self, name: str, key: str, payload: str) -> None:
        assert (name, key) == ("result-map", "agent-result.json")
        self.payload = payload


class Reflector:
    def __init__(self) -> None:
        self.kwargs: dict[str, object] | None = None

    def resume(self, **kwargs: object) -> ReflectionTurn:
        self.kwargs = kwargs
        return ReflectionTurn(
            summary="Already covered", learning_decision="no_change", no_change_reason="No gap", playbook_ids=[]
        )


class DirectReflector(Reflector):
    def resume(self, **kwargs: object) -> ReflectionTurn:
        self.kwargs = kwargs
        import os

        assert os.environ["SDO_ACTOR"] == "responder"
        assert os.environ["SDO_PHASE"] == "reflection"
        assert os.environ["SDO_BASE_COMMIT"] == "outcome-commit"
        worktree = kwargs["worktree"]
        assert isinstance(worktree, Path)
        readme = worktree / ".sdo/playbooks/missing-service/README.md"
        readme.parent.mkdir(parents=True)
        readme.write_text("# Final authored bytes\n", encoding="utf-8")
        return ReflectionTurn(
            summary="Capture signature", learning_decision="updated", playbook_ids=["missing-service"]
        )


def test_reflection_worker_uses_typed_context_and_returns_decision(tmp_path: Path) -> None:
    closure = _closure()
    request = ReflectionRequest(
        closure=closure,
        outcome=_outcome(closure.request.incident_id),
        outcome_commit="outcome-commit",
    )
    cause = request.cause_work.add(cause_index=0)
    cause.items.add(
        disposition="question", reason="binding_not_written", question="Why did this clear without a write?"
    )
    request.memory_snapshot.accepted_commit = "outcome-commit"
    packed = Any()
    packed.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        request=packed,
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "outcome-commit"},
    )
    path = tmp_path / "agent-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    publisher, reflector = Publisher(), Reflector()

    run_reflection_task(path, "result-map", publisher, reflector=reflector)

    assert reflector.kwargs is not None
    assert reflector.kwargs["cause_work"] == list(request.cause_work)
    assert reflector.kwargs["outcome_commit"] == "outcome-commit"
    assert reflector.kwargs["session_mode"] == "fresh"
    assert publisher.payload is not None
    result = parse_json(publisher.payload, AgentResult())
    assert result.proposal_id == "proposal-1"
    decision = ReflectionResult()
    assert result.response.Unpack(decision)
    assert decision.learning_decision == "no_change"


def test_reflection_worker_restores_result_program_runs_from_the_outcome(tmp_path: Path) -> None:
    closure = _closure()
    del closure.result.program_runs[:]
    outcome = _outcome(closure.request.incident_id)
    outcome.program_runs.add(program_id="targetport").duration.FromMilliseconds(5)
    request = ReflectionRequest(closure=closure, outcome=outcome, outcome_commit="outcome-commit")
    request.memory_snapshot.accepted_commit = "outcome-commit"
    packed = Any()
    packed.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        request=packed,
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "outcome-commit"},
    )
    path = tmp_path / "agent-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    reflector = Reflector()

    run_reflection_task(path, "result-map", Publisher(), reflector=reflector)

    assert reflector.kwargs is not None
    seen = reflector.kwargs["closure"]
    assert isinstance(seen, IncidentClosure)
    assert [run.program_id for run in seen.result.program_runs] == ["targetport"]


def test_reflection_worker_restores_result_transcripts_from_the_outcome(tmp_path: Path) -> None:
    closure = _closure()
    del closure.result.program_tool_calls[:]
    del closure.result.plan_applications[:]
    outcome = _outcome(closure.request.incident_id)
    outcome.program_tool_calls.add(command="kubectl get pods", summary="listed")
    outcome.plan_applications.add(application_id="plan-1")
    request = ReflectionRequest(closure=closure, outcome=outcome, outcome_commit="outcome-commit")
    request.memory_snapshot.accepted_commit = "outcome-commit"
    packed = Any()
    packed.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        request=packed,
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "outcome-commit"},
    )
    path = tmp_path / "agent-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    reflector = Reflector()

    run_reflection_task(path, "result-map", Publisher(), reflector=reflector)

    assert reflector.kwargs is not None
    seen = reflector.kwargs["closure"]
    assert isinstance(seen, IncidentClosure)
    assert [call.command for call in seen.result.program_tool_calls] == ["kubectl get pods"]
    assert [application.application_id for application in seen.result.plan_applications] == ["plan-1"]


def test_reflection_worker_resumes_only_with_valid_responder_session(tmp_path: Path) -> None:
    closure = _closure()
    assert closure.HasField("result")
    closure.result.responder_session_id = "responder-session-123"
    request = ReflectionRequest(
        closure=closure,
        outcome=_outcome(closure.request.incident_id),
        outcome_commit="outcome-commit",
        session_mode="resume",
    )
    request.memory_snapshot.accepted_commit = "outcome-commit"
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-resume",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "outcome-commit"},
    )
    task.request.Pack(request)
    path = tmp_path / "resume-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    reflector = Reflector()

    run_reflection_task(path, "result-map", Publisher(), reflector=reflector)
    assert reflector.kwargs is not None
    assert reflector.kwargs["session_mode"] == "resume"
    assert reflector.kwargs["session_id"] == "responder-session-123"

    request.closure.result.responder_session_id = ""
    task.request.Pack(request)
    path.write_text(to_canonical_json(task), encoding="utf-8")
    fallback = Reflector()
    result = run_reflection_task(path, "result-map", Publisher(), reflector=fallback)
    assert fallback.kwargs is None
    assert result.status == "failed"
    assert result.failure_reason == "session_unavailable"

    request.session_mode = "unknown"
    request.closure.result.responder_session_id = "responder-session-123"
    task.request.Pack(request)
    path.write_text(to_canonical_json(task), encoding="utf-8")
    with pytest.raises(ValueError, match="invalid reflection session mode"):
        run_reflection_task(path, "result-map", Publisher(), reflector=Reflector())

    request.session_mode = "resume"
    request.closure.result.responder_session_id = "invalid/session"
    task.request.Pack(request)
    path.write_text(to_canonical_json(task), encoding="utf-8")
    fallback = Reflector()
    result = run_reflection_task(path, "result-map", Publisher(), reflector=fallback)
    assert fallback.kwargs is None
    assert result.status == "failed"
    assert result.failure_reason == "session_unavailable"


def test_reflection_worker_uses_typed_guidance_for_builtin_reflector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from sdo.agents.responder import reflection_worker

    closure = _closure()
    request = ReflectionRequest(
        closure=closure,
        outcome=_outcome(closure.request.incident_id),
        outcome_commit="outcome-commit",
        guidance="generalize-spec",
    )
    request.memory_snapshot.accepted_commit = "outcome-commit"
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-guidance",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "outcome-commit"},
    )
    task.request.Pack(request)
    path = tmp_path / "guidance-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    seen: list[str] = []
    monkeypatch.setattr(reflection_worker, "_default_reflector", lambda guidance: seen.append(guidance) or Reflector())

    run_reflection_task(path, "result-map", Publisher())
    assert seen == ["generalize-spec"]

    request.guidance = "unsafe"
    task.request.Pack(request)
    path.write_text(to_canonical_json(task), encoding="utf-8")
    with pytest.raises(ValueError, match="invalid reflection guidance"):
        run_reflection_task(path, "result-map", Publisher())


def test_reflection_worker_requires_commit_bound_logical_memory(tmp_path: Path) -> None:
    closure = _closure()
    request = ReflectionRequest(
        closure=closure,
        outcome=_outcome(closure.request.incident_id),
        outcome_commit="outcome-commit",
    )
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-missing-memory",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "outcome-commit"},
    )
    task.request.Pack(request)
    path = tmp_path / "agent-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")

    with pytest.raises(ValueError, match="logical memory snapshot"):
        run_reflection_task(path, "result-map", Publisher(), reflector=Reflector())


def test_reflection_worker_keeps_direct_edits_and_returns_logical_ids(tmp_path: Path) -> None:
    closure = _closure()
    outcome = _outcome(closure.request.incident_id)
    request = ReflectionRequest(
        closure=closure,
        outcome=outcome,
        outcome_commit="outcome-commit",
    )
    request.memory_snapshot.accepted_commit = "outcome-commit"
    packed = Any()
    packed.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-logical",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        request=packed,
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "outcome-commit"},
    )
    path = tmp_path / "agent-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    publisher = Publisher()
    run_reflection_task(path, "result-map", publisher, reflector=DirectReflector())
    assert publisher.payload is not None
    result = parse_json(publisher.payload, AgentResult())
    decision = ReflectionResult()
    assert result.response.Unpack(decision)
    assert decision.proposed_artifact_ids == ["missing-service"]
    assert not decision.HasField("artifact_proposal")
    assert (tmp_path / ".sdo/playbooks/missing-service/README.md").read_text() == "# Final authored bytes\n"


@pytest.mark.parametrize("decision", ["no_change", "sdk_gap"])
def test_nonupdates_require_a_concrete_reason_and_no_ids(decision: str) -> None:
    with pytest.raises(ValueError, match="no_change_reason"):
        ReflectionTurn(summary="Nothing safe", learning_decision=decision)
    with pytest.raises(ValueError, match="cannot claim"):
        ReflectionTurn(
            summary="Nothing safe",
            learning_decision=decision,
            no_change_reason="No observation",
            playbook_ids=["fault"],
        )


def test_reflection_worker_forwards_go_retry_diagnostics(tmp_path: Path) -> None:
    closure = _closure()
    request = ReflectionRequest(
        closure=closure,
        outcome=_outcome(closure.request.incident_id),
        outcome_commit="outcome-commit",
        validation_attempt=2,
        previous_diagnostics=[MemoryDiagnostic(rule="detector.manifest", message="bad id")],
    )
    request.memory_snapshot.accepted_commit = "outcome-commit"
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1-retry1",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "outcome-commit"},
    )
    task.request.Pack(request)
    path = tmp_path / "retry-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    reflector = Reflector()

    run_reflection_task(path, "result-map", Publisher(), reflector=reflector)

    assert reflector.kwargs is not None
    assert reflector.kwargs["validation_attempt"] == 2
    assert [d.rule for d in reflector.kwargs["diagnostics"]] == ["detector.manifest"]  # type: ignore[attr-defined]


def test_reflection_schema_rejects_legacy_proposals() -> None:
    with pytest.raises(ValueError, match="Extra inputs"):
        ReflectionTurn.model_validate(
            {"summary": "Legacy", "learning_decision": "updated", "artifact_proposal": {"playbooks": []}}
        )


@pytest.mark.parametrize(
    ("disposition", "reason"), [("false_match", "fixed_capture_match"), ("covered", "verified_equivalent_repair")]
)
def test_registered_task_preserves_settled_work_in_actual_session_prompt(
    tmp_path: Path, disposition: str, reason: str
) -> None:
    from sdo.agents.responder.reflection import SessionReflector
    from tests.unit.sdo.agents.responder.test_reflection import _CapturingBackend

    closure = _closure()
    closure.result.responder_session_id = "incident-session"
    request = ReflectionRequest(
        closure=closure,
        outcome=_outcome(closure.request.incident_id),
        outcome_commit="outcome-commit",
        session_mode="resume",
    )
    request.memory_snapshot.accepted_commit = "outcome-commit"
    cause = request.cause_work.add(cause_index=0)
    cause.items.add(
        program_id="source-repair",
        disposition=disposition,
        reason=reason,
        question="Review the fixed regression" if disposition == "false_match" else "",
    )
    request.outcome.reflection_eligibility.version = 1
    request.outcome.reflection_eligibility.causes.extend(request.cause_work)
    packed = Any()
    packed.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        request=packed,
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "outcome-commit"},
    )
    path = tmp_path / "task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    backend = _CapturingBackend()
    run_reflection_task(path, "result-map", Publisher(), reflector=SessionReflector(backend))
    prompt = str(backend.calls[0]["prompt"])
    assert backend.calls[0]["mode"] == "resume"
    assert disposition in prompt
    assert reason in prompt
    assert "Return updated whenever final files include Go or agent edits" in prompt
    assert "leave the tree unchanged" not in prompt
    assert "skip validation" not in prompt
    assert prompt.count(reason) == 1


def _task_path(tmp_path: Path, *, deadline: float | None = None) -> Path:
    closure = _closure()
    request = ReflectionRequest(closure=closure, outcome=_outcome(closure.request.incident_id), outcome_commit="c")
    request.memory_snapshot.accepted_commit = "c"
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "c"},
    )
    task.request.Pack(request)
    if deadline is not None:
        task.deadline.FromNanoseconds(int(deadline * 1e9))
    path = tmp_path / "agent-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    return path


class FailingReflector:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def resume(self, **_: object) -> ReflectionTurn:
        raise self.error


def _timed_out() -> RuntimeError:
    from libs.agent_cli.structured import StructuredTurnTimeout

    try:
        raise StructuredTurnTimeout(451.02)
    except StructuredTurnTimeout as exc:
        try:
            raise RuntimeError(f"codex reflection failed: {exc}") from exc
        except RuntimeError as wrapped:
            return wrapped


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (_timed_out(), "agent_timeout"),
        (RuntimeError("model I/O exceeds 4194304 bytes"), "agent_failed"),
        (SessionHandoffTimeout("writer did not release"), "session_handoff_timeout"),
        (SessionHandoffError("invalid record"), "session_handoff_invalid"),
    ],
)
def test_failed_reflection_publishes_a_typed_reason_instead_of_failing_the_job(
    tmp_path: Path, error: BaseException, reason: str
) -> None:
    publisher = Publisher()

    run_reflection_task(_task_path(tmp_path), "result-map", publisher, reflector=FailingReflector(error))

    assert publisher.payload is not None
    result = parse_json(publisher.payload, AgentResult())
    assert (result.status, result.failure_reason, result.error_summary) == ("failed", reason, str(error))
    assert not result.HasField("response")
    validate(result)
    validate(ReflectionDecision(learning_decision="failed", failure_reason=reason))


def test_reflection_budget_ends_before_the_frozen_task_deadline(tmp_path: Path) -> None:
    # The Job's active deadline and any claimed standby end no earlier than the task
    # deadline, so an agent budget derived from it cannot be killed from outside first.
    import time

    from sdo.agents.responder.reflection import REFLECTION_RESULT_MARGIN_SECONDS, reflection_budget_seconds

    deadline = time.time() + 1800
    budgets: list[float] = []

    class BudgetReflector(Reflector):
        def resume(self, **kwargs: object) -> ReflectionTurn:
            budgets.append(reflection_budget_seconds())
            return super().resume(**kwargs)

    run_reflection_task(_task_path(tmp_path, deadline=deadline), "result-map", Publisher(), reflector=BudgetReflector())

    (budget,) = budgets
    assert 1800 - REFLECTION_RESULT_MARGIN_SECONDS - 5 < budget <= 1800 - REFLECTION_RESULT_MARGIN_SECONDS


def test_expired_task_deadline_is_a_typed_timeout(tmp_path: Path) -> None:
    import time

    from sdo.agents.responder.reflection import reflection_budget_seconds

    class BudgetReflector(Reflector):
        def resume(self, **kwargs: object) -> ReflectionTurn:
            reflection_budget_seconds()
            return super().resume(**kwargs)

    publisher = Publisher()
    run_reflection_task(
        _task_path(tmp_path, deadline=time.time() + 60), "result-map", publisher, reflector=BudgetReflector()
    )

    assert publisher.payload is not None
    assert parse_json(publisher.payload, AgentResult()).failure_reason == "agent_timeout"


def test_reflection_default_budget_fits_the_controller_and_eval_deadlines() -> None:
    from benchmarks.sregym.adapter.native_operator import NativeInstallConfig
    from benchmarks.sregym.adapter.persistent import DRAIN_TIMEOUT_SECONDS
    from sdo.agents.responder.reflection import DEFAULT_REFLECTION_SECONDS

    response_timeout = NativeInstallConfig.__dataclass_fields__["response_timeout_seconds"].default
    assert DEFAULT_REFLECTION_SECONDS < response_timeout < DRAIN_TIMEOUT_SECONDS
