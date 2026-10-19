from pathlib import Path

import pytest
from google.protobuf import json_format
from google.protobuf.any_pb2 import Any

from sdo.agents.responder.reflection import ReflectionTurn
from sdo.agents.responder.reflection_worker import run_reflection_task
from sdo.contracts import (
    AgentResult,
    AgentTask,
    IncidentClosure,
    MemoryDiagnostic,
    OperationalMemorySnapshot,
    ReflectionRequest,
    ReflectionResult,
    parse_json,
    to_canonical_json,
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
            summary="Already covered", learning_decision="no_change", no_change_reason="No gap", proposed_changes=[]
        )


class LogicalReflector(Reflector):
    def resume(self, **kwargs: object) -> ReflectionTurn:
        self.kwargs = kwargs
        outcome = kwargs["outcome"]
        return ReflectionTurn.model_validate(
            {
                "summary": "Capture the confirmed signature",
                "learning_decision": "updated",
                "proposed_changes": [],
                "artifact_proposal": {
                    "playbooks": [
                        {
                            "id": "missing-service",
                            "owner": "responder",
                            "fault_class": "missing-service",
                            "originating_incident": outcome.incident_id,
                            "markdown": "# Missing service\n\n## Verification\nCheck recovery.\n",
                        }
                    ]
                },
            }
        )


class WrongOriginReflector(LogicalReflector):
    def resume(self, **kwargs: object) -> ReflectionTurn:
        turn = super().resume(**kwargs)
        assert turn.artifact_proposal is not None
        return turn.model_copy(
            update={
                "artifact_proposal": turn.artifact_proposal.model_copy(
                    update={
                        "playbooks": [
                            turn.artifact_proposal.playbooks[0].model_copy(
                                update={"originating_incident": "unrelated-incident"}
                            )
                        ]
                    }
                )
            }
        )


def test_reflection_worker_uses_typed_context_and_returns_decision(tmp_path: Path) -> None:
    closure = _closure()
    request = ReflectionRequest(
        closure=closure,
        outcome=_outcome(closure.request.incident_id),
        outcome_commit="outcome-commit",
    )
    request.memory_snapshot.accepted_commit = "outcome-commit"
    packed = Any()
    packed.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1",
        agent_id="sdo-reflector",
        task_type="outcome-reflection",
        request=packed,
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "outcome-commit"},
    )
    path = tmp_path / "agent-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    publisher, reflector = Publisher(), Reflector()

    run_reflection_task(path, "result-map", publisher, reflector=reflector)

    assert reflector.kwargs is not None
    assert reflector.kwargs["outcome_commit"] == "outcome-commit"
    assert reflector.kwargs["session_mode"] == "fresh"
    assert publisher.payload is not None
    result = parse_json(publisher.payload, AgentResult())
    assert result.proposal_id == "proposal-1"
    decision = ReflectionResult()
    assert result.response.Unpack(decision)
    assert decision.learning_decision == "no_change"


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
        task_type="outcome-reflection",
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
    with pytest.raises(ValueError, match="valid responder session"):
        run_reflection_task(path, "result-map", Publisher(), reflector=Reflector())

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
    with pytest.raises(ValueError, match="valid responder session"):
        run_reflection_task(path, "result-map", Publisher(), reflector=Reflector())


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
        task_type="outcome-reflection",
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
        task_type="outcome-reflection",
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "outcome-commit"},
    )
    task.request.Pack(request)
    path = tmp_path / "agent-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")

    with pytest.raises(ValueError, match="logical memory snapshot"):
        run_reflection_task(path, "result-map", Publisher(), reflector=Reflector())


def test_reflection_worker_returns_logical_artifact_proposal(tmp_path: Path) -> None:
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
        task_type="outcome-reflection",
        request=packed,
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "outcome-commit"},
    )
    path = tmp_path / "agent-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    publisher = Publisher()
    run_reflection_task(path, "result-map", publisher, reflector=LogicalReflector())
    assert publisher.payload is not None
    result = parse_json(publisher.payload, AgentResult())
    decision = ReflectionResult()
    assert result.response.Unpack(decision)
    assert decision.proposed_artifact_ids == ["missing-service"]
    assert decision.artifact_proposal.playbooks[0].id == "missing-service"


def test_reflection_worker_rejects_false_origin(tmp_path: Path) -> None:
    closure = _closure()
    request = ReflectionRequest(
        closure=closure,
        outcome=_outcome(closure.request.incident_id),
        outcome_commit="outcome-commit",
    )
    request.memory_snapshot.accepted_commit = "outcome-commit"
    packed = Any()
    packed.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-origin",
        agent_id="sdo-reflector",
        task_type="outcome-reflection",
        request=packed,
        workspace={"proposal_id": "proposal-1", "checkout_path": str(tmp_path), "base_commit": "outcome-commit"},
    )
    path = tmp_path / "agent-task.json"
    path.write_text(to_canonical_json(task), encoding="utf-8")
    publisher = Publisher()
    import pytest

    with pytest.raises(ValueError, match="origin"):
        run_reflection_task(path, "result-map", publisher, reflector=WrongOriginReflector())
    assert publisher.payload is None


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
        task_type="outcome-reflection",
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
