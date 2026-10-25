"""T1: typed Go work survives the real worker/session/provider boundary."""

from __future__ import annotations

from pathlib import Path

import pytest
from google.protobuf import json_format

from sdo.agents.responder.reflection import CodexSessionBackend, SessionReflector
from sdo.agents.responder.reflection_worker import run_reflection_task
from sdo.contracts import (
    AgentTask,
    IncidentClosure,
    OutcomeRecord,
    ReflectionRequest,
    ReflectionResult,
    Verdict,
    to_canonical_json,
)
from tests.structured_turns import ScriptedAgent, reply


class _Publisher:
    def __init__(self) -> None:
        self.payloads: list[str] = []

    def publish_payload(self, name: str, key: str, payload: str) -> None:
        assert (name, key) == ("t1-result", "agent-result.json")
        self.payloads.append(payload)


@pytest.mark.parametrize(
    ("disposition", "reason", "unassigned"),
    [
        ("false_match", "fixed_capture_match", False),
        ("question", "binding_not_written", False),
        ("question", "repair_equivalence_question", False),
        ("question", "program_refinement", False),
        ("question", "unbound_write", False),
        ("question", "uncertain", False),
        ("question", "evidence_gap", False),
        ("covered", "verified_equivalent_repair", False),
        ("false_match", "fixed_capture_match", True),
        ("question", "unbound_write", True),
    ],
)
def test_t1_work_reaches_scripted_session(
    tmp_path: Path, disposition: str, reason: str, unassigned: bool, prepare_reflection_memory
) -> None:
    # This pre-existing transport fixture predates typed cause work. The new
    # expectations and cause items are hand-written independently of R6 tests.
    fixture = Path(__file__).resolve().parents[4] / "fixtures/sdo/contracts/go/closure_repaired.json"
    closure = json_format.Parse(fixture.read_text(encoding="utf-8"), IncidentClosure())
    closure.result.responder_session_id = "t1-session"
    incident = closure.request.incident_id
    request = ReflectionRequest(
        closure=closure,
        outcome=OutcomeRecord(schema_version=1, incident_id=incident, classification="success"),
        outcome_commit="accepted-outcome",
        session_mode="resume",
    )
    request.memory_snapshot.accepted_commit = "accepted-outcome"
    request.memory_snapshot.goal_markdown = "Keep the service available."
    request.memory_snapshot.architecture_markdown = "One HTTP service."
    marker = f"T1_{disposition}_{reason}_{unassigned}"
    items = request.unassigned_work if unassigned else request.cause_work.add(cause_index=0).items
    item = items.add(disposition=disposition, reason=reason, program_id=marker)
    if disposition != "covered":
        item.question = f"{marker}: resolve this Go-owned question from protected evidence."
    else:
        # A forced reflection has Go's positive corpus edit already present.
        # The single session prompt must still carry the settled item.
        corpus = tmp_path / ".sdo/corpus/repeat-t1/labels.json"
        corpus.parent.mkdir(parents=True)
        corpus.write_text('{"root_cause_program_ids":["contract-repair"]}', encoding="utf-8")
    if reason == "evidence_gap":
        item.evidence_phase = "fixed"
        item.resources.add(kind="Service", namespace="app", name="api")
    if reason == "program_refinement":
        item.program_revision = "accepted-program-revision"
        item.evaluation.program_id = marker
        item.evaluation.program_revision = item.program_revision
        item.evaluation.verdict.result = Verdict.RESULT_UNKNOWN
        item.evaluation.error = "primitive observation unavailable"
        item.evaluation.duration.FromMilliseconds(1)

    base = prepare_reflection_memory(tmp_path, incident)
    request.outcome_commit = base
    request.memory_snapshot.accepted_commit = base
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="t1-task",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        workspace={"proposal_id": "t1-proposal", "checkout_path": str(tmp_path), "base_commit": base},
    )
    task.request.Pack(request)
    task_path = tmp_path.with_suffix(".task.json")
    task_path.write_text(to_canonical_json(task), encoding="utf-8")
    agent = ScriptedAgent(
        lambda _: reply(
            "codex",
            {"summary": "reviewed", "learning_decision": "no_change", "no_change_reason": "scripted result"},
            session_id="t1-session",
            branch="reflection",
        )
    )
    publisher = _Publisher()
    result = run_reflection_task(
        task_path,
        "t1-result",
        publisher,
        reflector=SessionReflector(CodexSessionBackend(model="scripted", executor=agent.executor)),
    )
    decision = ReflectionResult()
    assert result.response.Unpack(decision)
    assert decision.learning_decision == "no_change"
    assert len(publisher.payloads) == 1
    (prompt,) = agent.prompts
    assert marker in prompt, f"session discarded controller-owned {disposition}/{reason} work"
    if disposition != "covered":
        assert item.question in prompt
    if reason == "program_refinement":
        assert '"evaluation": {' in prompt
        assert '"result": "RESULT_UNKNOWN"' in prompt
        assert item.program_revision in prompt
        assert item.evaluation.error in prompt
    assert result.proposal_id == "t1-proposal"
