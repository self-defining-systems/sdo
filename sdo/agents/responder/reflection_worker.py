"""Reflection worker behind the implementation-neutral task contract."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Protocol, cast

from google.protobuf import json_format

from sdo.agents.responder.credentials import prepare_claude_home, prepare_codex_home
from sdo.agents.responder.job import KubernetesResultPublisher
from sdo.agents.responder.reflection import (
    REFLECTION_GUIDANCE_MODES,
    ClaudeSessionBackend,
    CodexSessionBackend,
    ReflectionGuidance,
    ReflectionSessionMode,
    ReflectionTurn,
    SessionReflector,
)
from sdo.agents.views import (
    BrokerClosure,
    OperationalMemorySnapshot,
)
from sdo.contracts import (
    AgentResult,
    AgentTask,
    OperationalArtifactProposal,
    ReflectionRequest,
    ReflectionResult,
    parse_json,
    to_canonical_json,
)

AGENT_ID = "sdo-reflector"
TASK_TYPE = "outcome-reflection"
DEFAULT_TASK_PATH = Path("/sdo/request/agent-task.json")
_SESSION_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")


class ResultPublisher(Protocol):
    def publish_payload(self, name: str, key: str, payload: str) -> None: ...


class Reflector(Protocol):
    def resume(self, **kwargs: object) -> ReflectionTurn: ...


def _default_reflector(guidance: ReflectionGuidance) -> SessionReflector:
    provider = os.environ.get("SDO_AGENT_PROVIDER", "codex").strip().lower()
    if provider == "codex":
        backend = CodexSessionBackend()
    elif provider == "claude":
        backend = ClaudeSessionBackend()
    else:
        raise ValueError(f"unsupported reflection provider {provider!r}")
    return SessionReflector(backend, guidance=guidance)


def run_reflection_task(
    task_path: Path,
    result_name: str,
    publisher: ResultPublisher,
    *,
    reflector: Reflector | None = None,
) -> AgentResult:
    task = cast("AgentTask", parse_json(task_path.read_text(encoding="utf-8"), AgentTask()))
    if task.schema_version != "sdo.dev/v1alpha1" or not task.task_id:
        raise ValueError("reflection task has invalid version or task ID")
    if task.agent_id != AGENT_ID or task.task_type != TASK_TYPE:
        raise ValueError(f"reflection worker cannot handle agent {task.agent_id!r} task type {task.task_type!r}")
    request = ReflectionRequest()
    if not task.request.Unpack(request):
        raise ValueError("reflection task request is not a ReflectionRequest")
    if not request.HasField("outcome") or not request.outcome_commit:
        raise ValueError("reflection task requires an outcome and outcome commit")
    if not task.workspace.proposal_id or not task.workspace.checkout_path:
        raise ValueError("reflection task requires a proposal workspace")
    if not request.HasField("memory_snapshot"):
        raise ValueError("reflection task requires a logical memory snapshot")
    outcome = request.outcome
    history = list(request.prior_outcomes)
    closure = BrokerClosure.model_validate_json(
        json_format.MessageToJson(
            request.closure,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
    )
    snapshot = OperationalMemorySnapshot.model_validate(
        json_format.MessageToDict(request.memory_snapshot, preserving_proto_field_name=True)
    )
    if snapshot.accepted_commit != request.outcome_commit:
        raise ValueError("reflection memory snapshot is not bound to committed outcome")
    closure.request = closure.request.model_copy(update={"memory_snapshot": snapshot})
    if closure.request.incident_id != outcome.incident_id:
        raise ValueError("reflection outcome incident does not match closure")
    session_mode = request.session_mode or "fresh"
    if session_mode not in {"fresh", "resume"}:
        raise ValueError(f"invalid reflection session mode {session_mode!r}")
    prior_session = closure.result.responder_session_id if closure.result is not None else ""
    if session_mode == "resume" and not _SESSION_TOKEN.fullmatch(prior_session or ""):
        raise ValueError("resume reflection requires a valid responder session")
    guidance = request.guidance or "baseline"
    if guidance not in REFLECTION_GUIDANCE_MODES:
        raise ValueError(f"invalid reflection guidance {guidance!r}")
    selected = reflector or _default_reflector(cast("ReflectionGuidance", guidance))
    turn = selected.resume(
        session_id=prior_session or "",
        incident_id=outcome.incident_id,
        worktree=Path(task.workspace.checkout_path),
        outcome=outcome,
        history=history,
        outcome_commit=request.outcome_commit,
        session_mode=cast("ReflectionSessionMode", session_mode),
        closure=closure,
    )
    if turn.proposed_changes:
        raise ValueError("logical reflection cannot return physical paths")
    if turn.learning_decision == "updated" and turn.artifact_proposal is None:
        raise ValueError("updated logical reflection requires typed artifacts")
    if turn.learning_decision == "no_change" and turn.artifact_proposal is not None:
        raise ValueError("no-change reflection cannot return artifacts")
    if turn.artifact_proposal is not None:
        for detector in turn.artifact_proposal.detectors:
            if (
                detector.originating_commit != request.outcome_commit
                or detector.originating_incident != outcome.incident_id
            ):
                raise ValueError("reflected detector origin must match authoritative outcome")
        for playbook in turn.artifact_proposal.playbooks:
            if playbook.originating_incident != outcome.incident_id:
                raise ValueError("reflected playbook origin must match authoritative outcome")
    typed_result = ReflectionResult(
        summary=turn.summary,
        learning_decision=turn.learning_decision,
        no_change_reason=turn.no_change_reason or "",
        proposed_artifact_ids=turn.artifact_proposal.artifact_ids() if turn.artifact_proposal else [],
    )
    if turn.artifact_proposal is not None:
        typed_result.artifact_proposal.CopyFrom(
            json_format.ParseDict(turn.artifact_proposal.model_dump(mode="json"), OperationalArtifactProposal())
        )
    result = AgentResult(
        schema_version="sdo.dev/v1alpha1",
        task_id=task.task_id,
        status="completed",
        proposal_id=task.workspace.proposal_id,
    )
    result.response.Pack(typed_result)
    publisher.publish_payload(result_name, "agent-result.json", to_canonical_json(result))
    return result


def main() -> int:
    namespace = os.environ.get("SDO_NAMESPACE", "")
    result_name = os.environ.get("SDO_RESULT_CONFIGMAP", "")
    if not namespace or not result_name:
        print("SDO_NAMESPACE and SDO_RESULT_CONFIGMAP are required", file=sys.stderr)
        return 2
    try:
        prepare_codex_home()
        prepare_claude_home()
        run_reflection_task(
            Path(os.environ.get("SDO_TASK_PATH", str(DEFAULT_TASK_PATH))),
            result_name,
            KubernetesResultPublisher(namespace=namespace),
        )
    except (OSError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
