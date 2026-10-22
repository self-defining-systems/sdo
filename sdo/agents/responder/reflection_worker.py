"""Reflection worker behind the implementation-neutral task contract."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from google.protobuf import json_format

from sdo.agents.provider import resolve_agent
from sdo.agents.responder.credentials import AGENT_SKILLS, prepare_claude_home, prepare_codex_home
from sdo.agents.responder.job import KubernetesResultPublisher
from sdo.agents.responder.reflection import (
    REFLECTION_DEPTHS,
    REFLECTION_GUIDANCE_MODES,
    ClaudeSessionBackend,
    CodexSessionBackend,
    ReflectionDepth,
    ReflectionGuidance,
    ReflectionSessionMode,
    ReflectionTurn,
    SessionReflector,
)
from sdo.contracts import (
    SCHEMA_VERSION,
    AgentResult,
    AgentTask,
    IncidentClosure,
    MemoryDiagnostic,
    OutcomeRecord,
    ReflectionRequest,
    ReflectionResult,
    SdkGap,
    parse_json,
    to_canonical_json,
    validate,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

AGENT_ID = "sdo-reflector"
TASK_TYPE = "outcome-reflection-direct-v1"
DEFAULT_TASK_PATH = Path("/sdo/request/agent-task.json")
_SESSION_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")


class ResultPublisher(Protocol):
    def publish_payload(self, name: str, key: str, payload: str) -> None: ...


class Reflector(Protocol):
    def resume(
        self,
        *,
        session_id: str,
        incident_id: str,
        worktree: Path,
        outcome: OutcomeRecord,
        history: list[OutcomeRecord],
        outcome_commit: str,
        session_mode: ReflectionSessionMode = "resume",
        closure: IncidentClosure | None = None,
        validation_attempt: int = 1,
        diagnostics: Sequence[MemoryDiagnostic] = (),
        depth: ReflectionDepth = "full",
    ) -> ReflectionTurn: ...


def _default_reflector(guidance: ReflectionGuidance) -> SessionReflector:
    resolved = resolve_agent("reflection", "reflection")
    backend = (ClaudeSessionBackend if resolved.provider == "claude" else CodexSessionBackend)(model=resolved.model)
    return SessionReflector(backend, guidance=guidance)


def run_reflection_task(
    task_path: Path,
    result_name: str,
    publisher: ResultPublisher,
    *,
    reflector: Reflector | None = None,
) -> AgentResult:
    task = cast("AgentTask", parse_json(task_path.read_text(encoding="utf-8"), AgentTask()))
    validate(task)
    request = ReflectionRequest()
    if not task.request.Unpack(request):
        raise ValueError("reflection task request is not a ReflectionRequest")
    if not request.HasField("outcome") or not request.outcome_commit:
        raise ValueError("reflection task requires an outcome and outcome commit")
    if not task.workspace.proposal_id or not task.workspace.checkout_path or not task.workspace.base_commit:
        raise ValueError("reflection task requires a proposal workspace")
    if not request.HasField("memory_snapshot"):
        raise ValueError("reflection task requires a logical memory snapshot")
    outcome = request.outcome
    history = list(request.prior_outcomes)
    closure = IncidentClosure()
    closure.CopyFrom(request.closure)
    if closure.HasField("result") and not closure.result.program_runs:
        # The request carries the responder's program runs once, in the outcome derived from them.
        closure.result.program_runs.extend(outcome.program_runs)
    if (
        request.memory_snapshot.accepted_commit != request.outcome_commit
        or task.workspace.base_commit != request.outcome_commit
    ):
        raise ValueError("reflection memory snapshot is not bound to committed outcome")
    closure.request.memory_snapshot.CopyFrom(request.memory_snapshot)
    if closure.request.incident_id != outcome.incident_id:
        raise ValueError("reflection outcome incident does not match closure")
    session_mode = request.session_mode or "fresh"
    if session_mode not in {"fresh", "resume"}:
        raise ValueError(f"invalid reflection session mode {session_mode!r}")
    prior_session = closure.result.responder_session_id if closure.HasField("result") else ""
    if session_mode == "resume" and not _SESSION_TOKEN.fullmatch(prior_session or ""):
        print("reflection: responder session ID is missing or invalid; starting fresh", file=sys.stderr)
        session_mode = "fresh"
    guidance = request.guidance or "baseline"
    if guidance not in REFLECTION_GUIDANCE_MODES:
        raise ValueError(f"invalid reflection guidance {guidance!r}")
    depth = request.depth or "full"
    if depth not in REFLECTION_DEPTHS:
        raise ValueError(f"invalid reflection depth {depth!r}")
    # The task, rather than inherited launcher environment, pins check ownership.
    environment = {
        "SDO_ACTOR": "responder",
        "SDO_PHASE": "reflection",
        "SDO_BASE_COMMIT": task.workspace.base_commit,
        "SDO_INCIDENT_ID": outcome.incident_id,
        "SDO_CAPTURED_CORPUS_DIR": os.environ.get("SDO_CAPTURED_CORPUS_DIR", "/sdo/session/corpus"),
    }
    previous = {key: os.environ.get(key) for key in environment}
    selected = reflector or _default_reflector(cast("ReflectionGuidance", guidance))
    try:
        os.environ.update(environment)
        turn = selected.resume(
            session_id=prior_session or "",
            incident_id=outcome.incident_id,
            worktree=Path(task.workspace.checkout_path),
            outcome=outcome,
            history=history,
            outcome_commit=request.outcome_commit,
            session_mode=cast("ReflectionSessionMode", session_mode),
            closure=closure,
            validation_attempt=request.validation_attempt or 1,
            diagnostics=list(request.previous_diagnostics),
            depth=cast("ReflectionDepth", depth),
        )
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    typed_result = ReflectionResult(
        summary=turn.summary,
        learning_decision=turn.learning_decision,
        no_change_reason=turn.no_change_reason or "",
        proposed_artifact_ids=turn.playbook_ids,
        sdk_gaps=[json_format.ParseDict(gap.model_dump(), SdkGap()) for gap in turn.sdk_gaps],
    )
    result = AgentResult(
        schema_version=SCHEMA_VERSION,
        task_id=task.task_id,
        status="completed",
        proposal_id=task.workspace.proposal_id,
    )
    result.response.Pack(typed_result)
    payload = to_canonical_json(result)
    if len(payload.encode("utf-8")) > 700 << 10:
        raise ValueError(
            "reflection result exceeds the worker transport budget; "
            "use corpus references and reduce proposal source size"
        )
    publisher.publish_payload(result_name, "agent-result.json", payload)
    return result


def main() -> int:
    namespace = os.environ.get("SDO_NAMESPACE", "")
    result_name = os.environ.get("SDO_RESULT_CONFIGMAP", "")
    if not namespace or not result_name:
        print("SDO_NAMESPACE and SDO_RESULT_CONFIGMAP are required", file=sys.stderr)
        return 2
    try:
        prepare_codex_home(skills=AGENT_SKILLS["reflector"])
        prepare_claude_home(skills=AGENT_SKILLS["reflector"])
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
