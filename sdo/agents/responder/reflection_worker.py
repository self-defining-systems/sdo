"""Reflection worker behind the implementation-neutral task contract."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from libs.agent_cli.session_ownership import SessionHandoffError, SessionHandoffTimeout
from libs.agent_cli.structured import StructuredTurnTimeout
from sdo.agents.provider import resolve_agent
from sdo.agents.responder.credentials import prepare_claude_home, prepare_codex_home, skills_for_role
from sdo.agents.responder.job import KubernetesResultPublisher
from sdo.agents.responder.reflection import (
    REFLECTION_DEADLINE_ENV,
    REFLECTION_GUIDANCE_MODES,
    ClaudeSessionBackend,
    CodexSessionBackend,
    ReflectionDeadlineExceeded,
    ReflectionGuidance,
    ReflectionSessionMode,
    ReflectionTurn,
    ResponderSessionUnavailable,
    SessionReflector,
)
from sdo.agents.responder.termination import record_worker_failure, records_worker_failure
from sdo.contracts import (
    SCHEMA_VERSION,
    AgentResult,
    AgentTask,
    IncidentClosure,
    MemoryDiagnostic,
    OutcomeRecord,
    ReflectionCauseWork,
    ReflectionRequest,
    ReflectionWorkItem,
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
RESULT_BUDGET_BYTES = 700 << 10


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
        cause_work: Sequence[ReflectionCauseWork] = (),
        unassigned_work: Sequence[ReflectionWorkItem] = (),
    ) -> ReflectionTurn: ...


def _default_reflector(guidance: ReflectionGuidance) -> SessionReflector:
    resolved = resolve_agent("reflection", "reflection")
    backend = (ClaudeSessionBackend if resolved.provider == "claude" else CodexSessionBackend)(model=resolved.model)
    return SessionReflector(backend, guidance=guidance)


def _binds_committed_outcome(checkout: Path, *, outcome_commit: str, base_commit: str, snapshot_commit: str) -> bool:
    """The snapshot describes the workspace base, and the base contains the incident's outcome.

    A memory-conflict retry rebuilds the workspace on the current accepted head, which peers sharing
    the memory may have advanced past the outcome commit.
    """
    if snapshot_commit != base_commit:
        return False
    if base_commit == outcome_commit:
        return True
    ancestry = subprocess.run(
        ["git", "-C", str(checkout), "merge-base", "--is-ancestor", outcome_commit, base_commit],
        capture_output=True,
        check=False,
    )
    return ancestry.returncode == 0


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
    if closure.HasField("result"):
        # The request carries the responder's raw evidence once, in the outcome derived from it.
        if not closure.result.program_runs:
            closure.result.program_runs.extend(outcome.program_runs)
        if not closure.result.program_tool_calls:
            closure.result.program_tool_calls.extend(outcome.program_tool_calls)
        if not closure.result.plan_applications:
            closure.result.plan_applications.extend(outcome.plan_applications)
    if not _binds_committed_outcome(
        Path(task.workspace.checkout_path),
        outcome_commit=request.outcome_commit,
        base_commit=task.workspace.base_commit,
        snapshot_commit=request.memory_snapshot.accepted_commit,
    ):
        raise ValueError("reflection memory snapshot is not bound to committed outcome")
    closure.request.memory_snapshot.CopyFrom(request.memory_snapshot)
    if closure.request.incident_id != outcome.incident_id:
        raise ValueError("reflection outcome incident does not match closure")
    session_mode = request.session_mode or "fresh"
    if session_mode not in {"fresh", "resume"}:
        raise ValueError(f"invalid reflection session mode {session_mode!r}")
    prior_session = closure.result.responder_session_id if closure.HasField("result") else ""
    invalid_session = session_mode == "resume" and not _SESSION_TOKEN.fullmatch(prior_session or "")
    guidance = request.guidance or "baseline"
    if guidance not in REFLECTION_GUIDANCE_MODES:
        raise ValueError(f"invalid reflection guidance {guidance!r}")
    # The task, rather than inherited launcher environment, pins check ownership.
    environment = {
        "SDO_ACTOR": "responder",
        "SDO_PHASE": "reflection",
        "SDO_BASE_COMMIT": task.workspace.base_commit,
        "SDO_INCIDENT_ID": outcome.incident_id,
        "SDO_CAPTURED_CORPUS_DIR": os.environ.get("SDO_CAPTURED_CORPUS_DIR", "/sdo/session/corpus"),
    }
    if task.HasField("deadline"):
        environment[REFLECTION_DEADLINE_ENV] = repr(task.deadline.ToNanoseconds() / 1e9)
    previous = {key: os.environ.get(key) for key in environment}
    selected = reflector or _default_reflector(cast("ReflectionGuidance", guidance))
    try:
        os.environ.update(environment)
        if invalid_session:
            raise ResponderSessionUnavailable("reflection responder session ID is missing or invalid")
        if reflector is None and session_mode == "resume" and not os.environ.get("SDO_SESSION_DIR"):
            raise SessionHandoffError("reflection resume requires the durable incident session directory")
        turn: ReflectionTurn | None = selected.resume(
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
            cause_work=list(request.cause_work),
            unassigned_work=list(request.unassigned_work),
        )
        failure: tuple[str, str] | None = None
    except (OSError, ValueError, RuntimeError) as exc:
        # A failed turn is a typed outcome for the controller, not a failed Job.
        turn, failure = None, (_failure_reason(exc), str(exc))
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    result = AgentResult(
        schema_version=SCHEMA_VERSION,
        task_id=task.task_id,
        status="completed",
        proposal_id=task.workspace.proposal_id,
    )
    if turn is not None:
        result.response.Pack(turn.to_result())
        if len(to_canonical_json(result).encode("utf-8")) > RESULT_BUDGET_BYTES:
            failure = (
                "result_too_large",
                "reflection result exceeds the worker transport budget; "
                "use corpus references and reduce proposal source size",
            )
    if failure is not None:
        result.ClearField("response")
        result.status = "failed"
        result.failure_reason, summary = failure
        result.error_summary = summary[:4096] or result.failure_reason
        print(f"reflection failed ({result.failure_reason}): {summary}", file=sys.stderr)
    publisher.publish_payload(result_name, "agent-result.json", to_canonical_json(result))
    return result


def _failure_reason(exc: BaseException) -> str:
    cause: BaseException | None = exc
    while cause is not None:
        if isinstance(cause, SessionHandoffTimeout):
            return "session_handoff_timeout"
        if isinstance(cause, SessionHandoffError):
            return "session_handoff_invalid"
        if isinstance(cause, ResponderSessionUnavailable):
            return "session_unavailable"
        if isinstance(cause, (StructuredTurnTimeout, ReflectionDeadlineExceeded)):
            return "agent_timeout"
        cause = cause.__cause__ or cause.__context__
    return "agent_failed"


@records_worker_failure
def main() -> int:
    namespace = os.environ.get("SDO_NAMESPACE", "")
    result_name = os.environ.get("SDO_RESULT_CONFIGMAP", "")
    if not namespace or not result_name:
        print("SDO_NAMESPACE and SDO_RESULT_CONFIGMAP are required", file=sys.stderr)
        record_worker_failure(ValueError("SDO_NAMESPACE and SDO_RESULT_CONFIGMAP are required"))
        return 2
    try:
        prepare_codex_home(skills=skills_for_role("reflector"))
        prepare_claude_home(skills=skills_for_role("reflector"))
        run_reflection_task(
            Path(os.environ.get("SDO_TASK_PATH", str(DEFAULT_TASK_PATH))),
            result_name,
            KubernetesResultPublisher(namespace=namespace),
        )
    except (OSError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        record_worker_failure(exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
