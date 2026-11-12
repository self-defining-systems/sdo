"""Closing-turn worker behind the implementation-neutral task contract.

The controller dispatches it twice over an incident's life, always into the responder's own session: once to clean up
the app edits the agent left (a cleanup turn, in the incident's proposal checkout, before the source repair is
accepted) and once, when the incident earned it, to turn the fix into a playbook (the learning turn, in a checkout of the
outcome commit).
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from libs.agent_cli.errors import rate_limit_failure
from libs.agent_cli.session_ownership import SessionHandoffError, SessionHandoffTimeout
from libs.agent_cli.structured import StructuredTurnTimeout
from sdo.agents.provider import resolve_agent
from sdo.agents.responder.credentials import prepare_claude_home, prepare_codex_home, skills_for_role
from sdo.agents.responder.job import KubernetesResultPublisher, export_proxy_kubeconfig
from sdo.agents.responder.memory_check import MemoryCheckTimeout
from sdo.agents.responder.reflection import (
    REFLECTION_DEADLINE_ENV,
    ClaudeSessionBackend,
    CodexSessionBackend,
    ReflectionDeadlineExceeded,
    ReflectionTurn,
    ResponderSessionUnavailable,
    SessionReflector,
)
from sdo.agents.responder.termination import record_worker_failure, records_worker_failure
from sdo.contracts import (
    SCHEMA_VERSION,
    AgentResult,
    AgentTask,
    ReflectionRequest,
    ReflectionResult,
    parse_json,
    to_canonical_json,
    validate,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sdo.contracts import IncidentClosure

AGENT_ID = "sdo-reflector"
TASK_TYPE = "outcome-reflection-direct-v1"
DEFAULT_TASK_PATH = Path("/sdo/request/agent-task.json")
_SESSION_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
RESULT_BUDGET_BYTES = 700 << 10


class ResultPublisher(Protocol):
    def publish_payload(self, name: str, key: str, payload: str) -> None: ...


class Reflector(Protocol):
    def learn(
        self,
        *,
        session_id: str,
        incident_id: str,
        outcome_commit: str,
        worktree: Path,
        changes: Sequence[str],
        closure: IncidentClosure,
    ) -> ReflectionTurn: ...

    def cleanup(
        self, *, session_id: str, incident_id: str, worktree: Path, leftovers: Sequence[str], turn: int
    ) -> ReflectionTurn: ...


def _default_reflector() -> SessionReflector:
    resolved = resolve_agent("reflection", "reflection")
    backend = (ClaudeSessionBackend if resolved.provider == "claude" else CodexSessionBackend)(
        model=resolved.model, reasoning_effort=resolved.effort
    )
    return SessionReflector(backend)


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
    if not task.workspace.proposal_id or not task.workspace.checkout_path or not task.workspace.base_commit:
        raise ValueError("reflection task requires a proposal workspace")
    cleanup = request.cleanup_turn > 0
    incident_id = request.closure.request.incident_id
    if not cleanup:
        if not request.HasField("outcome") or not request.outcome_commit:
            raise ValueError("reflection task requires an outcome and outcome commit")
        if not request.HasField("memory_snapshot"):
            raise ValueError("reflection task requires a logical memory snapshot")
        incident_id = request.outcome.incident_id
        if request.closure.request.incident_id != incident_id:
            raise ValueError("reflection outcome incident does not match closure")
        if not _binds_committed_outcome(
            Path(task.workspace.checkout_path),
            outcome_commit=request.outcome_commit,
            base_commit=task.workspace.base_commit,
            snapshot_commit=request.memory_snapshot.accepted_commit,
        ):
            raise ValueError("reflection memory snapshot is not bound to committed outcome")
    # The closing turn always continues the responder's session; a closure without one is never dispatched.
    session_id = request.closure.result.responder_session_id if request.closure.HasField("result") else ""
    # The task, rather than inherited launcher environment, pins check ownership.
    environment = {
        "SDO_ACTOR": "responder",
        "SDO_PHASE": "proposal" if cleanup else "reflection",
        "SDO_BASE_COMMIT": task.workspace.base_commit,
        "SDO_INCIDENT_ID": incident_id,
        "SDO_CAPTURED_CORPUS_DIR": os.environ.get("SDO_CAPTURED_CORPUS_DIR", "/sdo/session/corpus"),
    }
    if task.HasField("deadline"):
        environment[REFLECTION_DEADLINE_ENV] = repr(task.deadline.ToNanoseconds() / 1e9)
    previous = {key: os.environ.get(key) for key in environment}
    turn: ReflectionTurn | None = None
    failure: tuple[str, str] | None = None
    try:
        os.environ.update(environment)
        if not _SESSION_TOKEN.fullmatch(session_id):
            raise ResponderSessionUnavailable("closing turn responder session ID is missing or invalid")
        if reflector is None and not os.environ.get("SDO_SESSION_DIR"):
            raise SessionHandoffError("the closing turn requires the durable incident session directory")
        selected = reflector or _default_reflector()
        worktree = Path(task.workspace.checkout_path)
        if cleanup:
            turn = selected.cleanup(
                session_id=session_id,
                incident_id=incident_id,
                worktree=worktree,
                leftovers=list(request.leftovers),
                turn=request.cleanup_turn,
            )
        else:
            turn = selected.learn(
                session_id=session_id,
                incident_id=incident_id,
                outcome_commit=request.outcome_commit,
                worktree=worktree,
                changes=list(request.changes),
                closure=request.closure,
            )
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
        result.response.Pack(
            ReflectionResult(summary=turn.note, learning_decision="no_change", no_change_reason=turn.note)
            if cleanup
            else turn.to_result(Path(task.workspace.checkout_path))
        )
        if len(to_canonical_json(result).encode("utf-8")) > RESULT_BUDGET_BYTES:
            failure = ("result_too_large", "closing turn result exceeds the worker transport budget")
    if failure is not None:
        result.ClearField("response")
        result.status = "failed"
        result.failure_reason, summary = failure
        result.error_summary = summary[:4096] or result.failure_reason
        print(f"closing turn failed ({result.failure_reason}): {summary}", file=sys.stderr)
    publisher.publish_payload(result_name, "agent-result.json", to_canonical_json(result))
    return result


def _failure_reason(exc: BaseException) -> str:
    if limited := rate_limit_failure(exc):
        return limited.failure_reason
    cause: BaseException | None = exc
    while cause is not None:
        if isinstance(cause, MemoryCheckTimeout):
            return "agent_timeout"
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
        export_proxy_kubeconfig(namespace)
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
