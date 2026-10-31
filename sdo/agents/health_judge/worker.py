"""Typed health judge worker for a role-scoped proposal checkout."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

from sdo.agents.lifecycle import (
    ClaudeLifecycleBackend,
    CodexLifecycleBackend,
    TopologyResourceDTO,
)
from sdo.agents.memory_diagnostics import render_validation_feedback
from sdo.agents.provider import resolve_agent
from sdo.agents.responder import KubernetesResultPublisher, prepare_claude_home, prepare_codex_home
from sdo.contracts import (
    SCHEMA_VERSION,
    AgentResult,
    AgentTask,
    HealthJudgeRequest,
    HealthJudgeResult,
    parse_json,
    to_canonical_json,
    validate,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

DEFAULT_TASK_PATH = Path("/sdo/request/agent-task.json")


class ResultPublisher(Protocol):
    def publish_payload(self, name: str, key: str, payload: str) -> None: ...


class _JudgeDraft(Protocol):
    @property
    def failure_patterns(self) -> Sequence[str]: ...

    @property
    def detector_ids(self) -> Sequence[str]: ...


def author_health_diagnostics(request: HealthJudgeRequest, checkout: Path) -> HealthJudgeResult:
    """Ask a fresh judge to author deterministic diagnostics in the proposal."""

    summary = request.architecture_summary_markdown.strip()
    if not summary:
        raise ValueError("health judge requires accepted architecture summary")
    resources = [
        TopologyResourceDTO(
            kind=row.kind,
            name=row.name,
            namespace=row.namespace,
            source=row.source,
            dependencies=list(row.dependencies),
        )
        for row in request.source_resources
    ]
    resolved = resolve_agent("health_judge", "SDO_LIFECYCLE_MODEL")
    backend = (ClaudeLifecycleBackend if resolved.provider == "claude" else CodexLifecycleBackend)(model=resolved.model)
    artifact = backend.run_health_judge(
        repository=checkout,
        application=request.application,
        health_objective=request.health_objective,
        architecture=summary,
        resources=resources,
        correction_feedback=_judge_feedback(request),
    )
    _validate_judge_draft(artifact)
    return HealthJudgeResult(
        authored=True,
        summary="health diagnostics authored",
        agent_session_id=artifact.session_id,
        detector_ids=list(artifact.detector_ids),
    )


def _judge_feedback(request: HealthJudgeRequest) -> str | None:
    """Whole-attempt verifier feedback plus Go's typed broker rejection diagnostics."""

    parts = [
        request.correction_feedback,
        render_validation_feedback(request.validation_attempt or 1, request.previous_diagnostics),
    ]
    return "\n".join(part for part in parts if part) or None


def _validate_judge_draft(artifact: _JudgeDraft) -> None:
    """Check logical detector metadata before authoritative Go validation."""
    if not artifact.failure_patterns:
        raise ValueError("health judge must enumerate failure patterns")
    if not artifact.detector_ids or any(
        not re.fullmatch(r"[a-z0-9][a-z0-9_.-]*", item) for item in artifact.detector_ids
    ):
        raise ValueError("health judge must return logical detector IDs")
    if len(set(artifact.detector_ids)) != len(artifact.detector_ids):
        raise ValueError("health judge detector IDs must be unique")


def run_judge_task(
    task_path: Path,
    result_name: str,
    publisher: ResultPublisher,
    *,
    execute: Callable[[HealthJudgeRequest, Path], HealthJudgeResult] = author_health_diagnostics,
) -> AgentResult:
    task = cast("AgentTask", parse_json(task_path.read_text(encoding="utf-8"), AgentTask()))
    validate(task)
    if not task.workspace.proposal_id or not task.workspace.checkout_path or not task.workspace.base_commit:
        raise ValueError("health judge requires an accepted proposal checkout")
    request = HealthJudgeRequest()
    if not task.request.Unpack(request):
        raise ValueError("health judge request has wrong type")
    if not request.application or not request.namespace or request.attempt < 1:
        raise ValueError("health judge request is incomplete")
    checkout = Path(task.workspace.checkout_path)
    if not request.accepted_source_commit or not request.architecture_summary_markdown:
        raise ValueError("health judge requires accepted source and architecture context")
    if any(not (row.kind and row.name and row.namespace and row.source) for row in request.source_resources):
        raise ValueError("health judge received an incomplete source inventory resource")
    typed = execute(request, checkout)
    if typed.HasField("artifact_proposal"):
        raise ValueError("direct-edit health judge rejects deprecated artifact_proposal")
    validate(typed)
    if typed.authored and not typed.detector_ids:
        raise ValueError("health judge must return logical detector IDs")
    result = AgentResult(
        schema_version=SCHEMA_VERSION,
        task_id=task.task_id,
        status="completed",
        proposal_id=task.workspace.proposal_id,
    )
    result.response.Pack(typed)
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
        run_judge_task(
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
