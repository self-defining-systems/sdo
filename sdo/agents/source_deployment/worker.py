"""Source deployment worker: the agent commits deployment files; Go builds, deploys, verifies and accepts."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field

from sdo.agents.prompt_templates import named_prompt, render_prompt
from sdo.agents.prompt_templates import run_agent_turn as run_structured_turn
from sdo.agents.provider import ResolvedAgent, resolve_agent
from sdo.agents.responder import (
    AGENT_SKILLS,
    KubernetesResultPublisher,
    export_proxy_kubeconfig,
    prepare_claude_home,
    prepare_codex_home,
    record_worker_failure,
    records_worker_failure,
)
from sdo.contracts import (
    SCHEMA_VERSION,
    AgentResult,
    AgentTask,
    SourceDeploymentRequest,
    SourceDeploymentResult,
    parse_json,
    to_canonical_json,
    validate,
)

if TYPE_CHECKING:
    from collections.abc import Callable

DEFAULT_TASK_PATH = Path("/sdo/request/agent-task.json")


class ResultPublisher(Protocol):
    def publish_payload(self, name: str, key: str, payload: str) -> None: ...


class SourceProposalDraft(BaseModel):
    """The agent's reply: its work is the checkout's last commit, not the reply."""

    model_config = ConfigDict(extra="forbid")

    proposed: bool
    summary: str = Field(min_length=1)


def _provider(agent: ResolvedAgent) -> Literal["codex", "claude"]:
    return "claude" if agent.provider == "claude" else "codex"


def propose_source(request: SourceDeploymentRequest, checkout: Path) -> SourceDeploymentResult:
    """Let the built-in coding agent commit deployment files in its proposal checkout."""

    agent = resolve_agent("source_deployment", "deployment")
    prompt = render_prompt(
        "source_deployment",
        "source_deployment",
        part="source_deployment.instructions",
        application=repr(request.application),
        namespace=repr(request.namespace),
        # Producer text is unbounded: it stays inline when it fits and spills to a pointed-at file otherwise.
        health_objective=named_prompt("source_deployment.health_objective", request.health_objective, required=False),
        correction_feedback=named_prompt(
            "source_deployment.correction_feedback", request.correction_feedback or "none", required=False
        ),
        registry_host=repr(request.registry_host),
        registry_host_2=repr(request.registry_host + "/"),
    )
    turn = run_structured_turn(
        _provider(agent),
        prompt,
        output_schema=SourceProposalDraft.model_json_schema(),
        cwd=checkout,
        access="danger-full-access",
        model=agent.model,
        reasoning_effort=agent.effort,
        timeout_seconds=1800,
    )
    draft = SourceProposalDraft.model_validate_json(turn.output_json)
    return SourceDeploymentResult(proposed=draft.proposed, summary=draft.summary, agent_session_id=turn.session_id)


def run_deployment_task(
    task_path: Path,
    result_name: str,
    publisher: ResultPublisher,
    *,
    execute: Callable[[SourceDeploymentRequest, Path], SourceDeploymentResult] = propose_source,
) -> AgentResult:
    task = cast("AgentTask", parse_json(task_path.read_text(encoding="utf-8"), AgentTask()))
    validate(task)
    if not task.workspace.proposal_id or not task.workspace.checkout_path:
        raise ValueError("source deployment requires a proposal checkout")
    request = SourceDeploymentRequest()
    if not task.request.Unpack(request):
        raise ValueError("source deployment request has wrong type")
    if not request.application or not request.namespace or request.attempt < 1:
        raise ValueError("source deployment request is incomplete")
    typed = execute(request, Path(task.workspace.checkout_path))
    result = AgentResult(
        schema_version=SCHEMA_VERSION,
        task_id=task.task_id,
        status="completed",
        proposal_id=task.workspace.proposal_id,
    )
    result.response.Pack(typed)
    publisher.publish_payload(result_name, "agent-result.json", to_canonical_json(result))
    return result


@records_worker_failure
def main() -> int:
    namespace = os.environ.get("SDO_NAMESPACE", "")
    result_name = os.environ.get("SDO_RESULT_CONFIGMAP", "")
    if not namespace or not result_name:
        print("SDO_NAMESPACE and SDO_RESULT_CONFIGMAP are required", file=sys.stderr)
        record_worker_failure(ValueError("SDO_NAMESPACE and SDO_RESULT_CONFIGMAP are required"))
        return 2
    try:
        prepare_codex_home(skills=AGENT_SKILLS["source_deployment"])
        prepare_claude_home(skills=AGENT_SKILLS["source_deployment"])
        task_path = Path(os.environ.get("SDO_TASK_PATH", str(DEFAULT_TASK_PATH)))
        task = cast("AgentTask", parse_json(task_path.read_text(encoding="utf-8"), AgentTask()))
        export_proxy_kubeconfig(namespace)
        publisher = KubernetesResultPublisher(namespace=namespace)
        if task.request.Is(SourceDeploymentRequest.DESCRIPTOR):
            run_deployment_task(task_path, result_name, publisher)
        else:
            raise ValueError(f"unsupported source deployment request type {task.request.TypeName()!r}")
    except (OSError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        record_worker_failure(exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
