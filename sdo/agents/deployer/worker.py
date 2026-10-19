"""Typed deployer workers. Go owns build, apply, verification, and acceptance."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field

from libs.agent_cli.structured import run_structured_turn
from sdo.agents.lifecycle import (
    ClaudeLifecycleBackend,
    CodexLifecycleBackend,
    DeployerAssessment,
    TopologyResourceDTO,
)
from sdo.agents.responder import KubernetesResultPublisher, prepare_claude_home, prepare_codex_home
from sdo.contracts import (
    SCHEMA_VERSION,
    AgentResult,
    AgentTask,
    ArchitectureRequest,
    ArchitectureResult,
    DeploymentPlan,
    ImageBuild,
    RawManifestStrategy,
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


class ImageBuildDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    image: str = Field(min_length=1)
    context_path: str = Field(min_length=1)
    dockerfile_path: str = Field(min_length=1)


class SourceProposalDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    proposed: bool
    summary: str = Field(min_length=1)
    image_builds: list[ImageBuildDraft]
    manifest_paths: list[str]
    expected_workload_ids: list[str]


def _provider() -> Literal["codex", "claude"]:
    provider = os.environ.get("SDO_AGENT_PROVIDER", "codex").strip().lower()
    if provider == "codex":
        return "codex"
    if provider == "claude":
        return "claude"
    raise ValueError(f"unsupported deployment provider {provider!r}")


def _source_path(value: str) -> str:
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts or (path.parts and path.parts[0] == ".sdo"):
        raise ValueError(f"deployment plan path must stay in application source: {value!r}")
    return path.as_posix()


def _trusted_image(image: str, registry_host: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]*:[1-9][0-9]{0,4}", registry_host):
        raise ValueError("source deployment requires a trusted registry host")
    if not image.startswith(registry_host + "/") or image == registry_host + "/":
        raise ValueError(f"image build target must use trusted registry {registry_host!r}")
    return image


def _workload_id(value: str) -> str:
    parts = value.split("/")
    if len(parts) != 3 or any(not part or part.strip() != part for part in parts):
        raise ValueError(f"expected workload ID must be Kind/namespace/name: {value!r}")
    return value


def propose_source(request: SourceDeploymentRequest, checkout: Path) -> SourceDeploymentResult:
    """Let the built-in coding agent edit a proposal and return a constrained plan."""

    provider = _provider()
    prompt = f"""You are the SDO source deployer for application {request.application!r}.
Inspect this proposal checkout and prepare source/manifests to run in Kubernetes namespace {request.namespace!r}.
The human health objective is: {request.health_objective}
Feedback from the prior independently verified attempt: {request.correction_feedback or "none"}.
The operator's trusted image registry is {request.registry_host!r}; every image build target must start with
{request.registry_host + "/"!r}. Use an image path under that host in manifests as well.

Edit only application source and conventional deployment files. Do not edit .sdo/. Do not run kubectl apply,
build/push images, or claim the deployment is healthy. A trusted Go adapter will build the exact candidate source,
apply manifests, and independently verify live resources after your task ends. Return a plan containing image builds
(image name, repository-relative context and Dockerfile paths), repository-relative manifest paths, and expected
workload IDs as Kind/namespace/name. Every plan path must be inside this checkout. Set proposed=true only when the
files and plan are ready for that independent adapter. Do not create a separate health-check script or inspect
benchmark data.
"""
    turn = run_structured_turn(
        provider,
        prompt,
        output_schema=SourceProposalDraft.model_json_schema(),
        cwd=checkout,
        access="workspace-write",
        model=os.environ.get("SDO_DEPLOYMENT_MODEL") or None,
        reasoning_effort="high",
        timeout_seconds=1800,
    )
    draft = SourceProposalDraft.model_validate_json(turn.output_json)
    raw_strategy = RawManifestStrategy(
        image_builds=[
            ImageBuild(
                image=_trusted_image(build.image, request.registry_host),
                context_path=_source_path(build.context_path),
                dockerfile_path=_source_path(build.dockerfile_path),
            )
            for build in draft.image_builds
        ],
        manifest_paths=[_source_path(path) for path in draft.manifest_paths],
        expected_workload_ids=[_workload_id(value) for value in draft.expected_workload_ids],
    )
    if draft.proposed and (not raw_strategy.manifest_paths or not raw_strategy.expected_workload_ids):
        raise ValueError("deployment proposal requires manifests and expected workloads")
    plan = DeploymentPlan(strategy_id="raw-manifests-v1")
    plan.strategy_config.Pack(raw_strategy)
    return SourceDeploymentResult(
        proposed=draft.proposed,
        summary=draft.summary,
        agent_session_id=turn.session_id,
        plan=plan,
    )


def describe_architecture(request: ArchitectureRequest, checkout: Path) -> ArchitectureResult:
    """Generate a source-grounded handoff after the broker accepted source."""

    provider = _provider()
    if provider == "codex":
        architect = CodexLifecycleBackend(model=os.environ.get("SDO_LIFECYCLE_MODEL") or None)
    else:
        architect = ClaudeLifecycleBackend(model=os.environ.get("SDO_LIFECYCLE_MODEL") or None)
    if not request.source_topology_fingerprint or not request.source_resources:
        raise ValueError("architecture requires trusted accepted-source inventory")
    handoff = architect.run_deployer(
        repository=checkout,
        application=request.application,
        correction_feedback=(
            "Copy the trusted source commit and topology fingerprint exactly:\n"
            + f"{request.accepted_source_commit}\n{request.source_topology_fingerprint}\n"
            + request.correction_feedback
        ),
    )
    inventory = [
        TopologyResourceDTO(
            kind=row.kind,
            name=row.name,
            namespace=row.namespace,
            source=row.source,
            dependencies=list(row.dependencies),
        )
        for row in request.source_resources
    ]
    assessment = DeployerAssessment(**handoff.model_dump(), resources=inventory)
    if assessment.source_commit != request.accepted_source_commit:
        raise ValueError("architecture source commit does not match the broker accepted commit")
    if assessment.topology_fingerprint != request.source_topology_fingerprint:
        raise ValueError("architecture topology fingerprint does not match trusted source inventory")
    summary = assessment.architecture_summary_markdown.lower()
    missing = sorted({resource.name for resource in inventory if resource.name.lower() not in summary})
    if missing:
        raise ValueError("architecture summary omits source-backed resources: " + ", ".join(missing))
    return ArchitectureResult(
        summary_markdown=assessment.architecture_summary_markdown,
        topology_fingerprint=assessment.topology_fingerprint,
        source_commit=assessment.source_commit,
        agent_session_id=assessment.session_id,
    )


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
    if (
        not request.application
        or not request.namespace
        or not request.health_objective
        or request.attempt < 1
        or not request.registry_host
    ):
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


def run_architecture_task(
    task_path: Path,
    result_name: str,
    publisher: ResultPublisher,
    *,
    execute: Callable[[ArchitectureRequest, Path], ArchitectureResult] = describe_architecture,
) -> AgentResult:
    task = cast("AgentTask", parse_json(task_path.read_text(encoding="utf-8"), AgentTask()))
    validate(task)
    if not task.workspace.proposal_id or not task.workspace.checkout_path:
        raise ValueError("architecture requires a source proposal workspace")
    request = ArchitectureRequest()
    if not task.request.Unpack(request):
        raise ValueError("architecture request has wrong type")
    if not request.application or request.accepted_source_commit != task.workspace.base_commit or request.attempt < 1:
        raise ValueError("architecture request does not match accepted source")
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


def main() -> int:
    namespace = os.environ.get("SDO_NAMESPACE", "")
    result_name = os.environ.get("SDO_RESULT_CONFIGMAP", "")
    if not namespace or not result_name:
        print("SDO_NAMESPACE and SDO_RESULT_CONFIGMAP are required", file=sys.stderr)
        return 2
    try:
        prepare_codex_home()
        prepare_claude_home()
        task_path = Path(os.environ.get("SDO_TASK_PATH", str(DEFAULT_TASK_PATH)))
        task = cast("AgentTask", parse_json(task_path.read_text(encoding="utf-8"), AgentTask()))
        publisher = KubernetesResultPublisher(namespace=namespace)
        if task.request.Is(SourceDeploymentRequest.DESCRIPTOR):
            run_deployment_task(task_path, result_name, publisher)
        elif task.request.Is(ArchitectureRequest.DESCRIPTOR):
            run_architecture_task(task_path, result_name, publisher)
        else:
            raise ValueError(f"unsupported deployer request type {task.request.TypeName()!r}")
    except (OSError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
