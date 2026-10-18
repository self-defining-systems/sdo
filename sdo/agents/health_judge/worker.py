"""Typed health judge worker for a role-scoped proposal checkout."""

from __future__ import annotations

import hashlib
import os
import re
import sys
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Protocol

import yaml
from google.protobuf import json_format

from sdo.agents.lifecycle import (
    HEALTH_OBJECTIVE_WATCHES,
    ClaudeLifecycleBackend,
    CodexLifecycleBackend,
    DeployerAssessment,
    TopologyResourceDTO,
)
from sdo.agents.responder import KubernetesResultPublisher, prepare_claude_home, prepare_codex_home
from sdo.contracts import (
    AgentResult,
    AgentTask,
    HealthJudgeRequest,
    HealthJudgeResult,
    OperationalArtifactProposal,
    ProposedDetector,
    ProposedGoSource,
    ProposedPlaybook,
    ProposedTrafficGenerator,
    ProtoMemoryWatch,
    ProtoTrafficWorkload,
    parse_json,
    to_canonical_json,
)

if TYPE_CHECKING:
    from collections.abc import Callable

TASK_TYPE = "health-judgment"
DEFAULT_TASK_PATH = Path("/sdo/request/agent-task.json")


class ResultPublisher(Protocol):
    def publish_payload(self, name: str, key: str, payload: str) -> None: ...


def author_health_diagnostics(request: HealthJudgeRequest, checkout: Path) -> HealthJudgeResult:
    """Ask a fresh judge to author deterministic diagnostics in the proposal."""

    summary = request.architecture_summary_markdown.strip()
    if not summary:
        raise ValueError("health judge requires accepted architecture summary")
    deployer = DeployerAssessment(
        source_commit=request.accepted_source_commit,
        topology_fingerprint=request.source_topology_fingerprint,
        architecture_summary_markdown=summary,
        session_id="accepted-source",
        resources=[
            TopologyResourceDTO(
                kind=row.kind,
                name=row.name,
                namespace=row.namespace,
                source=row.source,
                dependencies=list(row.dependencies),
            )
            for row in request.source_resources
        ],
    )
    provider = os.environ.get("SDO_AGENT_PROVIDER", "codex").strip().lower()
    if provider == "codex":
        backend = CodexLifecycleBackend(model=os.environ.get("SDO_LIFECYCLE_MODEL") or None)
    elif provider == "claude":
        backend = ClaudeLifecycleBackend(model=os.environ.get("SDO_LIFECYCLE_MODEL") or None)
    else:
        raise ValueError(f"unsupported health judge provider {provider!r}")
    artifact = backend.run_health_judge(
        repository=checkout,
        application=request.application,
        health_objective=request.health_objective,
        deployer=deployer,
        round_index=1,
        previous=None,
        correction_feedback=request.correction_feedback or None,
    )
    _validate_judge_draft(
        artifact, health_objective=request.health_objective, source_commit=request.accepted_source_commit
    )
    proposal = _judge_proposal(artifact)
    return HealthJudgeResult(
        authored=True,
        summary="health diagnostics authored",
        agent_session_id=artifact.session_id,
        objective_digest=artifact.objective_digest,
        detector_ids=["health-objective"],
        artifact_proposal=proposal,
    )


def _validate_judge_draft(artifact: object, *, health_objective: str, source_commit: str) -> None:
    """Check agent self-consistency before the Go broker's authoritative validation."""

    expected_digest = hashlib.sha256(health_objective.strip().encode()).hexdigest()
    if artifact.objective_digest != expected_digest:
        raise ValueError("health judge objective digest does not match human objective")
    if artifact.source_commit != source_commit:
        raise ValueError("health judge source commit does not match accepted source")
    if artifact.round != 1:
        raise ValueError("health judge must return the requested authoring round")
    if not artifact.failure_patterns:
        raise ValueError("health judge must enumerate failure patterns")
    if "package objective" not in artifact.detector_source or "func New() sdk.Detector" not in artifact.detector_source:
        raise ValueError("health judge must return complete objective detector source")
    if "package objective" not in artifact.detector_test_source or "func Test" not in artifact.detector_test_source:
        raise ValueError("health judge must return deterministic objective detector tests")


def _judge_proposal(artifact: object) -> OperationalArtifactProposal:
    """Convert an agent-authored draft to logical artifacts for the Go mapper."""

    proposal = OperationalArtifactProposal()
    proposal.detectors.append(
        ProposedDetector(
            id="health-objective",
            detector_class="health",
            owner="health_judge",
            constructor="New",
            watches=[ProtoMemoryWatch(api_version=api, kind=kind) for api, kind in HEALTH_OBJECTIVE_WATCHES],
            interval="30s",
            firing=2,
            clearing=2,
            severity="critical",
            debounce="500ms",
            possible_playbook_ids=["health-objective"],
            originating_commit="lifecycle-bootstrap",
            sources=[
                ProposedGoSource(name="detector.go", content=artifact.detector_source),
                ProposedGoSource(name="detector_test.go", content=artifact.detector_test_source),
            ],
        )
    )
    proposal.playbooks.append(
        ProposedPlaybook(
            id="health-objective",
            owner="responder",
            fault_class="health-objective-failure",
            originating_incident="lifecycle-bootstrap",
            markdown=(
                "# Health objective failure\n\nInspect `<AFFECTED_RESOURCE>`, repair the source of truth, "
                "redeploy, and verify the human-owned objective.\n"
            ),
        )
    )
    generators: dict[str, list[ProposedGoSource]] = {}
    for file in artifact.traffic_files:
        relative = PurePosixPath(file.path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("traffic artifact has invalid logical name")
        if len(relative.parts) == 2 and relative.parts[0] == "generators" and relative.suffix == ".go":
            generators.setdefault("application", []).append(ProposedGoSource(name=relative.name, content=file.content))
        elif len(relative.parts) == 2 and relative.parts[0] == "workloads" and relative.suffix == ".yaml":
            document = yaml.safe_load(file.content)
            if not isinstance(document, dict):
                raise ValueError("traffic workload must be a YAML object")
            workload = json_format.ParseDict(document, ProtoTrafficWorkload())
            if workload.name != relative.stem:
                raise ValueError("traffic workload logical ID differs from authored name")
            proposal.traffic_workloads.append(workload)
        else:
            raise ValueError("traffic artifact has unsupported kind")
    for generator_id, sources in generators.items():
        proposal.traffic_generators.append(ProposedTrafficGenerator(id=generator_id, sources=sources))
    return proposal


def run_judge_task(
    task_path: Path,
    result_name: str,
    publisher: ResultPublisher,
    *,
    execute: Callable[[HealthJudgeRequest, Path], HealthJudgeResult] = author_health_diagnostics,
) -> AgentResult:
    task = parse_json(task_path.read_text(encoding="utf-8"), AgentTask())
    if (
        task.schema_version != "sdo.dev/v1alpha1"
        or not task.task_id
        or not task.agent_id
        or task.task_type != TASK_TYPE
    ):
        raise ValueError("invalid health judge task identity")
    if not task.workspace.proposal_id or not task.workspace.checkout_path or not task.workspace.base_commit:
        raise ValueError("health judge requires an accepted proposal checkout")
    request = HealthJudgeRequest()
    if not task.request.Unpack(request):
        raise ValueError("health judge request has wrong type")
    if not request.application or not request.namespace or not request.health_objective or request.attempt < 1:
        raise ValueError("health judge request is incomplete")
    checkout = Path(task.workspace.checkout_path)
    if not request.accepted_source_commit or not request.architecture_summary_markdown:
        raise ValueError("health judge requires accepted source and architecture context")
    if not re.fullmatch(r"[a-f0-9]{64}", request.source_topology_fingerprint):
        raise ValueError("health judge requires Go-prepared source inventory fingerprint")
    if any(not (row.kind and row.name and row.namespace and row.source) for row in request.source_resources):
        raise ValueError("health judge received an incomplete source inventory resource")
    typed = execute(request, checkout)
    if not typed.HasField("artifact_proposal") or not typed.artifact_proposal.detectors:
        raise ValueError("health judge must return typed diagnostic artifacts")
    result = AgentResult(
        schema_version="sdo.dev/v1alpha1",
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
