"""Structured, fresh-session agents used during SDO lifecycle bootstrap."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, ClassVar, Protocol

from pydantic import BaseModel, ConfigDict, Field

from libs.agent_cli.structured import (
    AgentProvider,
    StructuredTurnError,
    StructuredTurnTimeout,
)
from sdo.agents.code_paths import SKILLS_ROOT, agent_guidance
from sdo.agents.prompt_templates import named_prompt, render_prompt
from sdo.agents.prompt_templates import run_agent_turn as run_structured_turn
from sdo.agents.provider import DEFAULT_AGENT_EFFORT
from sdo.agents.replay import selected_skills_root

if TYPE_CHECKING:
    from pathlib import Path

    from agentshim import CommandExecutor


#: The health-objective detector watches every Kubernetes kind the detector SDK's
#: snapshot exposes except high-churn Events, in this order. The set follows the SDK,
#: not any fault class, so a judge can check any object the snapshot offers.
HEALTH_OBJECTIVE_WATCHES: tuple[tuple[str, str], ...] = (
    ("v1", "Pod"),
    ("v1", "ConfigMap"),
    ("v1", "Service"),
    ("apps/v1", "Deployment"),
    ("apps/v1", "ReplicaSet"),
    ("networking.k8s.io/v1", "NetworkPolicy"),
    ("v1", "Endpoints"),
    ("discovery.k8s.io/v1", "EndpointSlice"),
)

#: Kinds whose source-backed objects count as health-judge coverage provenance: the
#: application configuration kinds the deployment adapter observes as active topology.
#: Coverage is controller-canonicalized metadata; it selects no check.
COVERED_RESOURCE_KINDS: frozenset[str] = frozenset({"ConfigMap", "Deployment", "NetworkPolicy", "Service"})


class LifecycleAgentError(RuntimeError):
    """Raised when a lifecycle agent cannot produce a valid structured handoff."""


class TopologyResourceDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str = Field(min_length=1)
    name: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    source: str = Field(min_length=1)
    dependencies: list[str]


class ActiveTopologyResourceDTO(BaseModel):
    """A resource selected by the deployment backend for the active variant."""

    model_config = ConfigDict(extra="forbid")

    kind: str = Field(min_length=1)
    name: str = Field(min_length=1)


class DeployerDraft(BaseModel):
    """Structured deployer output.

    The resource inventory is deliberately absent: it is a deterministic controller
    fact derived from tracked manifests, so the controller attaches it instead of
    asking the model to transcribe it.
    """

    model_config = ConfigDict(extra="forbid")

    source_commit: str = Field(min_length=1)
    topology_fingerprint: str = Field(min_length=64, max_length=64)
    architecture_summary_markdown: str = Field(min_length=20)


class DeployerHandoff(DeployerDraft):
    """A deployer draft attributed to the fresh agent session that produced it."""

    session_id: str = Field(min_length=1)


class DeployerAssessment(DeployerHandoff):
    """A published deployer handoff with the controller-derived resource inventory."""

    resources: list[TopologyResourceDTO]


class HealthJudgeDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    round: int = Field(ge=1)
    objective_digest: str = Field(min_length=64, max_length=64)
    source_commit: str = Field(min_length=1)
    covered_resources: list[TopologyResourceDTO]
    failure_patterns: list[str] = Field(min_length=1)
    detector_ids: list[str] = Field(min_length=1)


class HealthJudgeArtifact(HealthJudgeDraft):
    session_id: str = Field(min_length=1)


_WATCH_LIST = ", ".join(f"{api_version} {kind}" for api_version, kind in HEALTH_OBJECTIVE_WATCHES)
_COVERED_KINDS = ", ".join(sorted(COVERED_RESOURCE_KINDS))

#: Health-judge instructions for direct synthetic traffic edits.
TRAFFIC_AUTHORING = render_prompt("health_judge", "traffic_authoring", part="health_judge.traffic_authoring")

#: Metrics are an optional detector input, present only when an operator configured a metrics source.
METRICS_AUTHORING = render_prompt("health_judge", "metrics_authoring", part="health_judge.metrics_authoring")


class LifecycleAgentBackend(Protocol):
    def run_deployer(
        self,
        *,
        repository: Path,
        application: str,
        correction_feedback: str | None,
    ) -> DeployerHandoff: ...

    def run_health_judge(
        self,
        *,
        repository: Path,
        application: str,
        health_objective: str,
        deployer: DeployerAssessment,
        round_index: int,
        previous: HealthJudgeArtifact | None,
        correction_feedback: str | None,
        active_resources: list[ActiveTopologyResourceDTO] | None = None,
    ) -> HealthJudgeArtifact: ...


class CodexLifecycleBackend:
    """Run each lifecycle handoff in a fresh session inside its worker pod."""

    provider: ClassVar[AgentProvider] = "codex"

    def __init__(
        self,
        *,
        model: str | None = None,
        reasoning_effort: str = DEFAULT_AGENT_EFFORT,
        timeout_seconds: int = 900,
        executor: CommandExecutor | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("lifecycle agent timeout must be positive")
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout_seconds = timeout_seconds
        self.executor = executor

    def run_deployer(
        self,
        *,
        repository: Path,
        application: str,
        correction_feedback: str | None,
    ) -> DeployerHandoff:
        feedback = correction_feedback or render_prompt("lifecycle", "run_deployer_01", part="lifecycle.instructions")
        prompt = render_prompt(
            "lifecycle",
            "architecture",
            part="lifecycle.instructions",
            application=repr(application),
            feedback=feedback,
        )
        draft, session_id = self._execute(repository, prompt, DeployerDraft)
        return DeployerHandoff(**draft.model_dump(), session_id=session_id)

    def run_health_judge(
        self,
        *,
        repository: Path,
        application: str,
        health_objective: str,
        deployer: DeployerAssessment,
        round_index: int,
        previous: HealthJudgeArtifact | None,
        correction_feedback: str | None,
        active_resources: list[ActiveTopologyResourceDTO] | None = None,
    ) -> HealthJudgeArtifact:
        objective_digest = hashlib.sha256(health_objective.strip().encode()).hexdigest()
        previous_payload = previous.model_dump_json(indent=2) if previous else "null"
        active_payload = (
            json.dumps([resource.model_dump(mode="json") for resource in active_resources], indent=2)
            if active_resources is not None
            else "null"
        )
        feedback = correction_feedback or render_prompt(
            "health_judge", "run_health_judge_01", part="health_judge.instructions"
        )
        prompt = render_prompt(
            "health_judge",
            "health_judge_shell" if agent_guidance() == "shell" else "health_judge",
            part="health_judge.instructions",
            skills_root=selected_skills_root(SKILLS_ROOT),
            source_commit=deployer.source_commit,
            application=repr(application),
            round_index=round_index,
            health_objective=named_prompt("health_judge.health_objective", health_objective),
            objective_digest=objective_digest,
            deployer=named_prompt("health_judge.deployer", deployer.model_dump_json(indent=2), required=False),
            active_payload=named_prompt("health_judge.active_payload", active_payload, required=False),
            previous_payload=named_prompt("health_judge.previous_payload", previous_payload, required=False),
            feedback=feedback,
            watch_list=_WATCH_LIST,
            round_index_2=round_index,
            covered_kinds=_COVERED_KINDS,
            traffic_authoring=TRAFFIC_AUTHORING,
            metrics_authoring=METRICS_AUTHORING,
        )
        draft, session_id = self._execute(repository, prompt, HealthJudgeDraft)
        return HealthJudgeArtifact(**draft.model_dump(), session_id=session_id)

    def _execute(
        self,
        repository: Path,
        prompt: str,
        output_type: type[DeployerDraft] | type[HealthJudgeDraft],
    ) -> tuple[DeployerDraft | HealthJudgeDraft, str]:
        role = f"{self.provider.capitalize()} lifecycle session"
        try:
            turn = run_structured_turn(
                self.provider,
                prompt,
                output_schema=output_type.model_json_schema(),
                cwd=repository,
                access="danger-full-access",
                model=self.model,
                reasoning_effort=self.reasoning_effort,
                timeout_seconds=self.timeout_seconds,
                executor=self.executor,
            )
        except StructuredTurnTimeout as exc:
            raise LifecycleAgentError(f"{role} timed out after {self.timeout_seconds}s") from exc
        except StructuredTurnError as exc:
            raise LifecycleAgentError(f"{role} failed: {exc}") from exc
        try:
            output = output_type.model_validate_json(turn.output_json)
        except ValueError as exc:
            raise LifecycleAgentError(f"invalid structured {role} output: {exc}") from exc
        return output, turn.session_id


class ClaudeLifecycleBackend(CodexLifecycleBackend):
    """Run lifecycle handoffs as fresh structured Claude Code sessions."""

    provider: ClassVar[AgentProvider] = "claude"
