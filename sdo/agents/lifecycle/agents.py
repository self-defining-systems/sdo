"""Structured, fresh-session agents used during SDO lifecycle bootstrap."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Protocol

from pydantic import BaseModel, ConfigDict, Field

from libs.agent_cli.structured import (
    AccessMode,
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


class LifecycleAgentError(RuntimeError):
    """Raised when a lifecycle agent cannot produce a valid structured handoff."""


class TopologyResourceDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str = Field(min_length=1)
    name: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    source: str = Field(min_length=1)
    dependencies: list[str]


class HealthJudgeDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    failure_patterns: list[str] = Field(min_length=1)
    detector_ids: list[str] = Field(min_length=1)


class HealthJudgeArtifact(HealthJudgeDraft):
    session_id: str = Field(min_length=1)


#: Health-judge instructions for direct synthetic traffic edits.
TRAFFIC_AUTHORING = render_prompt("health_judge", "traffic_authoring", part="health_judge.traffic_authoring")

#: Metrics are an optional detector input, present only when an operator configured a metrics source.
METRICS_AUTHORING = render_prompt("health_judge", "metrics_authoring", part="health_judge.metrics_authoring")


class LifecycleAgentBackend(Protocol):
    def run_health_judge(
        self,
        *,
        repository: Path,
        application: str,
        health_objective: str,
        architecture: str,
        resources: list[TopologyResourceDTO],
        correction_feedback: str | None,
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

    def run_health_judge(
        self,
        *,
        repository: Path,
        application: str,
        health_objective: str,
        architecture: str,
        resources: list[TopologyResourceDTO],
        correction_feedback: str | None,
    ) -> HealthJudgeArtifact:
        feedback = correction_feedback or render_prompt(
            "health_judge", "run_health_judge_01", part="health_judge.instructions"
        )
        prompt = render_prompt(
            "health_judge",
            "health_judge_shell" if agent_guidance() == "shell" else "health_judge",
            part="health_judge.instructions",
            skills_root=selected_skills_root(SKILLS_ROOT),
            application=repr(application),
            health_objective=named_prompt(
                "health_judge.health_objective",
                health_objective or "No goal supplied; infer application health from source.",
            ),
            architecture=named_prompt("health_judge.architecture", architecture, required=False),
            resources=named_prompt(
                "health_judge.resources",
                str([resource.model_dump() for resource in resources]),
                required=False,
            ),
            feedback=feedback,
            traffic_authoring=TRAFFIC_AUTHORING,
            metrics_authoring=METRICS_AUTHORING,
        )
        draft, session_id = self._execute(repository, prompt, HealthJudgeDraft, sandbox="workspace-write")
        return HealthJudgeArtifact(**draft.model_dump(), session_id=session_id)

    def _execute(
        self,
        repository: Path,
        prompt: str,
        output_type: type[HealthJudgeDraft],
        *,
        sandbox: AccessMode = "read-only",
    ) -> tuple[HealthJudgeDraft, str]:
        role = f"{self.provider.capitalize()} lifecycle session"
        try:
            turn = run_structured_turn(
                self.provider,
                prompt,
                output_schema=output_type.model_json_schema(),
                cwd=repository,
                access=sandbox,
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
