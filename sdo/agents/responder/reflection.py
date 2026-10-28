from __future__ import annotations

import json
import os
import re
import time
from typing import TYPE_CHECKING, ClassVar, Literal, Protocol, cast

from agentshim import SessionResumeError
from google.protobuf import json_format
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from libs.agent_cli.structured import (
    AgentProvider,
    StructuredTurn,
    StructuredTurnError,
    turn_usage,
)
from sdo.agents.code_paths import agent_guidance
from sdo.agents.context_output import save_context
from sdo.agents.memory_diagnostics import render_validation_feedback
from sdo.agents.prompt_templates import RenderedPrompt, named_prompt, render_prompt
from sdo.agents.prompt_templates import run_agent_turn as run_structured_turn
from sdo.agents.responder.credentials import SKILLS_ROOT
from sdo.agents.responder.memory_check import VALIDATION_TIMEOUT_SECONDS, ValidationResult, check_reflection_result
from sdo.agents.responder.reflection_authoring import SdkGap
from sdo.agents.responder.reflection_brief import incident_brief
from sdo.agents.responder.reflection_evidence import reflection_memory_view
from sdo.agents.responder.reflection_outcomes import current_outcome_view, history_view
from sdo.agents.responder.session_turn import (
    WrongBranchError,
    branch_correction,
    branch_instruction,
    session_turn_options,
    unwrap_branch,
)
from sdo.contracts import ReflectionResult, model_output_schema, validate, worker_result_payload
from sdo.contracts import SdkGap as ProtoSdkGap

ReflectionGuidance = Literal["baseline", "generalize", "generalize-spec"]
ReflectionSessionMode = Literal["resume", "fresh"]
REFLECTION_GUIDANCE_MODES: tuple[ReflectionGuidance, ...] = ("baseline", "generalize", "generalize-spec")
REFLECTION_SESSION_MODES: tuple[ReflectionSessionMode, ...] = ("resume", "fresh")

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from agentshim import CommandExecutor

    from sdo.contracts import IncidentClosure, MemoryDiagnostic, OutcomeRecord, ReflectionCauseWork, ReflectionWorkItem


class ReflectionTurn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str = Field(min_length=1)
    learning_decision: Literal["updated", "no_change", "sdk_gap"]
    no_change_reason: str | None = Field(default=None, min_length=1)
    decline_reason: Literal["", "no_recorded_repair"] = ""
    playbook_ids: list[str] = Field(default_factory=list)
    sdk_gaps: list[SdkGap] = Field(default_factory=list)
    # Provider accounting for the turn; kept out of the model-facing schema.
    _usage: dict[str, int | float] = PrivateAttr(default_factory=dict)

    @property
    def usage(self) -> dict[str, int | float]:
        return dict(self._usage)

    @classmethod
    def from_model_output(cls, payload: object) -> ReflectionTurn:
        """Parse the model's reply as the ``ReflectionResult`` it was asked for, then view it as a turn."""

        try:
            result = json_format.ParseDict(
                worker_result_payload(payload, ReflectionResult.DESCRIPTOR), ReflectionResult()
            )
        except json_format.ParseError as exc:
            raise ValueError(f"invalid reflection result: {exc}") from exc
        validate(result)
        return cls(
            summary=result.summary,
            learning_decision=cast("Literal['updated', 'no_change', 'sdk_gap']", result.learning_decision),
            no_change_reason=result.no_change_reason or None,
            decline_reason=cast("Literal['', 'no_recorded_repair']", result.decline_reason),
            playbook_ids=list(result.proposed_artifact_ids),
            sdk_gaps=[
                SdkGap.model_validate(json_format.MessageToDict(gap, preserving_proto_field_name=True))
                for gap in result.sdk_gaps
            ],
        )

    def to_result(self) -> ReflectionResult:
        """The exact decision checked locally and published in the worker envelope."""
        return ReflectionResult(
            summary=self.summary,
            learning_decision=self.learning_decision,
            no_change_reason=self.no_change_reason or "",
            decline_reason=self.decline_reason,
            proposed_artifact_ids=self.playbook_ids,
            sdk_gaps=[json_format.ParseDict(gap.model_dump(), ProtoSdkGap()) for gap in self.sdk_gaps],
        )

    def with_usage(self, usage: dict[str, int | float]) -> ReflectionTurn:
        self._usage = dict(usage)
        return self

    def model_post_init(self, __context: object) -> None:
        if any(not re.fullmatch(r"[a-z][a-z0-9_-]*", item) for item in self.playbook_ids):
            raise ValueError("reflection requires logical playbook IDs")
        if len(set(self.playbook_ids)) != len(self.playbook_ids):
            raise ValueError("reflection playbook IDs must be unique")
        if self.learning_decision == "no_change" and self.sdk_gaps:
            raise ValueError("no_change reflection cannot report SDK gaps")
        if self.learning_decision in {"no_change", "sdk_gap"}:
            if self.playbook_ids:
                raise ValueError("no_change or sdk_gap reflection cannot claim playbook IDs")
            if not self.no_change_reason or not self.no_change_reason.strip():
                raise ValueError("no_change or sdk_gap reflection requires no_change_reason")
        if self.learning_decision == "sdk_gap" and not self.sdk_gaps:
            raise ValueError("sdk_gap reflection requires typed gaps")


def reflection_output_schema() -> dict[str, object]:
    """The model-facing schema of ``ReflectionResult``, generated from the proto the Go decision loader reads."""

    return model_output_schema(ReflectionResult.DESCRIPTOR)


class StatefulResponderBackend(Protocol):
    def resume(
        self,
        *,
        session_id: str,
        worktree: Path,
        prompt: str,
        idempotency_key: str,
    ) -> ReflectionTurn: ...

    def fresh(
        self,
        *,
        worktree: Path,
        prompt: str,
        idempotency_key: str,
    ) -> ReflectionTurn: ...


class ResponderSessionUnavailable(RuntimeError):
    """The provider cannot continue the responder's incident session."""


#: Reasoning effort of the incident agents (responder and reflection). A
#: benchmark comparing SDO against a stock agent declares the same effort.
INCIDENT_REASONING_EFFORT = "medium"

#: Absolute Unix time of the controller's frozen reflection task deadline. The
#: worker binds it from the task; the Job and any claimed standby end no earlier.
REFLECTION_DEADLINE_ENV = "SDO_REFLECTION_DEADLINE"
#: The agent stops this long before that deadline so its typed result is published first.
REFLECTION_RESULT_MARGIN_SECONDS = 120.0
#: Budget without a bound deadline: the controller's default 30-minute task deadline.
DEFAULT_REFLECTION_SECONDS = 1800.0 - REFLECTION_RESULT_MARGIN_SECONDS


class ReflectionDeadlineExceeded(RuntimeError):
    """The reflection used its whole budget before the agent returned a valid result."""


class ReflectionCorrectionStopped(RuntimeError):
    """A blocking check cannot be corrected safely within this dispatch."""

    def __init__(
        self, reason: Literal["unchanged_validation_failure", "validation_budget_reserved"], check: ValidationResult
    ) -> None:
        self.reason = reason
        super().__init__(f"reflection memory check failed ({reason}): {check.stdout}\n{check.stderr}")


def reflection_budget_seconds() -> float:
    """Seconds the reflection agent may use, ending before the frozen task deadline."""
    raw = os.environ.get(REFLECTION_DEADLINE_ENV)
    if not raw:
        return DEFAULT_REFLECTION_SECONDS
    budget = float(raw) - time.time() - REFLECTION_RESULT_MARGIN_SECONDS
    if not budget > 0:
        raise ReflectionDeadlineExceeded("reflection task deadline leaves no agent budget")
    return budget


_GENERALIZING_GUIDANCE = ("generalize", "generalize-spec")


def _logical_learning_request(
    *,
    outcome: OutcomeRecord,
    history: list[OutcomeRecord],
    outcome_commit: str,
    closure: IncidentClosure,
    guidance: ReflectionGuidance,
    cause_work: Sequence[ReflectionCauseWork] = (),
    unassigned_work: Sequence[ReflectionWorkItem] = (),
) -> RenderedPrompt:
    if not closure.request.HasField("memory_snapshot"):
        raise ValueError("logical reflection requires an operational memory snapshot")
    snapshot = closure.request.memory_snapshot
    findings = [*outcome.findings, *closure.request.findings]
    linked = {finding.detector_id for finding in findings}
    linked.update(playbook for finding in findings for playbook in finding.playbook_ids)
    linked.update([*outcome.applied_playbooks, *outcome.confirmed_playbooks, *outcome.rejected_playbooks])
    linked.update(run.program_id for run in outcome.program_runs)
    linked.update(match.playbook_id for match in closure.request.playbook_coverage.matches)
    memory_index = reflection_memory_view(snapshot, outcome, linked_ids=linked)
    _, memory_path = save_context(json.dumps(memory_index, indent=2))
    _, outcome_path = save_context(json_format.MessageToJson(outcome, preserving_proto_field_name=True))
    outcome_view = current_outcome_view(outcome)
    outcome_summary = {
        key: outcome_view[key]
        for key in (
            "incident_id",
            "classification",
            "findings",
            "confirmed_root_causes",
            "repair_actions",
            "plan_applications",
            "undetected_causes",
            "final_detector_states",
            "source_check_notes",
            "disabled_setup_detectors",
        )
        if key in outcome_view
    }
    outcome_summary["full_current_outcome_path"] = outcome_path
    memory_index["full_index_path"] = memory_path
    if agent_guidance() == "shell":
        return render_prompt(
            "responder",
            "reflection_shell",
            part="reflection.instructions",
            skills_root=SKILLS_ROOT,
            branch_instruction=branch_instruction("reflection"),
            memory_view_json=named_prompt("reflection.memory", json.dumps(memory_index, indent=2)),
            outcome_commit=outcome_commit,
            outcome_json=named_prompt("reflection.outcome", json.dumps(outcome_summary, indent=2)),
            history_section=named_prompt("reflection.history", _history_section(outcome, history)),
        )
    work_section = ""
    if cause_work or unassigned_work:
        work_json = json.dumps(
            {
                "causes": [json_format.MessageToDict(item, preserving_proto_field_name=True) for item in cause_work],
                "unassigned": [
                    json_format.MessageToDict(item, preserving_proto_field_name=True) for item in unassigned_work
                ],
            },
            indent=2,
        )
        work_section = render_prompt("responder", "learning_01", part="reflection.instructions", work_json=work_json)
    prompt = render_prompt(
        "responder",
        "reflection",
        part="reflection.instructions",
        skills_root=SKILLS_ROOT,
        branch_instruction=branch_instruction("reflection"),
        work_section=named_prompt("reflection.cause_work", work_section),
        outcome_commit=outcome_commit,
        memory_view_json=named_prompt("reflection.memory", json.dumps(memory_index, indent=2)),
        current_outcome_view_json=named_prompt("reflection.outcome", json.dumps(outcome_summary, indent=2)),
        history_section=named_prompt("reflection.history", _history_section(outcome, history)),
    )
    if closure.request.HasField("namespace_onboarding"):
        prompt += render_prompt(
            "responder",
            "namespace_onboarding",
            part="reflection.outcome",
            onboarding_json=json_format.MessageToJson(
                closure.request.namespace_onboarding, preserving_proto_field_name=True
            ),
        )
    return prompt


def _history_section(outcome: OutcomeRecord, history: list[OutcomeRecord]) -> str:
    detector_ids = {finding.detector_id for finding in outcome.findings}
    playbook_ids = set(outcome.applied_playbooks) | set(outcome.confirmed_playbooks)
    resources = {
        (ref.kind, ref.namespace, ref.name) for cause in outcome.confirmed_root_causes for ref in cause.resources
    }
    relevant = [
        record
        for record in history
        if (
            any(finding.detector_id in detector_ids for finding in record.findings)
            or playbook_ids.intersection(record.applied_playbooks)
            or playbook_ids.intersection(record.confirmed_playbooks)
            or any(
                (ref.kind, ref.namespace, ref.name) in resources
                for cause in record.confirmed_root_causes
                for ref in cause.resources
            )
        )
    ]
    prior = history_view(relevant[-3:], current_incident_id=outcome.incident_id)
    if not prior:
        return render_prompt("responder", "history_section_01", part="reflection.instructions")
    return render_prompt(
        "responder", "history_section_02", part="reflection.instructions", prior_json=json.dumps(prior, indent=2)
    )


class SessionReflector:
    """Reflect on a verified outcome.

    The first attempt resumes the responder session (``session_mode="resume"``)
    or, opt-in, starts a fresh session from a compact incident brief
    (``"fresh"``). A retry after a broker rejection carries typed diagnostics in the same
    resumed incident session.
    """

    def __init__(self, backend: StatefulResponderBackend, *, guidance: str = "baseline") -> None:
        if guidance not in REFLECTION_GUIDANCE_MODES:
            raise ValueError(f"unsupported reflection guidance: {guidance!r}")
        self.backend = backend
        self.guidance: ReflectionGuidance = cast("ReflectionGuidance", guidance)

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
    ) -> ReflectionTurn:
        if session_mode not in REFLECTION_SESSION_MODES:
            raise ValueError(f"unsupported reflection session mode: {session_mode!r}")
        if closure is None or not closure.request.HasField("memory_snapshot"):
            raise ValueError("reflection requires a logical memory snapshot")
        request = _logical_learning_request(
            outcome=outcome,
            history=history,
            outcome_commit=outcome_commit,
            closure=closure,
            guidance=self.guidance,
            cause_work=cause_work,
            unassigned_work=unassigned_work,
        )
        idempotency_key = f"reflection:{incident_id}:{outcome_commit}"
        if diagnostics:
            # Keep the incident session and the rejected edits; only the
            # rejection is new. A fresh session also needs the request.
            original_request = (
                render_prompt("responder", "reflection_retry_request", part="reflection.instructions", request=request)
                if session_mode == "fresh"
                else RenderedPrompt(())
            )
            prompt = render_prompt(
                "responder",
                "reflection_retry",
                part="reflection.instructions",
                render_validation_feedback=named_prompt(
                    "reflection.validation_feedback",
                    render_validation_feedback(validation_attempt, diagnostics, kept_edits=True),
                ),
                original_request=original_request,
            )
            if session_mode == "resume":
                return self.backend.resume(
                    session_id=session_id, worktree=worktree, prompt=prompt, idempotency_key=idempotency_key
                )
            return self.backend.fresh(worktree=worktree, prompt=prompt, idempotency_key=idempotency_key)
        if session_mode == "fresh":
            if agent_guidance() == "shell":
                prompt = render_prompt(
                    "responder", "reflection_shell_fresh", part="reflection.instructions", request=request
                )
                return self.backend.fresh(worktree=worktree, prompt=prompt, idempotency_key=idempotency_key)
            if closure is None:
                raise ValueError("a fresh first reflection attempt requires the verified closure for its brief")
            # The request already contains the complete accepted inventory;
            # the brief replaces only the missing responder transcript.
            brief = incident_brief(
                closure,
                include_memory=False,
                worktree=worktree,
                detector_detail=self.guidance in _GENERALIZING_GUIDANCE,
                event_dependence=self.guidance == "generalize-spec",
            )
            prompt = render_prompt(
                "responder", "reflection_fresh", part="reflection.instructions", brief=brief, request=request
            )
            return self.backend.fresh(worktree=worktree, prompt=prompt, idempotency_key=idempotency_key)
        prompt = render_prompt("responder", "reflection_resume", part="reflection.instructions", request=request)
        return self.backend.resume(
            session_id=session_id,
            worktree=worktree,
            prompt=prompt,
            idempotency_key=idempotency_key,
        )


def memory_check_correction(check: ValidationResult, idempotency_key: str) -> RenderedPrompt:
    details = check.stdout + "\n" + check.stderr
    feedback = details
    try:
        report = json.loads(check.stdout)
        diagnostics = report.get("diagnostics", [])
        if diagnostics:
            feedback = "\n".join(
                f"[{item.get('rule', '')}] {item.get('path', '')}: {item.get('message', '')}" for item in diagnostics
            )
            _, path = save_context(details)
            feedback += "\nFull check: " + path
    except (ValueError, AttributeError, TypeError):
        pass
    correction = render_prompt(
        "responder",
        "reflection_check_correction",
        part="reflection.instructions",
        diagnostics=named_prompt("reflection.validation_feedback", feedback),
    )
    return render_prompt(
        "responder",
        "turn_01",
        part="reflection.instructions",
        idempotency_key=idempotency_key,
        prompt=correction,
    )


class CodexSessionBackend:
    """Resume the responder's own Codex session to reflect on its outcome.

    Validation retries retain the responder session and carry typed feedback.
    """

    provider: ClassVar[AgentProvider] = "codex"

    def __init__(
        self,
        *,
        model: str | None = None,
        reasoning_effort: str = INCIDENT_REASONING_EFFORT,
        timeout_seconds: float | None = None,
        executor: CommandExecutor | None = None,
        memory_checker: Callable[[Path, ReflectionResult, float], ValidationResult] = check_reflection_result,
    ) -> None:
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout_seconds = timeout_seconds
        self.executor = executor
        self.memory_checker = memory_checker

    def resume(
        self,
        *,
        session_id: str,
        worktree: Path,
        prompt: str,
        idempotency_key: str,
    ) -> ReflectionTurn:
        return self._turn(worktree=worktree, prompt=prompt, idempotency_key=idempotency_key, session_id=session_id)

    def fresh(
        self,
        *,
        worktree: Path,
        prompt: str,
        idempotency_key: str,
    ) -> ReflectionTurn:
        """Run a new session in the incident worktree; used for validation retries."""

        return self._turn(worktree=worktree, prompt=prompt, idempotency_key=idempotency_key, session_id=None)

    def _turn(
        self,
        *,
        worktree: Path,
        prompt: str,
        idempotency_key: str,
        session_id: str | None,
    ) -> ReflectionTurn:
        budget = reflection_budget_seconds() if self.timeout_seconds is None else self.timeout_seconds
        deadline = time.monotonic() + budget
        # Keep validation inside the frozen worker budget. Small explicit test or
        # operator budgets reserve the same fraction, up to one full check.
        validation_reserve = min(VALIDATION_TIMEOUT_SECONDS, budget / 3)

        def remaining() -> float:
            left = deadline - time.monotonic()
            if left <= 0:
                raise ReflectionDeadlineExceeded(f"reflection exceeded its {budget:g}s budget")
            return left

        def agent_remaining() -> float:
            left = remaining() - validation_reserve
            if left <= 0:
                raise ReflectionDeadlineExceeded("reflection validation budget reserved; no agent budget remains")
            return left

        turn = self._run(
            worktree=worktree,
            prompt=render_prompt(
                "responder", "turn_01", part="reflection.instructions", idempotency_key=idempotency_key, prompt=prompt
            ),
            session_id=session_id,
            timeout_seconds=agent_remaining(),
        )
        usage: dict[str, int | float] = {}
        previous_failure: ValidationResult | None = None
        for attempt in range(4):
            for name, value in turn_usage(turn).items():
                usage[name] = usage.get(name, 0) + value
            try:
                payload = unwrap_branch(turn.output_json, "reflection")
            except WrongBranchError:
                turn = self._run(
                    worktree=worktree,
                    prompt=branch_correction("reflection"),
                    session_id=turn.session_id,
                    timeout_seconds=agent_remaining(),
                )
                for name, value in turn_usage(turn).items():
                    usage[name] = usage.get(name, 0) + value
                payload = unwrap_branch(turn.output_json, "reflection")
            # Preserve the complete returned proto through validation: Python's
            # view must not discard deprecated payloads or reject a decision
            # before the shared Go diagnostics can drive an in-session correction.
            decision = json_format.ParseDict(
                worker_result_payload(payload, ReflectionResult.DESCRIPTOR), ReflectionResult()
            )
            check = self.memory_checker(worktree, decision, min(VALIDATION_TIMEOUT_SECONDS, remaining()))
            if check.exit_code == 0 and not check.timed_out:
                return ReflectionTurn.from_model_output(
                    json_format.MessageToDict(decision, preserving_proto_field_name=True)
                ).with_usage(usage)
            if attempt == 3 or time.monotonic() >= deadline:
                raise RuntimeError(f"reflection memory check failed before submission: {check.stdout}\n{check.stderr}")
            if check == previous_failure:
                raise ReflectionCorrectionStopped("unchanged_validation_failure", check)
            if remaining() <= validation_reserve:
                raise ReflectionCorrectionStopped("validation_budget_reserved", check)
            previous_failure = check
            turn = self._run(
                worktree=worktree,
                prompt=memory_check_correction(check, idempotency_key),
                session_id=turn.session_id,
                timeout_seconds=agent_remaining(),
            )
        raise AssertionError("unreachable reflection validation state")

    def _run(self, *, worktree: Path, prompt: str, session_id: str | None, timeout_seconds: float) -> StructuredTurn:
        if os.environ.get("SDO_API_HISTORY_DIR"):
            prompt += render_prompt("responder", "run_01", part="reflection.instructions")
        try:
            return run_structured_turn(
                self.provider,
                prompt,
                cwd=worktree,
                access="danger-full-access",
                model=self.model,
                reasoning_effort=self.reasoning_effort,
                timeout_seconds=timeout_seconds,
                resume_session_id=session_id,
                executor=self.executor,
                **session_turn_options(self.provider, worktree, phase="reflection"),
            )
        except StructuredTurnError as exc:
            if session_id is not None and (
                str(exc) == f"{self.provider} cannot resume session {session_id!r}"
                or (
                    isinstance(exc.__cause__, SessionResumeError)
                    and (
                        self.provider == "codex"
                        or "no conversation found" in exc.__cause__.stderr.lower()
                        or "session not found" in exc.__cause__.stderr.lower()
                    )
                )
            ):
                raise ResponderSessionUnavailable(
                    f"{self.provider} cannot resume session {session_id!r}: {exc}"
                ) from exc
            raise RuntimeError(f"{self.provider} reflection failed: {exc}") from exc


class ClaudeSessionBackend(CodexSessionBackend):
    """Resume the responder's own Claude Code session to reflect on its outcome."""

    provider: ClassVar[AgentProvider] = "claude"


render_reflection_prompt = _logical_learning_request
