from __future__ import annotations

import json
import re
import sys
from typing import TYPE_CHECKING, ClassVar, Literal, Protocol, cast

from agentshim import SessionResumeError
from google.protobuf import json_format
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from libs.agent_cli.structured import (
    AgentProvider,
    StructuredTurn,
    StructuredTurnError,
    run_structured_turn,
    turn_usage,
)
from sdo.agents.memory_diagnostics import render_validation_feedback
from sdo.agents.responder.credentials import SKILLS_ROOT
from sdo.agents.responder.reflection_authoring import SdkGap
from sdo.agents.responder.reflection_brief import incident_brief
from sdo.agents.responder.reflection_outcomes import current_outcome_view, history_view
from sdo.agents.responder.reflection_refine import refine_learning_request
from sdo.agents.responder.session_turn import (
    WrongBranchError,
    branch_correction,
    branch_instruction,
    session_turn_options,
    unwrap_branch,
)
from sdo.contracts import OutcomeClassification, ReflectionResult, model_output_schema, validate

ReflectionGuidance = Literal["baseline", "generalize", "generalize-spec"]
ReflectionSessionMode = Literal["resume", "fresh"]
REFLECTION_GUIDANCE_MODES: tuple[ReflectionGuidance, ...] = ("baseline", "generalize", "generalize-spec")
REFLECTION_SESSION_MODES: tuple[ReflectionSessionMode, ...] = ("resume", "fresh")
ReflectionDepth = Literal["full", "refine"]
REFLECTION_DEPTHS: tuple[ReflectionDepth, ...] = ("full", "refine")

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from agentshim import CommandExecutor

    from sdo.contracts import IncidentClosure, MemoryDiagnostic, OutcomeRecord


def _classification_directive(classification: str, guidance: ReflectionGuidance = "baseline") -> str:
    if classification == OutcomeClassification.SUCCESS:
        if guidance in _GENERALIZING_GUIDANCE:
            return (
                "Capture the confirmed signature at the level of its root-cause class, following the generalization "
                "protocol above, and stay within what the evidence supports."
            )
        return "Capture the confirmed signature without generalizing beyond the observed successful evidence."
    return "Do not encode the result as successful operational memory."


class ReflectionTurn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str = Field(min_length=1)
    learning_decision: Literal["updated", "no_change", "sdk_gap"]
    no_change_reason: str | None = Field(default=None, min_length=1)
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
            result = json_format.ParseDict(payload, ReflectionResult())
        except json_format.ParseError as exc:
            raise ValueError(f"invalid reflection result: {exc}") from exc
        validate(result)
        return cls(
            summary=result.summary,
            learning_decision=cast("Literal['updated', 'no_change', 'sdk_gap']", result.learning_decision),
            no_change_reason=result.no_change_reason or None,
            playbook_ids=list(result.proposed_artifact_ids),
            sdk_gaps=[
                SdkGap.model_validate(json_format.MessageToDict(gap, preserving_proto_field_name=True))
                for gap in result.sdk_gaps
            ],
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

_GENERALIZING_GUIDANCE = ("generalize", "generalize-spec")


def _logical_guidance(guidance: ReflectionGuidance) -> str:
    if guidance == "baseline":
        return (
            "Capture the confirmed signature without generalizing beyond the observed successful evidence. "
            "Add a precise matching test and near-miss regression test for any incident detector.\n"
        )
    protocol = (
        "Generalize by root-cause class: compare the confirmed failure mechanism and repair with existing "
        "incident detectors. Widen one existing detector and playbook when only resource identity or a parameter "
        "differs; do not add a sibling. Use Finding.ParameterBindings for affected roles, preserve existing "
        "originatingIncident and originatingCommit, and include matching and near-miss tests. "
        "Create a new logical detector and playbook only for a different failure mechanism or repair.\n"
    )
    if guidance == "generalize-spec":
        protocol += (
            "Spec-first: prefer a detector predicate decidable from current Kubernetes spec or status. "
            "Events, logs, and restart counts may corroborate, but must not be the firing precondition when "
            "current state is sufficient. Prove a matching snapshot without Event objects and a near-miss. "
            "If the confirmed cause is not decidable from spec or status, name the earliest available evidence.\n"
        )
    return protocol


def _logical_learning_request(
    *,
    outcome: OutcomeRecord,
    history: list[OutcomeRecord],
    outcome_commit: str,
    closure: IncidentClosure,
    guidance: ReflectionGuidance,
) -> str:
    if not closure.request.HasField("memory_snapshot"):
        raise ValueError("logical reflection requires an operational memory snapshot")
    snapshot = closure.request.memory_snapshot
    classification = outcome.classification
    return (
        "The controller verified this incident and committed its outcome. "
        "Learn a reusable relation for each confirmed fault, refine accepted memory when the mechanism is shared, "
        "or explain why the captured evidence cannot support safe learning. "
        "Record reused program IDs in the summary. "
        f"Start with the skill index at {SKILLS_ROOT}/sdo-memory/SKILL.md and follow its authoring links. "
        "The installed skills are the authoring guide; authoring_reference retrieves the same shipped references. "
        "Read only what is needed and keep accepted provenance. "
        "Edit the final files directly in the operational memory directory of this isolated checkout. "
        "Source edits, outcomes, goal and architecture are protected. "
        "Use captured observations, parameterized programs, and matching and near-miss tests. "
        "Unavailable observations remain Unknown. Preserve supported source-restoration mitigators. "
        "Use create_corpus for captured cases and check_memory with typed decision fields for final validation. "
        "Correct named diagnostics until it exits zero; the broker independently checks the same final files. "
        "Return updated only with a nonempty diff and every changed logical playbook ID; otherwise return "
        "no_change or a typed sdk_gap with a concrete reason and no authored diff. "
        "Return decisions and logical IDs only. "
        f"{branch_instruction('reflection')}\n\n"
        f"{_logical_guidance(guidance)}"
        f"For this {classification} outcome, "
        f"{_classification_directive(classification, guidance)}\n\n"
        "Before returning an update, the complete final files must be in the checkout and memory check must pass. "
        f"Outcome commit: {outcome_commit}\n"
        f"Operational memory snapshot:\n{json_format.MessageToJson(snapshot, preserving_proto_field_name=True)}\n\n"
        "Repeated program evaluations and tool calls below carry counts. Large tool responses are excerpted; "
        "outcome_call_index and response_json_sha256 identify the full response in the committed outcome. "
        "Root causes, findings, action receipts and program transitions are retained.\n"
        f"Current outcome:\n{json.dumps(current_outcome_view(outcome), indent=2)}\n\n"
        f"{_history_section(outcome, history)}"
    )


def _history_section(outcome: OutcomeRecord, history: list[OutcomeRecord]) -> str:
    prior = history_view(history, current_incident_id=outcome.incident_id)
    if not prior:
        return "Outcome history (prior incidents, compact): none\n"
    return f"Outcome history (prior incidents, compact; raw evidence omitted):\n{json.dumps(prior, indent=2)}\n"


class SessionReflector:
    """Reflect on a verified outcome.

    The first attempt resumes the responder session (``session_mode="resume"``)
    or, opt-in, starts a fresh session from a compact incident brief
    (``"fresh"``). A retry after a broker rejection (non-empty ``diagnostics``)
    is always a short fresh session that carries the typed diagnostics.
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
        depth: ReflectionDepth = "full",
    ) -> ReflectionTurn:
        if session_mode not in REFLECTION_SESSION_MODES:
            raise ValueError(f"unsupported reflection session mode: {session_mode!r}")
        if depth not in REFLECTION_DEPTHS:
            raise ValueError(f"unsupported reflection depth: {depth!r}")
        if closure is None or not closure.request.HasField("memory_snapshot"):
            raise ValueError("reflection requires a logical memory snapshot")
        request = _logical_learning_request(
            outcome=outcome,
            history=history,
            outcome_commit=outcome_commit,
            closure=closure,
            guidance=self.guidance,
        )
        idempotency_key = f"reflection:{incident_id}:{outcome_commit}"
        if diagnostics:
            # Re-sending the responder transcript on every model request is the
            # dominant retry cost; the typed diagnostics are all a retry needs.
            prompt = (
                "Author corrected final files. You are correcting operational-memory edits that SDO's broker rejected. "
                "The controller has "
                "independently verified incident closure and committed its authoritative outcome; the responder's "
                "session transcript is intentionally not available.\n\n"
                f"{render_validation_feedback(validation_attempt, diagnostics)}\n"
                "The rejected edit was rolled back, so the worktree is clean at the accepted outcome commit. "
                "Author corrected final files here, then call check_memory until green.\n\n"
                f"Original reflection request:\n{request}"
            )
            return self.backend.fresh(worktree=worktree, prompt=prompt, idempotency_key=idempotency_key)
        if depth == "refine" and session_mode == "resume":
            # Verified repeat: the programs already in memory did the work, so
            # the session only confirms them. A fresh session has no transcript
            # to confirm against, so it always gets the full request.
            request = (
                refine_learning_request(outcome=outcome, closure=closure, outcome_commit=outcome_commit)
                + f"\n\n{branch_instruction('reflection')}"
            )
        if session_mode == "fresh":
            if closure is None:
                raise ValueError("a fresh first reflection attempt requires the verified closure for its brief")
            # The brief replaces the responder transcript, which a resumed
            # session would re-send on every model request.
            brief = incident_brief(
                closure,
                worktree=worktree,
                detector_detail=self.guidance in _GENERALIZING_GUIDANCE,
                event_dependence=self.guidance == "generalize-spec",
            )
            prompt = (
                "The controller has independently verified incident closure and committed its authoritative "
                "outcome. You are reflecting in a fresh session: the responder's session transcript is not "
                "available, so rely on the incident brief below and supplied memory context.\n\n"
                f"{brief}\n"
                f"Reflection request:\n{request}"
            )
            return self.backend.fresh(worktree=worktree, prompt=prompt, idempotency_key=idempotency_key)
        prompt = (
            "The controller has independently verified incident closure and committed its authoritative outcome.\n"
            "Reflect using the same incident context.\n"
            f"{request}"
        )
        try:
            return self.backend.resume(
                session_id=session_id,
                worktree=worktree,
                prompt=prompt,
                idempotency_key=idempotency_key,
            )
        except ResponderSessionUnavailable as exc:
            print(f"reflection: cannot resume responder session ({exc}); starting fresh", file=sys.stderr)
            return self.resume(
                session_id=session_id,
                incident_id=incident_id,
                worktree=worktree,
                outcome=outcome,
                history=history,
                outcome_commit=outcome_commit,
                session_mode="fresh",
                closure=closure,
                depth="full",
            )


class CodexSessionBackend:
    """Resume the responder's own Codex session to reflect on its outcome.

    Validation retries use :meth:`fresh` instead, so they never re-send the
    responder transcript.
    """

    provider: ClassVar[AgentProvider] = "codex"

    def __init__(
        self,
        *,
        model: str | None = None,
        reasoning_effort: str = INCIDENT_REASONING_EFFORT,
        timeout_seconds: int = 900,
        executor: CommandExecutor | None = None,
    ) -> None:
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout_seconds = timeout_seconds
        self.executor = executor

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
        turn = self._run(
            worktree=worktree, prompt=f"Idempotency key: {idempotency_key}\n\n{prompt}", session_id=session_id
        )
        try:
            payload = unwrap_branch(turn.output_json, "reflection")
        except WrongBranchError:
            # One resumed turn moves the answer to the right branch; a second miss is the caller's error.
            turn = self._run(worktree=worktree, prompt=branch_correction("reflection"), session_id=turn.session_id)
            payload = unwrap_branch(turn.output_json, "reflection")
        return ReflectionTurn.from_model_output(payload).with_usage(turn_usage(turn))

    def _run(self, *, worktree: Path, prompt: str, session_id: str | None) -> StructuredTurn:
        try:
            return run_structured_turn(
                self.provider,
                prompt,
                cwd=worktree,
                access="danger-full-access",
                model=self.model,
                reasoning_effort=self.reasoning_effort,
                timeout_seconds=self.timeout_seconds,
                resume_session_id=session_id,
                executor=self.executor,
                **session_turn_options(self.provider, worktree),
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
