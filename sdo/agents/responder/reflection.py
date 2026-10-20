from __future__ import annotations

import json
import os
import sys
from typing import TYPE_CHECKING, ClassVar, Literal, Protocol

from agentshim import SessionResumeError
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from libs.agent_cli.structured import AgentProvider, StructuredTurnError, run_structured_turn, turn_usage
from sdo.agents.responder.reflection_brief import incident_brief
from sdo.agents.responder.reflection_outcomes import current_outcome_view, history_view
from sdo.agents.sdk_reference import DETECTOR_SDK_REFERENCE
from sdo.agents.views import (
    OperationalArtifactDraft,  # noqa: TC001 - Pydantic resolves this field at runtime.
)
from sdo.contracts import OutcomeClassification

ReflectionGuidance = Literal["baseline", "generalize", "generalize-spec"]
ReflectionSessionMode = Literal["resume", "fresh"]
REFLECTION_GUIDANCE_MODES: tuple[ReflectionGuidance, ...] = ("baseline", "generalize", "generalize-spec")
REFLECTION_SESSION_MODES: tuple[ReflectionSessionMode, ...] = ("resume", "fresh")

if TYPE_CHECKING:
    from pathlib import Path

    from agentshim import CommandExecutor

    from sdo.agents.views import BrokerClosure
    from sdo.contracts import OutcomeRecord


def _classification_directive(classification: OutcomeClassification, guidance: ReflectionGuidance = "baseline") -> str:
    if classification is OutcomeClassification.FALSE_POSITIVE:
        return "tighten the over-broad detector signature and add a near-miss regression test."
    if classification is OutcomeClassification.FALSE_NEGATIVE:
        return "add or widen the missed detector signature and include the reproducing test that previously failed."
    if classification is OutcomeClassification.SUCCESS:
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
    proposed_changes: list[str]
    artifact_proposal: OperationalArtifactDraft | None = None
    sdk_gaps: list[SdkGap] = Field(default_factory=list)
    # Provider accounting for the turn; kept out of the model-facing schema.
    _usage: dict[str, int | float] = PrivateAttr(default_factory=dict)

    @property
    def usage(self) -> dict[str, int | float]:
        return dict(self._usage)

    def with_usage(self, usage: dict[str, int | float]) -> ReflectionTurn:
        self._usage = dict(usage)
        return self

    def model_post_init(self, __context: object) -> None:
        if self.learning_decision == "updated" and not (self.proposed_changes or self.artifact_proposal):
            raise ValueError("updated reflection requires a logical artifact proposal or legacy changes")
        if self.learning_decision == "no_change":
            if self.proposed_changes or self.artifact_proposal or self.sdk_gaps:
                raise ValueError("no_change reflection cannot propose changes")
            if self.no_change_reason is None:
                raise ValueError("no_change reflection requires no_change_reason")
        if self.learning_decision == "sdk_gap" and (
            not self.sdk_gaps or self.proposed_changes or self.artifact_proposal
        ):
            raise ValueError("sdk_gap reflection requires typed gaps without artifact changes")


class SdkGap(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fault_class: str = Field(min_length=1)
    primitive: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    scratch_script: str = Field(min_length=1)


def reflection_output_schema() -> dict[str, object]:
    """ReflectionTurn as a strict structured-output schema.

    Strict provider schemas (Codex/OpenAI) require every property to be listed
    in ``required``; optional fields are expressed as nullable instead.
    """

    schema = ReflectionTurn.model_json_schema()

    def strict_object(node: object) -> None:
        if isinstance(node, dict):
            properties = node.get("properties")
            if isinstance(properties, dict):
                for prop in properties.values():
                    if isinstance(prop, dict):
                        prop.pop("default", None)
                node["required"] = list(properties)
                node["additionalProperties"] = False
            for child in node.values():
                strict_object(child)
        elif isinstance(node, list):
            for child in node:
                strict_object(child)

    strict_object(schema)
    return schema


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


#: Upper bound on the rejected diff quoted into a retry prompt.
_MAX_REJECTED_DIFF_CHARS = 48_000

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
    closure: BrokerClosure,
    guidance: ReflectionGuidance,
) -> str:
    snapshot = closure.request.memory_snapshot
    if snapshot is None:
        raise ValueError("logical reflection requires an operational memory snapshot")
    classification = OutcomeClassification(outcome.classification)
    example_dir = os.environ.get("SDO_PLAYBOOK_PROGRAM_EXAMPLE_DIR") or "/opt/sdo/examples/playbook-programs"
    return (
        "The controller verified this incident and committed its outcome. "
        "Read the supplied logical memory snapshot and source code. Propose reusable incident "
        "detectors and playbooks as structured artifacts in artifact_proposal. "
        "Do not edit operational-memory files in the checkout. Source edits are not part of reflection. "
        "Each detector needs a stable logical ID, Kubernetes watches, Go source and tests, "
        "and the originating incident and outcome commit; for each confirmed fault you handled, make exactly "
        "one decision: compile or refine its program, reuse an existing program and explain why it was enough, "
        "or return an sdk_gap with the missing SDK primitive, reason, and the scratch script that proved the check. "
        "Record each reused program ID and its reason in summary; for no_change also use no_change_reason. "
        "Extend an existing fault class instead of adding a near duplicate. If a program wrongly matched this "
        "incident, tighten it or add a NoMatch corpus case. "
        "A program proposal needs diagnose.go, optional mitigate.go, Go tests, and a corpus case from the "
        "captured open and fixed states; bind a healthy same-kind peer in the fixed snapshot for "
        "open, fixed, and healthy replay. "
        "The controller owns these snapshots, recordings, and root-cause provenance labels. "
        "Inspect the read-only /sdo/session/corpus/open@detect.json, open@settle.json, "
        "open@pre-apply.json (when present), and fixed.json captures. "
        "The propose command selects the latest pre-mitigation open capture by default; "
        "use --open-at detect, settle, or pre-apply to choose another. "
        "Use sdo.dev/operator/sdk/program and shared .sdo/lib helpers, importing helpers through this checkout's "
        "diagnostics module path. Bind resources through Params; "
        "derive names, ports and images from bindings and the ReadView; validation replays the corpus with every "
        "name, port and image renamed and the verdicts must not change. "
        "The allowed imports are SDK, Kubernetes read types, "
        "standard read-only packages, and shared helpers; no os/exec, net, unsafe, or writes. "
        "Diagnose must Match at open and return NoMatch at fixed and healthy; Mitigate must plan actions only "
        "at open. Prefer structured container status reasons to free-text event messages in Diagnose. "
        "When the desired value comes from source or rollout history, Mitigate should return "
        "ReconcileFromSource or Rollback. See "
        f"{example_dir}/playbooks/service-targetport-mismatch/diagnose.go and "
        f"{example_dir}/corpus/targetport-mismatch/ for a complete example. "
        "Keep playbook README prose to judgement and context, "
        "with no shell commands; never persist scratch scripts. "
        "Use proposed_changes=[]; the artifact IDs are derived from artifact_proposal. "
        "Return learning_decision=updated only with a nonempty artifact_proposal. "
        "Return learning_decision=sdk_gap for gaps only, or no_change with a specific reason when existing "
        "programs cover every fault or no safe learning exists. An updated proposal may also report sdk_gaps "
        "for other faults.\n\n"
        f"{_logical_guidance(guidance)}"
        f"For this {classification.value} outcome, "
        f"{_classification_directive(classification, guidance)}\n\n"
        "For a new fault class, start with `sdo-operator program scaffold --fault-class <class> --param name=Kind "
        "[--mitigate] --out <dir>` with a scratch directory outside the checkout. "
        "Fill its Go files; do not reconstruct or edit the captured corpus. "
        "Before returning an update, run `sdo-operator program propose --dir <playbook-dir> "
        "[--lib <dir>...] --incident <id> --corpus-from-incident <id> "
        "--bind name=Kind/namespace/name --peer-bind name=Kind/namespace/name` from the proposal checkout. "
        "Use --corpus <dir> only for a synthetic NoMatch case with a recorded reason. "
        "It fills proposal owner, ID, and incident provenance, runs draft-test in a scratch projection, "
        "and prints the complete OperationalArtifactProposal ProtoJSON only after validation passes. "
        "Read every failure, fix the files, and rerun until it passes; "
        "return the printed proposal in the structured result.\n\n"
        f"{DETECTOR_SDK_REFERENCE}\n"
        f"Outcome commit: {outcome_commit}\n"
        f"Operational memory snapshot:\n{snapshot.model_dump_json(indent=2)}\n\n"
        f"Current outcome:\n{json.dumps(current_outcome_view(outcome), indent=2)}\n\n"
        f"{_history_section(outcome, history)}"
    )


def _history_section(outcome: OutcomeRecord, history: list[OutcomeRecord]) -> str:
    prior = history_view(history, current_incident_id=outcome.incident_id)
    if not prior:
        return "Outcome history (prior incidents, compact): none\n"
    return f"Outcome history (prior incidents, compact; raw evidence omitted):\n{json.dumps(prior, indent=2)}\n"


def _bounded_diff(diff: str | None) -> str:
    if not diff or not diff.strip():
        return "(the rejected proposal changed no files, or its diff is unavailable)\n"
    if len(diff) <= _MAX_REJECTED_DIFF_CHARS:
        return diff if diff.endswith("\n") else diff + "\n"
    omitted = len(diff) - _MAX_REJECTED_DIFF_CHARS
    return f"{diff[:_MAX_REJECTED_DIFF_CHARS]}\n[... diff truncated: {omitted} more characters omitted ...]\n"


class SessionReflector:
    """Reflect on a verified outcome.

    The first attempt resumes the responder session (``session_mode="resume"``)
    or, opt-in, starts a fresh session from a compact incident brief
    (``"fresh"``); a retry after a validation rejection is always a short fresh
    session. ``responder_turn_log`` is the responder's per-turn usage log, the
    source of its shell commands for the brief.
    """

    def __init__(
        self,
        backend: StatefulResponderBackend,
        *,
        responder_turn_log: Path | None = None,
        guidance: ReflectionGuidance = "baseline",
    ) -> None:
        if guidance not in REFLECTION_GUIDANCE_MODES:
            raise ValueError(f"unsupported reflection guidance: {guidance!r}")
        self.backend = backend
        self.responder_turn_log = responder_turn_log
        self.guidance: ReflectionGuidance = guidance

    def should_reflect(self, outcome: OutcomeRecord, *, health_verified: bool, session_id: str | None) -> bool:
        return bool(
            health_verified
            and session_id
            and OutcomeClassification(outcome.classification)
            in {
                OutcomeClassification.SUCCESS,
                OutcomeClassification.FALSE_POSITIVE,
                OutcomeClassification.FALSE_NEGATIVE,
            }
        )

    def resume(
        self,
        *,
        session_id: str,
        incident_id: str,
        worktree: Path,
        outcome: OutcomeRecord,
        history: list[OutcomeRecord],
        outcome_commit: str,
        validation_feedback: str | None = None,
        topology_review: object | None = None,
        rejected_proposal_diff: str | None = None,
        session_mode: ReflectionSessionMode = "resume",
        closure: BrokerClosure | None = None,
    ) -> ReflectionTurn:
        if session_mode not in REFLECTION_SESSION_MODES:
            raise ValueError(f"unsupported reflection session mode: {session_mode!r}")
        if closure is None or closure.request.memory_snapshot is None:
            raise ValueError("reflection requires a logical memory snapshot")
        request = _logical_learning_request(
            outcome=outcome,
            history=history,
            outcome_commit=outcome_commit,
            closure=closure,
            guidance=self.guidance,
        )
        idempotency_key = f"reflection:{incident_id}:{outcome_commit}"
        if validation_feedback:
            # Re-sending the responder transcript on every model request is the
            # dominant retry cost, and the rejected diff plus the validator's
            # error is all the retry needs from the first attempt.
            prompt = (
                "You are correcting an operational-memory proposal that SDO's isolated validator rejected. The "
                "controller has independently verified incident closure and committed its authoritative outcome; "
                "the responder's session transcript is intentionally not available.\n\n"
                f"Validator error:\n{validation_feedback}\n\n"
                "The rejected proposal was rolled back, so the worktree is clean at the accepted outcome commit. "
                "Rejected proposal diff against that commit (reapply what was valid, fix every reported error):\n"
                f"```diff\n{_bounded_diff(rejected_proposal_diff)}```\n\n"
                f"Original reflection request:\n{request}"
            )
            return self.backend.fresh(worktree=worktree, prompt=prompt, idempotency_key=idempotency_key)
        if session_mode == "fresh":
            if closure is None:
                raise ValueError("a fresh first reflection attempt requires the verified closure for its brief")
            # The brief replaces the responder transcript, which a resumed
            # session would re-send on every model request.
            brief = incident_brief(
                closure,
                worktree=worktree,
                responder_turn_log=self.responder_turn_log,
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
        try:
            turn = run_structured_turn(
                self.provider,
                f"Idempotency key: {idempotency_key}\n\n{prompt}",
                output_schema=reflection_output_schema(),
                cwd=worktree,
                access="danger-full-access",
                model=self.model,
                reasoning_effort=self.reasoning_effort,
                timeout_seconds=self.timeout_seconds,
                resume_session_id=session_id,
                executor=self.executor,
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
        return ReflectionTurn.model_validate_json(turn.output_json).with_usage(turn_usage(turn))


class ClaudeSessionBackend(CodexSessionBackend):
    """Resume the responder's own Claude Code session to reflect on its outcome."""

    provider: ClassVar[AgentProvider] = "claude"
