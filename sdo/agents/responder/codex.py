from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import timezone
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from google.protobuf import json_format

from libs.agent_cli.structured import AGENT_PROVIDERS, StructuredTurnError, run_structured_turn, turn_usage
from sdo.agents.prompt_templates import RenderedPrompt, named_prompt, render_prompt
from sdo.agents.provider import MODEL_ENV, resolve_agent
from sdo.agents.responder import recorded_actions
from sdo.agents.responder.continuation import continuation_prompt
from sdo.agents.responder.credentials import SKILLS_ROOT
from sdo.agents.responder.reflection import INCIDENT_REASONING_EFFORT
from sdo.agents.responder.result_output import parse_incident_result
from sdo.agents.responder.session_recovery import SessionRecoveryError, open_recovery
from sdo.agents.responder.session_turn import (
    WrongBranchError,
    branch_correction,
    branch_instruction,
    session_turn_options,
    unwrap_branch,
)
from sdo.contracts import (
    DetectorEvaluation,
    Finding,
    IncidentRequest,
    IncidentResult,
    MemoryDetector,
    StateFieldChange,
    model_output_schema,
    timestamp_datetime,
)

if TYPE_CHECKING:
    from agentshim import CommandExecutor
    from google.protobuf.timestamp_pb2 import Timestamp


class ResponderExecutionError(RuntimeError):
    pass


def main() -> int:
    try:
        request = json_format.Parse(sys.stdin.read(), IncidentRequest())
    except (ValueError, json_format.ParseError) as exc:
        print(f"invalid incident request: {exc}", file=sys.stderr)
        return 2

    try:
        result = execute_incident(request)
    except ResponderExecutionError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json_format.MessageToJson(result, preserving_proto_field_name=True))
    return 0


def execute_incident(
    request: IncidentRequest,
    *,
    model: str | None = None,
    provider: str | None = None,
    executor: CommandExecutor | None = None,
) -> IncidentResult:
    if provider is None:
        try:
            resolved = resolve_agent("responder", "responder")
        except ValueError as exc:
            raise ResponderExecutionError(str(exc)) from exc
        selected_provider, model = resolved.provider, model or resolved.model
    else:
        selected_provider = provider.strip().lower()
        if selected_provider not in AGENT_PROVIDERS:
            raise ResponderExecutionError(f"unsupported agent provider {selected_provider!r}")
        model = model or os.getenv(MODEL_ENV["responder"]) or None
    prompt = _responder_prompt(request)
    try:
        recovery = open_recovery(request.incident_id, selected_provider)
        resume_session_id = recovery.resume_session_id() if recovery is not None else None
    except SessionRecoveryError as exc:
        raise ResponderExecutionError(str(exc)) from exc
    if resume_session_id is None:
        recorded_actions.reset()
    if recovery is not None:
        prompt += recovery.recovered_prompt()
    continuation = os.environ.get("SDO_RESPONSE_CONTINUATION", "")
    if continuation:
        if recovery is None or resume_session_id is None:
            raise ResponderExecutionError("controller continuation requires the existing provider session")
        try:
            prompt = continuation_prompt(continuation)
        except ValueError as exc:
            raise ResponderExecutionError(f"invalid controller continuation: {exc}") from exc
    corrected_branch = False
    while True:
        try:
            completed = run_structured_turn(
                selected_provider,  # type: ignore[arg-type]
                prompt,
                cwd=request.repository_worktree,
                access="danger-full-access",
                model=model,
                reasoning_effort=INCIDENT_REASONING_EFFORT,
                timeout_seconds=recovery.remaining_seconds() if recovery is not None else None,
                resume_session_id=resume_session_id,
                executor=executor,
                on_session_started=recovery.checkpoint if recovery is not None else None,
                **session_turn_options(selected_provider, Path(request.repository_worktree)),
            )
        except SessionRecoveryError as exc:
            raise ResponderExecutionError(str(exc)) from exc
        except StructuredTurnError as exc:
            raise ResponderExecutionError(f"{selected_provider} responder failed: {exc}") from exc
        try:
            result = parse_incident_result(
                unwrap_branch(completed.output_json, "incident_result"), turn_usage(completed)
            )
            break
        except WrongBranchError as exc:
            if corrected_branch:
                raise ResponderExecutionError(f"invalid {selected_provider} incident result: {exc}") from exc
            corrected_branch = True
            resume_session_id = completed.session_id
            prompt = branch_correction("incident_result")
        except (OSError, ValueError) as exc:
            raise ResponderExecutionError(f"invalid {selected_provider} incident result: {exc}") from exc
    try:
        if recovery is not None:
            recovery.acknowledge_reports()
    except SessionRecoveryError as exc:
        raise ResponderExecutionError(str(exc)) from exc
    try:
        recorded_actions.merge_into(result)
    except recorded_actions.RecordedActionsError as exc:
        raise ResponderExecutionError(f"invalid recorded repair actions: {exc}") from exc
    if not result.HasField("responder_session_id"):
        result.responder_session_id = completed.session_id
    if result.incident_id != request.incident_id:
        raise ResponderExecutionError(f"{selected_provider} result incident_id does not match request")
    return result


#: Proto fields the model does not supply: worker accounting and session, the controller's
#: program-loop state, and the detector states and artifact IDs the responder never reports.
_MODEL_OMITS: dict[str, tuple[str, ...]] = {
    "sdodev.contracts.v1alpha1.IncidentResult": (
        "schema_version",
        "final_detector_states",
        "proposed_artifact_ids",
        "usage",
        "responder_session_id",
        "error",
        "late_findings_pull",
        "program_tool_calls",
        "repair_actions",
        "program_runs",
        "plan_applications",
        "response_loops_used",
        "response_loop_exhausted",
        "program_failure_signals",
        "resolution_path",
    ),
    "sdodev.contracts.v1alpha1.AppliedPlaybook": ("parameter_bindings", "script_names"),
    "sdodev.contracts.v1alpha1.DriftEvent": (
        "source",
        "operation",
        "observed_at",
        "program_id",
        "manager_at",
        "investigation_status",
        "investigation_capture_id",
    ),
}


def _incident_result_schema() -> dict[str, object]:
    """The model-facing schema of ``IncidentResult``, generated from the proto the Go loader reads."""

    return model_output_schema(IncidentResult.DESCRIPTOR, omit=_MODEL_OMITS)


#: Upper bounds on playbook text inlined into a warm-path prompt.
_WARM_PLAYBOOK_MAX_CHARS = 8_000


@dataclass(frozen=True)
class WarmPlaybookMatch:
    path: str
    detector: MemoryDetector
    findings: tuple[Finding, ...]
    sources: tuple[str, ...]
    prior_incidents: tuple[str, ...]

    @property
    def detector_fired(self) -> bool:
        return bool(self.findings)


@dataclass(frozen=True)
class WarmPlaybook:
    """A validated incident playbook inlined for an exact-match incident."""

    path: str
    text: str
    scripts: tuple[tuple[str, str], ...]
    match: WarmPlaybookMatch

    def __post_init__(self) -> None:
        if self.path != self.match.path:
            raise ValueError("warm playbook path must equal its match path")

    @property
    def findings(self) -> tuple[Finding, ...]:
        return self.match.findings

    def precondition_script(self) -> str | None:
        """The playbook's diagnose (preferred) or verify script, for a not-yet-fired detector's sanity check."""

        for keyword in _PRECONDITION_SCRIPT_KEYWORDS:
            for script_path, _text in self.scripts:
                if keyword in PurePosixPath(script_path).stem.lower():
                    return script_path
        return None


#: Script-name keywords, in preference order, for the one sanity check of a not-yet-fired detector.
_PRECONDITION_SCRIPT_KEYWORDS = ("diagnose", "check", "verify")


def _bounded(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return render_prompt(
        "responder", "bounded_01", part="responder.instructions", text=text[:limit], len=len(text) - limit
    )


def warm_playbooks(request: IncidentRequest) -> list[WarmPlaybook]:
    """Select warm playbooks from a commit-bound logical memory snapshot."""

    return _logical_warm_playbooks(request)


def _logical_warm_playbooks(request: IncidentRequest) -> list[WarmPlaybook]:
    """Select validated warm memory using stable IDs and broker-supplied content."""

    snapshot = request.memory_snapshot
    if not request.HasField("memory_snapshot") or snapshot.accepted_commit != request.repository_base_commit:
        return []
    exact = [
        item
        for item in request.relevant_outcomes
        if item.match_reason in {"exact-fingerprint", "learned-detector-origin"}
    ]
    if not exact:
        return []
    prior_ids = tuple(sorted({item.incident_id for item in exact}))
    prior_applied = {item for outcome in exact for item in outcome.applied_playbook_ids}
    surfaced = {item.playbook_id for item in request.surfaced_playbooks}
    available = {item.id: item for item in snapshot.playbooks}
    matches: list[WarmPlaybook] = []
    for detector in snapshot.detectors:
        if detector.detector_class != "incident" or detector.owner != "responder":
            continue
        findings = tuple(
            item for item in request.findings if item.status == "active" and item.detector_id == detector.id
        )
        finding_ids = {item for finding in findings for item in finding.playbook_ids}
        for playbook_id in detector.possible_playbook_ids:
            playbook = available.get(playbook_id)
            if playbook is None:
                continue
            sources = tuple(
                source
                for source, holds in (
                    ("active-finding", playbook_id in finding_ids),
                    ("surfaced", playbook_id in surfaced),
                    ("prior-outcome-applied", playbook_id in prior_applied),
                    ("detector-learned-from-exact-prior", detector.originating_incident in prior_ids),
                )
                if holds
            )
            if not sources:
                continue
            match = WarmPlaybookMatch(
                path=playbook_id,
                detector=detector,
                findings=findings,
                sources=sources,
                prior_incidents=prior_ids,
            )
            matches.append(
                WarmPlaybook(
                    path=playbook_id,
                    text=_bounded(playbook.markdown, _WARM_PLAYBOOK_MAX_CHARS),
                    scripts=tuple((script.name, script.content) for script in playbook.scripts),
                    match=match,
                )
            )
    return sorted(matches, key=lambda item: (item.path, item.match.detector.id))


#: goal.md is inlined only when it is at most this long; otherwise it stays a path.
_GOAL_INLINE_MAX_CHARS = 4_000


def _detector_evidence(request: IncidentRequest) -> str:
    """The latest evaluation per detector, plus its latest firing one when that is older."""

    latest: dict[str, DetectorEvaluation] = {}
    latest_firing: dict[str, DetectorEvaluation] = {}
    for evaluation in sorted(request.detector_history, key=lambda item: item.evaluated_at.ToNanoseconds()):
        latest[evaluation.detector_id] = evaluation
        if evaluation.status == "firing":
            latest_firing[evaluation.detector_id] = evaluation

    def line(evaluation: DetectorEvaluation) -> str:
        text = f"{evaluation.detector_id}: {evaluation.status} at {_timestamp(evaluation.evaluated_at)}"
        if evaluation.fingerprints:
            text += f" [{', '.join(evaluation.fingerprints)}]"
        if evaluation.error:
            text += f" error: {evaluation.error}"
        return f"- {text}\n"

    lines = []
    for detector_id in sorted(latest):
        firing = latest_firing.get(detector_id)
        if firing is not None and firing is not latest[detector_id]:
            lines.append(line(firing))
        lines.append(line(latest[detector_id]))
    return render_prompt("responder", "detector_evidence_01", part="responder.instructions") + "".join(lines)


def _timestamp(value: Timestamp) -> str:
    return timestamp_datetime(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _inlined_health_objective(request: IncidentRequest) -> str:
    snapshot = request.memory_snapshot
    if not request.HasField("memory_snapshot"):
        raise ValueError("incident responder requires a logical memory snapshot")
    text = snapshot.goal_markdown
    if text and len(text) <= _GOAL_INLINE_MAX_CHARS:
        return render_prompt(
            "responder", "inlined_health_objective_01", part="responder.instructions", text=text.rstrip()
        )
    return ""


#: arch.md is inlined only when it is at most this long; otherwise it stays a path. Deployer-owned and
#: read-only for the responder, so it is capped like the warm playbook text rather than goal.md's tighter
#: bound. Without this, every incident (cold and warm) pays a separate shell round-trip to read it.
_ARCH_INLINE_MAX_CHARS = 8_000


def _inlined_architecture_summary(request: IncidentRequest) -> str:
    snapshot = request.memory_snapshot
    if not request.HasField("memory_snapshot"):
        raise ValueError("incident responder requires a logical memory snapshot")
    text = snapshot.architecture_markdown
    if text and len(text) <= _ARCH_INLINE_MAX_CHARS:
        return render_prompt(
            "responder", "inlined_architecture_summary_01", part="responder.instructions", text=text.rstrip()
        )
    return ""


def _cold_instructions() -> str:
    return render_prompt("responder", "cold_instructions_01", part="responder.instructions")


def _cold_surfaced_playbook_context(request: IncidentRequest) -> str:
    """Inline bounded broker-supplied playbook content when no warm match exists."""

    snapshot = request.memory_snapshot
    if not request.HasField("memory_snapshot"):
        return ""
    relevant_ids = {
        *[item.playbook_id for item in request.surfaced_playbooks],
        *(playbook_id for finding in request.findings for playbook_id in finding.playbook_ids),
    }
    available = {item.id: item for item in snapshot.playbooks}
    entries = []
    for playbook_id in sorted(relevant_ids & available.keys())[:4]:
        playbook = available[playbook_id]
        scripts = "".join(
            render_prompt(
                "responder",
                "cold_surfaced_playbook_context_01",
                part="responder.instructions",
                name=script.name,
                bounded=_bounded(script.content, 2_000),
            )
            for script in playbook.scripts[:4]
        )
        entries.append(
            render_prompt(
                "responder",
                "cold_surfaced_playbook_context_02",
                part="responder.instructions",
                playbook_id=playbook_id,
                bounded=_bounded(playbook.markdown, _WARM_PLAYBOOK_MAX_CHARS),
                scripts=scripts,
            )
        )
    return "\n".join(entries) + "\n" if entries else ""


def _fired_evidence(playbook: WarmPlaybook) -> str:
    findings = ", ".join(
        sorted(
            {
                render_prompt(
                    "responder",
                    "fired_evidence_01",
                    part="responder.instructions",
                    detector_id=finding.detector_id,
                    fingerprint=finding.fingerprint or finding.rule_id,
                )
                for finding in playbook.findings
            }
        )
    )
    return render_prompt(
        "responder", "fired_evidence_02", part="responder.instructions", path=playbook.path, findings=findings
    )


def _pending_evidence(playbook: WarmPlaybook, namespace: str) -> str:
    detector = playbook.match.detector
    kinds = ", ".join(
        sorted(
            {
                render_prompt(
                    "responder",
                    "pending_evidence_01",
                    part="responder.instructions",
                    kind=watch.kind,
                    api_version=watch.api_version,
                )
                for watch in detector.watches
            }
        )
    )
    watched = (
        render_prompt("responder", "pending_evidence_02", part="responder.instructions", kinds=kinds)
        if kinds
        else render_prompt("responder", "pending_evidence_03", part="responder.instructions")
    )
    script = playbook.precondition_script()
    script_step = (
        render_prompt("responder", "pending_evidence_04", part="responder.instructions", script=script)
        if script is not None
        else ""
    )
    return render_prompt(
        "responder",
        "pending_evidence_05",
        part="responder.instructions",
        path=playbook.path,
        id=detector.id,
        watched=watched,
        namespace=namespace,
        script_step=script_step,
    )


def _warm_instructions(playbooks: list[WarmPlaybook], namespace: str) -> str:
    evidence = "".join(
        _fired_evidence(playbook) if playbook.match.detector_fired else _pending_evidence(playbook, namespace)
        for playbook in playbooks
    )
    priors = sorted({incident for playbook in playbooks for incident in playbook.match.prior_incidents})
    sections = []
    for playbook in playbooks:
        section = render_prompt(
            "responder",
            "warm_instructions_01",
            part="responder.instructions",
            path=playbook.path,
            text=playbook.text.rstrip(),
        )
        for script_path, script_text in playbook.scripts:
            section += render_prompt(
                "responder",
                "warm_instructions_02",
                part="responder.instructions",
                script_path=script_path,
                script_text=script_text.rstrip(),
            )
        sections.append(section)
    return (
        render_prompt(
            "responder",
            "warm_instructions_03",
            part="responder.instructions",
            join=", ".join(priors),
            evidence=evidence,
        )
        + "\n".join(sections)
        + "\n"
    )


def _field_change(field: StateFieldChange) -> str:
    if not field.HasField("before") and not field.HasField("after"):
        return field.field
    if not field.HasField("before"):
        return render_prompt(
            "responder", "field_change_01", part="responder.instructions", field=field.field, after=field.after
        )
    return render_prompt(
        "responder",
        "field_change_02",
        part="responder.instructions",
        field=field.field,
        before=field.before,
        after=field.after or "(none)",
    )


def _state_changes_section(request: IncidentRequest) -> str:
    """Render the controller's configuration diff against the last healthy baseline."""

    changes = request.state_changes
    if not request.HasField("state_changes"):
        return ""
    window = render_prompt(
        "responder",
        "state_changes_section_01",
        part="responder.instructions",
        timestamp=_timestamp(changes.baseline_at),
        timestamp_2=_timestamp(changes.observed_at),
    )
    unobserved = (
        render_prompt(
            "responder",
            "state_changes_section_02",
            part="responder.instructions",
            join=", ".join(changes.unobserved_kinds),
        )
        if changes.unobserved_kinds
        else ""
    )
    if not changes.changes and not changes.omitted:
        return render_prompt(
            "responder", "state_changes_section_03", part="responder.instructions", window=window, unobserved=unobserved
        )
    lines = []
    for change in changes.changes:
        line = render_prompt(
            "responder",
            "state_changes_section_04",
            part="responder.instructions",
            kind=change.kind,
            name=change.name,
            change=change.change,
        )
        if change.fields:
            line += ": " + "; ".join(_field_change(field) for field in change.fields)
        lines.append(line + "\n")
    omitted = (
        render_prompt("responder", "state_changes_section_05", part="responder.instructions", omitted=changes.omitted)
        if changes.omitted
        else ""
    )
    return render_prompt(
        "responder",
        "state_changes_section_06",
        part="responder.instructions",
        window=window,
        join="".join(lines),
        omitted=omitted,
        unobserved=unobserved,
    )


def _verification_instructions() -> str:
    return render_prompt("responder", "verification_instructions_01", part="responder.instructions")


def _no_action_instructions() -> str:
    return render_prompt("responder", "no_action_instructions_01", part="responder.instructions")


LATE_FINDINGS_ENV = "SDO_LATE_FINDINGS"
LATE_FINDINGS_MODES = ("off", "pull")
LATE_FINDINGS_COMMAND = "sdo-operator late-findings"


def late_findings_guidance(request: IncidentRequest, *, mode: str | None = None) -> str:
    """Pull-before-act paragraph; empty unless the ``pull`` mode is selected (default: the environment)."""

    selected = (os.getenv(LATE_FINDINGS_ENV, "") if mode is None else mode).strip() or "off"
    if selected not in LATE_FINDINGS_MODES:
        raise ValueError(f"late-findings mode must be one of {', '.join(LATE_FINDINGS_MODES)}: {selected!r}")
    if selected == "off":
        return ""
    command = render_prompt(
        "responder",
        "late_findings_guidance_01",
        part="responder.instructions",
        late_findings_command=LATE_FINDINGS_COMMAND,
        incident_id=request.incident_id,
    )
    return render_prompt("responder", "late_findings_guidance_02", part="responder.instructions", command=command)


def _prompt_request(request: IncidentRequest) -> dict[str, object]:
    """The request as the prompt shows it; the bulky sections are rendered separately."""

    document = json_format.MessageToDict(request, preserving_proto_field_name=True)
    for omitted in ("detector_history", "state_changes", "memory_snapshot"):
        document.pop(omitted, None)
    runs = document.get("initial_program_runs", [])
    relevant = []
    omitted = 0
    for run in runs:
        if run.get("verdict", {}).get("result") == "RESULT_NO_MATCH":
            omitted += 1
            continue
        capability = run.get("can_mitigate")
        run["capability"] = (
            "unknown" if capability is None else "diagnosis-and-mitigation" if capability else "diagnosis-only"
        )
        relevant.append(run)
    if runs:
        document["initial_program_runs"] = relevant
        document["omitted_no_match_program_runs"] = omitted
    return document


def _responder_prompt(request: IncidentRequest) -> RenderedPrompt:
    if not request.HasField("memory_snapshot"):
        raise ValueError("incident responder requires a logical memory snapshot")
    extra_instructions = os.getenv("SDO_RESPONDER_EXTRA_INSTRUCTIONS", "").strip()
    additional_context = (
        render_prompt("responder", "incident_01", part="responder.instructions", extra_instructions=extra_instructions)
        if extra_instructions
        else ""
    )
    playbooks = warm_playbooks(request)
    strategy = _warm_instructions(playbooks, request.namespace) if playbooks else _cold_instructions()
    surfaced_context = "" if playbooks else _cold_surfaced_playbook_context(request)
    memory_guard = render_prompt("responder", "incident_02", part="responder.instructions")
    program_guidance = (
        render_prompt("responder", "incident_03", part="responder.instructions")
        if request.max_response_loops > 0
        else ""
    )
    drift_guidance = (
        render_prompt("responder", "incident_04", part="responder.instructions") if request.drift_events else ""
    )
    request_json = json.dumps(_prompt_request(request), indent=2)
    return render_prompt(
        "responder",
        "incident",
        part="responder.instructions",
        incident_id=request.incident_id,
        skills_root=SKILLS_ROOT,
        strategy=named_prompt("responder.strategy", strategy),
        program_guidance=named_prompt("responder.program_guidance", program_guidance),
        drift_guidance=named_prompt("responder.drift_guidance", drift_guidance),
        surfaced_context=named_prompt("responder.surfaced_context", surfaced_context),
        no_action_instructions=named_prompt("responder.no_action_instructions", _no_action_instructions()),
        verification_instructions=named_prompt("responder.verification_instructions", _verification_instructions()),
        memory_guard=named_prompt("responder.memory_guard", memory_guard),
        branch_instruction=branch_instruction("incident_result"),
        repair_policy=request.repair_policy,
        late_findings_guidance=named_prompt("responder.late_findings_guidance", late_findings_guidance(request)),
        additional_context=named_prompt("responder.additional_context", additional_context),
        inlined_architecture_summary=named_prompt(
            "responder.inlined_architecture_summary", _inlined_architecture_summary(request)
        ),
        inlined_health_objective=named_prompt("responder.inlined_health_objective", _inlined_health_objective(request)),
        detector_evidence=named_prompt("responder.detector_evidence", _detector_evidence(request)),
        state_changes_section=named_prompt("responder.state_changes_section", _state_changes_section(request)),
        request_json=request_json,
    )


if __name__ == "__main__":
    raise SystemExit(main())
