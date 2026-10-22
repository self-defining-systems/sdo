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
from sdo.agents.provider import MODEL_ENV, resolve_agent
from sdo.agents.responder import recorded_actions
from sdo.agents.responder.reflection import INCIDENT_REASONING_EFFORT
from sdo.agents.responder.result_output import EvidenceReferenceError, parse_incident_result
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
    recorded_actions.reset()
    resume_session_id: str | None = None
    corrected_evidence = corrected_branch = False
    while True:
        try:
            completed = run_structured_turn(
                selected_provider,  # type: ignore[arg-type]
                prompt,
                cwd=request.repository_worktree,
                access="danger-full-access",
                model=model,
                reasoning_effort=INCIDENT_REASONING_EFFORT,
                resume_session_id=resume_session_id,
                executor=executor,
                **session_turn_options(selected_provider, Path(request.repository_worktree)),
            )
        except StructuredTurnError as exc:
            raise ResponderExecutionError(f"{selected_provider} responder failed: {exc}") from exc
        try:
            result = parse_incident_result(
                unwrap_branch(completed.output_json, "incident_result"), turn_usage(completed)
            )
            break
        except EvidenceReferenceError as exc:
            if corrected_evidence:
                raise ResponderExecutionError(f"invalid {selected_provider} incident result: {exc}") from exc
            # One resumed turn lets the agent split a compound reference instead of losing the diagnosis.
            corrected_evidence = True
            resume_session_id = completed.session_id
            prompt = _evidence_reference_feedback(str(exc))
        except WrongBranchError as exc:
            if corrected_branch:
                raise ResponderExecutionError(f"invalid {selected_provider} incident result: {exc}") from exc
            corrected_branch = True
            resume_session_id = completed.session_id
            prompt = branch_correction("incident_result")
        except (OSError, ValueError) as exc:
            raise ResponderExecutionError(f"invalid {selected_provider} incident result: {exc}") from exc
    try:
        recorded_actions.merge_into(result)
    except recorded_actions.RecordedActionsError as exc:
        raise ResponderExecutionError(f"invalid recorded repair actions: {exc}") from exc
    if not result.HasField("responder_session_id"):
        result.responder_session_id = completed.session_id
    if result.incident_id != request.incident_id:
        raise ResponderExecutionError(f"{selected_provider} result incident_id does not match request")
    return result


def _evidence_reference_feedback(problems: str) -> str:
    return (
        "Your result was not accepted: a state-change evidence source must name exactly one object. "
        f"Problems: {problems}\n"
        "Resubmit the same result with each changed object as its own evidence entry, for example "
        '{"kind": "state-change", "source": "Service/geo", ...} and '
        '{"kind": "state-change", "source": "Service/recommendation", ...}. '
        "Change nothing else unless it is wrong."
    )


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
        "fast_path_programs",
        "fast_path_skipped_reason",
        "fast_path_relation_covered_findings",
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
    return f"{text[:limit]}\n[... truncated: {len(text) - limit} more characters omitted ...]\n"


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
    return "Latest detector evidence (compacted from detector_history):\n" + "".join(lines)


def _timestamp(value: Timestamp) -> str:
    return timestamp_datetime(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _inlined_health_objective(request: IncidentRequest) -> str:
    snapshot = request.memory_snapshot
    if not request.HasField("memory_snapshot"):
        raise ValueError("incident responder requires a logical memory snapshot")
    text = snapshot.goal_markdown
    if text and len(text) <= _GOAL_INLINE_MAX_CHARS:
        return f"Health objective (trusted memory snapshot):\n{text.rstrip()}\n\n"
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
        return f"Architecture summary (trusted memory snapshot):\n{text.rstrip()}\n\n"
    return ""


def _cold_instructions() -> str:
    return (
        "The incident request may contain compact relevant_outcomes selected deterministically from prior verified "
        "successes. Treat them as hypotheses, compare them with live state, and adapt or reject them explicitly; "
        "never replay a prior action without confirming its assumptions. "
        "Confirm a surfaced playbook against live state before applying it, repair the source of truth, redeploy when "
        "needed, and collect evidence that the independent health objective is restored. "
    )


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
            f"Script {script.name}:\n{_bounded(script.content, 2_000)}\n" for script in playbook.scripts[:4]
        )
        entries.append(
            f"Playbook {playbook_id} (hypothesis; verify against live state):\n"
            f"{_bounded(playbook.markdown, _WARM_PLAYBOOK_MAX_CHARS)}\n{scripts}"
        )
    return "\n".join(entries) + "\n" if entries else ""


def _fired_evidence(playbook: WarmPlaybook) -> str:
    findings = ", ".join(
        sorted({f"{finding.detector_id} ({finding.fingerprint or finding.rule_id})" for finding in playbook.findings})
    )
    return (
        f"- `{playbook.path}`: the finding(s) {findings} come from its validated incident detector. That evidence "
        "already establishes the playbook's preconditions: do not re-diagnose them. The sanity check confirms the "
        "specific finding evidence is still live (for example the missing resource is still absent and the "
        "affected workload still fails); it replaces the playbook's own diagnosis steps.\n"
    )


def _pending_evidence(playbook: WarmPlaybook, namespace: str) -> str:
    detector = playbook.match.detector
    kinds = ", ".join(sorted({f"{watch.kind} ({watch.api_version})" for watch in detector.watches}))
    watched = f"the detector's watched resources ({kinds})" if kinds else "the resources the playbook names"
    script = playbook.precondition_script()
    script_step = (
        f" Run `{script}` inside that check with the incident's values; a verify script is expected to fail "
        "before the repair, and its failure must point at the playbook's fault."
        if script is not None
        else ""
    )
    return (
        f"- `{playbook.path}`: owned by the registered incident detector `{detector.id}`, which has not fired for "
        "this incident, so no incident-detector evidence exists yet. The sanity check must confirm the playbook's "
        f"preconditions itself: inspect {watched} in namespace `{namespace}` "
        "(plus the pods and recent events of the affected workload) against the preconditions the playbook "
        f"states.{script_step}\n"
    )


def _warm_instructions(playbooks: list[WarmPlaybook], namespace: str) -> str:
    evidence = "".join(
        _fired_evidence(playbook) if playbook.match.detector_fired else _pending_evidence(playbook, namespace)
        for playbook in playbooks
    )
    priors = sorted({incident for playbook in playbooks for incident in playbook.match.prior_incidents})
    sections = []
    for playbook in playbooks:
        section = f"--- Playbook `{playbook.path}` (inlined; do not re-read it) ---\n{playbook.text.rstrip()}\n"
        for script_path, script_text in playbook.scripts:
            section += f"--- Playbook script `{script_path}` ---\n{script_text.rstrip()}\n"
        sections.append(section)
    return (
        "Warm path: validated incident memory matches this incident. "
        f"The prior verified outcome(s) {', '.join(priors)} match this incident by exact fingerprint, and each "
        "playbook below is owned by a responder-owned incident detector that SDO's commit broker validated "
        "(matching and near-miss tests) and learned from a prior independently verified success. Do not re-read "
        "the playbook, and do not explore the repository or cluster beyond what the steps below need.\n"
        f"Playbook evidence:\n{evidence}"
        "Fast procedure:\n"
        "1. Run one combined sanity check: a single shell command covering the evidence above.\n"
        "2. If it agrees, submit the diagnosis through any configured environment-specific channel, then apply the "
        "playbook's repair exactly (use its scripts when present).\n"
        "3. Run the playbook's verification and `python3 -m sdo.agents.responder.status_cli` together "
        "in one command, then, "
        "once it reports healthy, submit mitigation through any configured channel and return "
        "the IncidentResult, listing the applied logical playbook ID in applied_playbooks.\n"
        "Fall back to a full investigation only if the sanity check contradicts the playbook's preconditions, the "
        "repair fails, or the verification fails; then treat relevant_outcomes as hypotheses and confirm "
        "assumptions against live state. Investigate any other active finding the playbook's fault does not "
        "explain normally.\n\n" + "\n".join(sections) + "\n"
    )


def _field_change(field: StateFieldChange) -> str:
    if not field.HasField("before") and not field.HasField("after"):
        return field.field
    if not field.HasField("before"):
        return f"{field.field}: {field.after}"
    return f"{field.field}: {field.before} -> {field.after or '(none)'}"


def _state_changes_section(request: IncidentRequest) -> str:
    """Render the controller's configuration diff against the last healthy baseline."""

    changes = request.state_changes
    if not request.HasField("state_changes"):
        return ""
    window = f"baseline {_timestamp(changes.baseline_at)}, observed {_timestamp(changes.observed_at)}"
    unobserved = (
        f"(not observed: {', '.join(changes.unobserved_kinds)}; changes to these kinds are unknown)\n"
        if changes.unobserved_kinds
        else ""
    )
    if not changes.changes and not changes.omitted:
        return (
            "No Service, workload, NetworkPolicy, ConfigMap, Secret, or RBAC object changed since the last healthy "
            f"state ({window}). The fault is likely not a configuration change in these kinds (for example a "
            "process, data, permission-inside-a-database, or dependency fault); objects present then existed "
            "unchanged while the application was healthy.\n"
            f"{unobserved}\n"
        )
    lines = []
    for change in changes.changes:
        line = f"- {change.kind}/{change.name} {change.change}"
        if change.fields:
            line += ": " + "; ".join(_field_change(field) for field in change.fields)
        lines.append(line + "\n")
    omitted = f"({changes.omitted} more changes omitted)\n" if changes.omitted else ""
    return (
        f"Changes since the last healthy state ({window}; SDO's deterministic configuration diff, with "
        "ConfigMap and Secret values shown only as digests):\n"
        f"{''.join(lines)}{omitted}{unobserved}"
        "A change listed here happened after the application was last verified healthy and is a prime suspect. "
        "Objects not listed existed unchanged while the application was healthy, so they did not cause a new "
        "incident on their own. If no listed change explains the symptoms, the fault is likely not a "
        "configuration change in these kinds.\n\n"
    )


def _verification_instructions() -> str:
    return (
        "Prefer one blocking command over checking in on a backgrounded one: give a command enough time to "
        "finish inside a single call (for example `kubectl rollout status deployment/<NAME> --timeout=60s` or "
        "`kubectl wait --for=condition=Ready pod -l <SELECTOR> --timeout=60s`) rather than starting it in the "
        "background and returning in a later turn to check on it; each check-in is a separate model turn that "
        "resends the whole conversation so far.\n\n"
        "Verify before you submit or return: after the repair, run one `sdo verify` and "
        "`python3 -m sdo.agents.responder.status_cli` in a single shell call, with no verification script of your "
        "own (exit 0 healthy, 1 unhealthy, 3 unavailable; about 3-5 seconds). It runs the health judge's verify "
        "burst through SDO's isolated prober, the same synthetic traffic the controller requires before it closes "
        "this incident. "
        "It also reports unhealthy while the controller's other health detectors still fire (a fault traffic "
        "cannot see yet), and it lists configuration that changed after this request was taken, which the "
        "request's state diff lacks. "
        "Do not submit mitigation through any channel, and do not return a completed result, until it reports "
        "healthy. When it reports unhealthy, its failing scenarios, status codes, and request paths are live "
        "evidence: a change that leaves them failing did not fix the incident, however plausible the artifact it "
        "addressed, so keep investigating along those request paths. Only when it reports unavailable, rely on "
        "`sdo verify` and plain `kubectl get`/`rollout status` checks. Record its final output as a "
        "verification_evidence entry named `sdo-incident-status`.\n\n"
        "Every confirmed root cause needs live evidence: a detector finding (source: its detector ID or "
        "finding fingerprint), a failing "
        "synthetic scenario (source: its scenario ID), a change listed since the last healthy state (source: its "
        "exactly one `Kind/name` or `Kind/namespace/name`, never a list or a compound like `Service/a; Service/b`: "
        "when one cause changed two objects, give one state-change entry per object; a change your own repair "
        "made is not evidence of the cause, and SDO checks that the change predates your repair of that object), "
        "or a live observation (source: the command; observation: what its output showed). Scripts, "
        "manifests, ConfigMap bodies, source files, and architecture notes show what could go wrong, not what did: "
        "list them only in static_context. Name in explained_detectors the detectors whose findings the cause "
        "explains; they must clear after your fix, and SDO checks that they do.\n\n"
        "Label pods or Jobs you create only to investigate or check with `sdo.dev/responder-helper=true`; the "
        "controller deletes them when you finish. Never label application workloads with it. "
        "Clean up the temporary state changes you introduced while troubleshooting before you submit.\n\n"
    )


def _no_action_instructions() -> str:
    return (
        "Some triggering findings are transient and self-heal: if `python3 -m sdo.agents.responder.status_cli` already "
        "reports healthy before you make any change, or clears on its own while you are still investigating and "
        "nothing you did explains it, do not fabricate a repair. Return status `cancelled`, with no repair "
        "actions and no applied playbooks, and say in your summary that the finding had already cleared. Return "
        "`completed` only when a repair you made or recorded was needed and applied, or when all remaining drift "
        "has an evidence-grounded benign disposition; a `completed` result with no "
        "successful repair action, submitted while every triggering finding is already clear, is treated as that "
        "same no-op, never credited as a mitigation, and never learned from, so returning `cancelled` costs "
        "nothing extra. Never return `completed` while the health objective is still failing: a completion claim "
        "with no repair evidence is rejected outright when nothing is confirmed fixed.\n\n"
    )


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
    command = f"{LATE_FINDINGS_COMMAND} --incident-id {request.incident_id}"
    return (
        "Late findings: evidence for this incident can arrive after you start, and this request cannot change. "
        f"Run `{command}` once now, and again immediately before your first change to the cluster or the "
        "repository. It is read-only and prints compact JSON listing findings that activated after this request "
        "was built, each with surfaced logical playbook IDs. Treat every returned playbook as a hypothesis: read it, "
        "confirm its preconditions against live state, adapt or reject it, and never replay its steps blindly. "
        "An empty list means nothing more has arrived yet.\n\n"
    )


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


def _responder_prompt(request: IncidentRequest) -> str:
    if not request.HasField("memory_snapshot"):
        raise ValueError("incident responder requires a logical memory snapshot")
    extra_instructions = os.getenv("SDO_RESPONDER_EXTRA_INSTRUCTIONS", "").strip()
    additional_context = (
        f"\nAdditional environment-specific instructions:\n{extra_instructions}\n" if extra_instructions else ""
    )
    playbooks = warm_playbooks(request)
    strategy = _warm_instructions(playbooks, request.namespace) if playbooks else _cold_instructions()
    surfaced_context = "" if playbooks else _cold_surfaced_playbook_context(request)
    memory_guard = (
        "Operational memory is read-only. The controller verifies recovery and records the authoritative outcome; "
        "a separate reflection task may later edit operational memory in its own checkout. "
        "Report applied playbooks by logical playbook_id, with no repository memory paths. "
    )
    program_guidance = (
        "Start with ranked program verdicts and evidence in initial_program_runs. "
        "Prefer a matching program's plan when can_mitigate=true; a diagnosis-only program supplies evidence, "
        "not a repair. Investigate Unknown or NeedsJudgement against current state and accepted source. "
        "The local sdo interface exposes concise capability help and resource argument schemas. "
        "Use that index instead of broad tool-catalog output. "
        "Finish with independent verification; return once findings are clear and source changes are committed. "
        if request.max_response_loops > 0
        else ""
    )
    drift_guidance = (
        "Unexplained drift is a question: first determine whether it is a fault. Before any mutation, "
        "establish a concrete contradiction with accepted source AND a violated application relation or "
        "observed impact using captured or tool observations. Source absence and missing containerPort "
        "declarations alone are insufficient evidence. Missing evidence means unsure: take no action and "
        "leave the finding unresolved. Quiet health alone does not establish benign drift. "
        "For a checked benign observation, return its exact ref, fields and manager in drift_events with "
        "investigation_verdict benign, a concrete reason and observation evidence. A wholly benign no-op "
        "may return completed with no repairs; it earns no mitigation or learning credit. "
        "For a fault, repair only after the evidence, follow the configured repair policy, and verify every "
        "retained drift finding as well as health. Pull status/diagnose to read late drift before acting. "
        if request.drift_events
        else ""
    )
    request_json = json.dumps(_prompt_request(request), indent=2)
    return (
        f"You are the SDO incident responder for incident {request.incident_id}.\n\n"
        "Work autonomously in the supplied repository and Kubernetes namespace to resolve every triggering finding. "
        f"{strategy}"
        f"{program_guidance}"
        f"{drift_guidance}"
        f"{surfaced_context}"
        f"{_no_action_instructions()}"
        f"{_verification_instructions()}"
        "During this response, use plain commands and chain independent ones in one shell call. "
        "Do not write helper scripts for repairs, receipts or verification; keep interpretation and decisions in "
        "natural language. Never put scratch files in operational memory. "
        f"{memory_guard}Return only the IncidentResult JSON "
        f"required by the output schema. {branch_instruction('incident_result')}\n\n"
        f"Repair evidence mode: {request.repair_policy}. Every live mutation uses the session's sdo act recorder "
        "and declares every changed resource. The recorder supplies receipts; do not hand-write them. "
        "A confirmed cause requires a successful repair action that began before health cleared and mutated "
        "that cause's resources. Commit repository source changes when present.\n\n"
        f"{late_findings_guidance(request)}"
        f"{additional_context}\n"
        f"{_inlined_architecture_summary(request)}"
        f"{_inlined_health_objective(request)}"
        f"{_detector_evidence(request)}\n"
        f"{_state_changes_section(request)}"
        f"Incident request:\n{request_json}\n"
    )


if __name__ == "__main__":
    raise SystemExit(main())
