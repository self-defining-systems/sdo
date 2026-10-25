"""Compact incident brief for a fresh-session first reflection attempt.

A resumed reflection re-sends the responder's whole transcript on every model
request. The fresh mode instead hands a new session this brief: the closure's
live evidence (verbatim where it matters), the responder's structured result,
its shell commands when the per-turn usage log recorded them, and short
excerpts of the operational memory the incident touched. Every section has a
character budget so the brief stays under about 8K tokens.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format

from sdo.agents.prompt_templates import RenderedPrompt, join_prompts, named_prompt, prompt_prefix, render_prompt
from sdo.agents.responder import recorded_actions
from sdo.contracts import Verdict, timestamp_datetime, to_canonical_json

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from google.protobuf.timestamp_pb2 import Timestamp

    from sdo.contracts import DetectorEvaluation, Finding, IncidentClosure, IncidentResult, ObjectRef

#: Upper bound on the whole brief: about 4 characters per token, under 8K tokens.
BRIEF_MAX_CHARS = 30_000
_FINDINGS_BUDGET = 12_000
_RESULT_BUDGET = 6_000
_COMMANDS_BUDGET = 6_000
_MEMORY_BUDGET = 9_000
_MAX_FULL_FINDINGS = 4
_EVIDENCE_MAX_CHARS = 3_000
_METADATA_MAX_CHARS = 1_200
_DETAILS_MAX_CHARS = 700
_COMMAND_MAX_CHARS = 300
_HEAD_COMMANDS = 10
_PLAYBOOK_EXCERPT_CHARS = 700
_MAX_PLAYBOOK_EXCERPTS = 4
_INDEX_EXCERPT_CHARS = 1_000
_DETECTOR_DESCRIPTION_CHARS = 500
_DETECTOR_PREDICATE_CHARS = 1_400


def _clip(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    omitted = len(text) - limit
    return render_prompt(
        "responder", "clip_01", part="reflection_brief.instructions", text=prompt_prefix(text, limit), omitted=omitted
    )


def _iso(value: Timestamp) -> str:
    return timestamp_datetime(value).isoformat()


def _plain(value: Any) -> Any:
    """JSON-native form of a ``Struct`` value: protobuf numbers are doubles, so whole ones read as integers."""

    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


def _ref(ref: ObjectRef) -> str:
    location = f"{ref.namespace}/{ref.name}" if ref.namespace else ref.name
    version = (
        render_prompt("responder", "ref_01", part="reflection_brief.instructions", api_version=ref.api_version)
        if ref.api_version
        else ""
    )
    return render_prompt(
        "responder", "ref_02", part="reflection_brief.instructions", kind=ref.kind, location=location, version=version
    )


def _fenced(text: str) -> str:
    fence = "~~~~" if "```" in text else "```"
    return render_prompt(
        "responder", "fenced_01", part="reflection_brief.instructions", fence=fence, text=text, fence_2=fence
    )


def _finding(finding: Finding) -> str:
    lines: list[str] = [
        render_prompt(
            "responder",
            "finding_01",
            part="reflection_brief.instructions",
            severity=finding.severity,
            detector_id=finding.detector_id,
            rule_id=finding.rule_id,
            status=finding.status,
            summary=finding.summary,
        ),
        render_prompt(
            "responder", "finding_02", part="reflection_brief.instructions", ref=_ref(finding.primary_resource)
        ),
    ]
    if finding.related_resources:
        lines.append("  related: " + "; ".join(_ref(ref) for ref in finding.related_resources[:10]))
    if finding.parameter_bindings:
        bindings = "; ".join(f"{role}={_ref(ref)}" for role, ref in sorted(finding.parameter_bindings.items()))
        lines.append(render_prompt("responder", "finding_03", part="reflection_brief.instructions", bindings=bindings))
    if finding.fingerprint:
        lines.append(
            render_prompt(
                "responder", "finding_04", part="reflection_brief.instructions", fingerprint=finding.fingerprint
            )
        )
    if finding.playbook_ids:
        lines.append("  playbooks: " + ", ".join(finding.playbook_ids))
    lines.append(render_prompt("responder", "finding_05", part="reflection_brief.instructions"))
    lines.append(_fenced(_clip(finding.evidence, _EVIDENCE_MAX_CHARS)))
    if finding.metadata.fields:
        metadata = json.dumps(_plain(json_format.MessageToDict(finding.metadata)), sort_keys=True, default=str)
        lines.append(render_prompt("responder", "finding_06", part="reflection_brief.instructions"))
        lines.append(_fenced(_clip(metadata, _METADATA_MAX_CHARS)))
    return "\n".join(lines)


def _history(evaluations: Sequence[DetectorEvaluation]) -> list[str]:
    by_detector: dict[str, list[DetectorEvaluation]] = {}
    for evaluation in evaluations:
        by_detector.setdefault(evaluation.detector_id, []).append(evaluation)
    lines: list[str] = []
    for detector_id, entries in sorted(by_detector.items()):
        statuses = ", ".join(entry.status for entry in entries[-6:])
        lines.append(
            render_prompt(
                "responder",
                "history_01",
                part="reflection_brief.instructions",
                detector_id=detector_id,
                len=len(entries),
                iso=_iso(entries[0].evaluated_at),
                iso_2=_iso(entries[-1].evaluated_at),
                statuses=statuses,
            )
        )
    return lines


def _states(label: str, evaluations: Sequence[DetectorEvaluation]) -> list[str]:
    if not evaluations:
        return [render_prompt("responder", "states_01", part="reflection_brief.instructions", label=label)]
    rendered = []
    for state in evaluations:
        detail = (
            render_prompt(
                "responder", "states_02", part="reflection_brief.instructions", fingerprints=state.fingerprints
            )
            if state.fingerprints
            else ""
        )
        error = (
            render_prompt("responder", "states_03", part="reflection_brief.instructions", error=state.error)
            if state.error
            else ""
        )
        rendered.append(f"{state.detector_id}={state.status}{detail}{error}")
    return [
        render_prompt("responder", "states_04", part="reflection_brief.instructions", label=label) + "; ".join(rendered)
    ]


def _findings_section(closure: IncidentClosure) -> str:
    request = closure.request
    lines: list[str] = [
        render_prompt("responder", "findings_section_01", part="reflection_brief.instructions"),
        render_prompt(
            "responder",
            "findings_section_02",
            part="reflection_brief.instructions",
            application=request.application,
            namespace=request.namespace,
            source_commit=request.source_commit,
            deployed_commit=request.deployed_commit,
        ),
        render_prompt(
            "responder",
            "findings_section_03",
            part="reflection_brief.instructions",
            iso=_iso(closure.detected_at),
            iso_2=_iso(closure.dispatched_at),
            iso_3=_iso(closure.responder_completed_at),
            iso_4=_iso(closure.verified_at),
        ),
        render_prompt("responder", "brief_heading_01", part="reflection_brief.instructions"),
    ]
    if request.HasField("namespace_onboarding"):
        lines.append(
            render_prompt(
                "responder",
                "namespace_onboarding",
                part="reflection_brief.findings",
                onboarding_json=to_canonical_json(request.namespace_onboarding),
            )
        )
    if request.HasField("state_changes"):
        lines.append(render_prompt("responder", "findings_section_04", part="reflection_brief.instructions"))
        lines.append(_fenced(_clip(to_canonical_json(request.state_changes), 2_000)))
    if closure.HasField("final_state_changes"):
        lines.append(render_prompt("responder", "findings_section_05", part="reflection_brief.instructions"))
        lines.append(_fenced(_clip(to_canonical_json(closure.final_state_changes), 2_000)))
    lines.append(render_prompt("responder", "brief_heading_02", part="reflection_brief.instructions"))
    lines.extend(_finding(finding) for finding in request.findings[:_MAX_FULL_FINDINGS])
    lines.extend(
        render_prompt(
            "responder",
            "findings_section_06",
            part="reflection_brief.instructions",
            severity=finding.severity,
            detector_id=finding.detector_id,
            rule_id=finding.rule_id,
            summary=finding.summary,
            ref=_ref(finding.primary_resource),
        )
        for finding in request.findings[_MAX_FULL_FINDINGS:]
    )
    lines.append(render_prompt("responder", "findings_section_07", part="reflection_brief.instructions"))
    lines.extend(_history(request.detector_history))
    if request.surfaced_playbooks:
        lines.append(
            render_prompt("responder", "findings_section_08", part="reflection_brief.instructions")
            + ", ".join(playbook.playbook_id for playbook in request.surfaced_playbooks)
        )
    lines.extend(
        render_prompt(
            "responder",
            "findings_section_09",
            part="reflection_brief.instructions",
            incident_id=prior.incident_id,
            match_reason=prior.match_reason,
            list=list(prior.root_cause_summaries),
            list_2=list(prior.repair_action_summaries),
            list_3=list(prior.applied_playbook_ids),
        )
        for prior in request.relevant_outcomes
    )
    lines.extend(
        _states(
            render_prompt("responder", "findings_section_10", part="reflection_brief.instructions"),
            closure.final_detector_states,
        )
    )
    lines.extend(
        _states(
            render_prompt("responder", "findings_section_11", part="reflection_brief.instructions"),
            closure.incident_detector_states,
        )
    )
    return _clip("\n".join(lines), _FINDINGS_BUDGET)


def _result_section(result: IncidentResult | None, dispatch_error: str) -> str:
    lines: list[str] = [render_prompt("responder", "result_section_01", part="reflection_brief.instructions")]
    if result is None:
        lines.append(
            render_prompt(
                "responder",
                "result_section_02",
                part="reflection_brief.instructions",
                dispatch_error=dispatch_error or "none",
            )
        )
        return "\n".join(lines)
    lines.append(
        render_prompt("responder", "result_section_03", part="reflection_brief.instructions", status=result.status)
        + (
            render_prompt("responder", "result_section_04", part="reflection_brief.instructions", error=result.error)
            if result.error
            else ""
        )
    )
    verdict_names = {
        "RESULT_MATCH": "Match",
        "RESULT_NO_MATCH": "NoMatch",
        "RESULT_UNKNOWN": "Unknown",
        "RESULT_NEEDS_JUDGEMENT": "NeedsJudgement",
    }
    if result.program_runs:
        by_program: dict[str, list[str]] = {}
        for run in result.program_runs:
            verdict = verdict_names.get(
                Verdict.Result.Name(run.verdict.result), Verdict.Result.Name(run.verdict.result)
            )
            history = by_program.setdefault(run.program_id, [])
            if not history or history[-1] != verdict:
                history.append(verdict)
        lines.append(render_prompt("responder", "result_section_05", part="reflection_brief.instructions"))
        lines.extend(
            render_prompt(
                "responder",
                "result_section_06",
                part="reflection_brief.instructions",
                program_id=program_id,
                join=" -> ".join(history),
            )
            for program_id, history in list(by_program.items())[:12]
        )
        if len(by_program) > 12:
            lines.append(
                render_prompt(
                    "responder", "result_section_07", part="reflection_brief.instructions", len=len(by_program) - 12
                )
            )
    if result.plan_applications:
        lines.append(render_prompt("responder", "result_section_08", part="reflection_brief.instructions"))
        for application in result.plan_applications[:12]:
            verification = (
                verdict_names.get(
                    Verdict.Result.Name(application.verification.result),
                    Verdict.Result.Name(application.verification.result),
                )
                if application.HasField("verification")
                else "not recorded"
            )
            state = "applied" if application.applied else "not applied"
            programs = ", ".join(application.program_ids) or render_prompt(
                "responder", "result_section_09", part="reflection_brief.instructions"
            )
            lines.append(
                render_prompt(
                    "responder",
                    "result_section_10",
                    part="reflection_brief.instructions",
                    application_id=application.application_id or "unnamed",
                    programs=programs,
                    state=state,
                    verification=verification,
                )
                + (
                    render_prompt(
                        "responder",
                        "result_section_11",
                        part="reflection_brief.instructions",
                        clip=_clip(application.error, 120),
                    )
                    if application.error
                    else ""
                )
            )
        if len(result.plan_applications) > 12:
            lines.append(
                render_prompt(
                    "responder",
                    "result_section_12",
                    part="reflection_brief.instructions",
                    len=len(result.plan_applications) - 12,
                )
            )
    lines.append(render_prompt("responder", "result_section_13", part="reflection_brief.instructions"))
    lines.extend(
        render_prompt(
            "responder",
            "result_section_14",
            part="reflection_brief.instructions",
            summary=cause.summary,
            join="; ".join(_ref(ref) for ref in cause.resources),
        )
        for cause in result.confirmed_root_causes
    )
    if not result.confirmed_root_causes:
        lines.append(render_prompt("responder", "brief_heading_04", part="reflection_brief.instructions"))
    lines.append(render_prompt("responder", "result_section_15", part="reflection_brief.instructions"))
    lines.extend(
        render_prompt(
            "responder",
            "result_section_16",
            part="reflection_brief.instructions",
            value="ok" if action.success else "FAILED",
            kind=action.kind,
            target=action.target,
            summary=action.summary,
            clip=_clip(action.details, _DETAILS_MAX_CHARS),
        )
        for action in result.repair_actions
    )
    if not result.repair_actions:
        lines.append(render_prompt("responder", "result_section_17", part="reflection_brief.instructions"))
    if result.repair_changes:
        lines.append(
            render_prompt("responder", "result_section_18", part="reflection_brief.instructions")
            + ", ".join(result.repair_changes)
        )
    lines.append(render_prompt("responder", "brief_heading_03", part="reflection_brief.instructions"))
    lines.extend(
        render_prompt(
            "responder",
            "result_section_19",
            part="reflection_brief.instructions",
            value="passed" if evidence.passed else "FAILED",
            name=evidence.name,
            clip=_clip(evidence.details, _DETAILS_MAX_CHARS),
        )
        for evidence in result.verification_evidence
    )
    if not result.verification_evidence:
        lines.append(render_prompt("responder", "result_section_20", part="reflection_brief.instructions"))
    for playbook in result.applied_playbooks:
        scripts = (
            render_prompt(
                "responder",
                "result_section_21",
                part="reflection_brief.instructions",
                join=", ".join(playbook.script_names),
            )
            if playbook.script_names
            else ""
        )
        lines.append(
            render_prompt(
                "responder",
                "result_section_22",
                part="reflection_brief.instructions",
                playbook_id=playbook.playbook_id,
                scripts=scripts,
            )
        )
    return _clip("\n".join(lines), _RESULT_BUDGET)


def responder_shell_commands(log: Path | None, session_id: str | None) -> list[str] | None:
    """Shell commands the responder session ran, from the per-turn usage log; ``None`` when unavailable."""

    if log is None or not session_id:
        return None
    commands: list[str] = []
    found = False
    try:
        with log.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict) or record.get("session_id") != session_id:
                    continue
                found = True
                lines = record.get("shell_command_lines")
                if isinstance(lines, list):
                    commands.extend(recorded_actions.scrub_shell_command(str(command)) for command in lines)
    except OSError:
        return None
    return commands if found else None


def _commands_section(commands: list[str] | None) -> str:
    header = render_prompt("responder", "commands_section_01", part="reflection_brief.instructions")
    if commands is None:
        return render_prompt("responder", "commands_section_02", part="reflection_brief.instructions", header=header)
    if not commands:
        return render_prompt("responder", "commands_section_03", part="reflection_brief.instructions", header=header)
    rendered = [
        render_prompt(
            "responder",
            "commands_section_04",
            part="reflection_brief.instructions",
            index=index,
            clip=_clip(command, _COMMAND_MAX_CHARS),
        )
        for index, command in enumerate(commands, 1)
    ]
    if sum(len(line) + 1 for line in rendered) <= _COMMANDS_BUDGET:
        return "\n".join(
            [
                render_prompt(
                    "responder",
                    "commands_section_05",
                    part="reflection_brief.instructions",
                    header=header,
                    len=len(commands),
                ),
                *rendered,
            ]
        )
    # Keep the first diagnostic commands and as many of the last (repair and
    # verification) commands as fit.
    head = rendered[:_HEAD_COMMANDS]
    used = sum(len(line) + 1 for line in head)
    tail: list[str] = []
    for line in reversed(rendered[_HEAD_COMMANDS:]):
        if used + len(line) + 1 > _COMMANDS_BUDGET - 80:
            break
        tail.insert(0, line)
        used += len(line) + 1
    omitted = len(rendered) - len(head) - len(tail)
    return "\n".join(
        [
            render_prompt(
                "responder",
                "commands_section_06",
                part="reflection_brief.instructions",
                header=header,
                len=len(commands),
            ),
            *head,
            render_prompt("responder", "commands_section_07", part="reflection_brief.instructions", omitted=omitted),
            *tail,
        ]
    )


def _relevant_playbooks(
    closure: IncidentClosure, fired_detectors: set[str], possible: dict[str, list[str]]
) -> list[str]:
    paths: list[str] = []
    candidates = [playbook.playbook_id for playbook in closure.request.surfaced_playbooks]
    for finding in closure.request.findings:
        candidates.extend(finding.playbook_ids)
    if closure.HasField("result"):
        candidates.extend(playbook.playbook_id for playbook in closure.result.applied_playbooks)
    for detector_id in sorted(fired_detectors):
        candidates.extend(possible.get(detector_id, []))
    for path in candidates:
        if path not in paths:
            paths.append(path)
    return paths


def _logical_memory_section(closure: IncidentClosure, *, detector_detail: bool, event_dependence: bool) -> str:
    assert closure.request.HasField("memory_snapshot")
    snapshot = closure.request.memory_snapshot
    fired = {finding.detector_id for finding in closure.request.findings}
    lines: list[str] = [render_prompt("responder", "logical_memory_section_01", part="reflection_brief.instructions")]
    health = [item.id for item in snapshot.detectors if item.detector_class == "health"]
    lines.append(
        render_prompt("responder", "logical_memory_section_02", part="reflection_brief.instructions")
        + (", ".join(health) or "none")
    )
    possible = {item.id: list(item.possible_playbook_ids) for item in snapshot.detectors}
    for detector in snapshot.detectors:
        if detector.detector_class != "incident":
            continue
        watches = ", ".join(f"{watch.api_version}/{watch.kind}" for watch in detector.watches) or "none"
        marker = (
            render_prompt("responder", "logical_memory_section_03", part="reflection_brief.instructions")
            if detector.id in fired
            else ""
        )
        lines.append(
            render_prompt(
                "responder",
                "logical_memory_section_04",
                part="reflection_brief.instructions",
                id=detector.id,
                watches=watches,
                firing=detector.firing,
                list=list(detector.possible_playbook_ids),
                originating_incident=detector.originating_incident,
                marker=marker,
            )
        )
        if detector_detail:
            if detector.description:
                lines.append(
                    render_prompt("responder", "logical_memory_section_05", part="reflection_brief.instructions")
                    + _clip(detector.description, _DETECTOR_DESCRIPTION_CHARS)
                )
            if detector.predicate_excerpt:
                lines.append(_fenced(_clip(detector.predicate_excerpt, _DETECTOR_PREDICATE_CHARS)))
            if event_dependence:
                lines.append(
                    render_prompt("responder", "logical_memory_section_06", part="reflection_brief.instructions")
                    + ("yes" if detector.reads_events else "no")
                )
    if snapshot.playbook_index_markdown:
        lines.append(render_prompt("responder", "logical_memory_section_07", part="reflection_brief.instructions"))
        lines.append(_fenced(_clip(snapshot.playbook_index_markdown, _INDEX_EXCERPT_CHARS)))
    available = {item.id: item for item in snapshot.playbooks}
    lines.append(
        render_prompt("responder", "logical_memory_section_08", part="reflection_brief.instructions")
        + (", ".join(sorted(available)) or "none")
    )
    relevant = [item for item in _relevant_playbooks(closure, fired, possible) if item in available]
    if not relevant:
        relevant = sorted(available)
    for playbook_id in relevant[:_MAX_PLAYBOOK_EXCERPTS]:
        lines.append(
            render_prompt(
                "responder", "logical_memory_section_09", part="reflection_brief.instructions", playbook_id=playbook_id
            )
        )
        lines.append(_fenced(_clip(available[playbook_id].markdown.strip(), _PLAYBOOK_EXCERPT_CHARS)))
    return _clip("\n".join(lines), _MEMORY_BUDGET)


def incident_brief(
    closure: IncidentClosure,
    *,
    worktree: Path,
    responder_turn_log: Path | None = None,
    detector_detail: bool = False,
    event_dependence: bool = False,
) -> RenderedPrompt:
    """Render the bounded incident brief for a fresh first reflection attempt.

    ``detector_detail`` adds, for every existing incident detector, its Spec
    description and a bounded excerpt of its match predicate, so the reader can
    compare the confirmed signature with what each detector already matches.
    ``event_dependence`` (with ``detector_detail``) also states whether the detector
    source reads Event objects, which may not exist when a fault is introduced.
    """

    result = closure.result if closure.HasField("result") else None
    session_id = None if result is None else result.responder_session_id
    sections = [
        named_prompt("reflection_brief.findings", _findings_section(closure)),
        named_prompt("reflection_brief.result", _result_section(result, closure.dispatch_error)),
        named_prompt(
            "reflection_brief.memory",
            _logical_memory_section(closure, detector_detail=detector_detail, event_dependence=event_dependence),
        ),
        named_prompt(
            "reflection_brief.commands", _commands_section(responder_shell_commands(responder_turn_log, session_id))
        ),
    ]
    body = _clip(join_prompts("\n\n", sections, part="reflection_brief.separators"), BRIEF_MAX_CHARS)
    return render_prompt(
        "responder", "incident_brief", part="reflection_brief.instructions", len=len(body) // 4, body=body
    )
