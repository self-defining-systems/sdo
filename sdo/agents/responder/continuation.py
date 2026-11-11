"""Typed controller feedback rendered as the resumed responder's user turn."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from sdo.agents.context_output import budget_for
from sdo.agents.prompt_templates import RenderedPrompt, named_prompt, render_prompt
from sdo.agents.responder import tool_output


class FindingResource(BaseModel):
    model_config = ConfigDict(extra="forbid")
    api_version: str = ""
    kind: str = ""
    namespace: str = ""
    name: str = ""


class BrokenFinding(BaseModel):
    model_config = ConfigDict(extra="forbid")
    primary_resource: FindingResource
    fingerprint: str
    detector_id: str
    rule_id: str
    summary: str
    evidence: str


class RepairReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")
    action_id: str
    kind: str = ""
    target: str = ""
    resources: list[FindingResource] | None = None
    summary: str = ""
    details: str = ""
    started_at: str = ""
    completed_at: str = ""
    success: bool
    reversible: bool = False
    failure_reason: str = ""


class CompletedRepair(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["program-application", "recorded-action"]
    action_id: str
    application_id: str = ""
    program_ids: list[str] = Field(default_factory=list)
    summary: str
    rolled_back: bool = False
    error: str = ""
    receipt: RepairReceipt


class VerificationBlocker(BaseModel):
    model_config = ConfigDict(extra="forbid")
    detector_id: str
    reason: str


class ContinuationFeedback(BaseModel):
    model_config = ConfigDict(extra="forbid")
    attempt: int = Field(ge=1)
    healthy: bool
    summary: str
    error: str = ""
    still_broken: list[BrokenFinding]
    source_differences: list[BrokenFinding] = Field(default_factory=list)
    already_done: list[CompletedRepair]
    verification_blockers: list[VerificationBlocker] = Field(default_factory=list)


MAX_CONTINUATION_CHARS = budget_for("continuation.instructions") + budget_for("continuation.incident_data")


#: Row text widths tried widest first before any row is dropped.
_WIDTHS = (64, 40, 24, 12)


def _brief(value: str, limit: int) -> str:
    text = " ".join(value.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _evidence_path(payload: str) -> Path:
    """Keep full transport evidence private and stable across renderer retries."""
    encoded = json.dumps(json.loads(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    directory = tool_output.OUTPUT_ROOT / "continuations"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = directory / (hashlib.sha256(encoded.encode()).hexdigest() + ".json")
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=directory, delete=False) as output:
        temporary = Path(output.name)
        try:
            output.write(encoded)
            output.close()
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    return path


def _resource_key(resource: FindingResource) -> tuple[str, str, str, str]:
    return resource.api_version, resource.kind, resource.namespace, resource.name


def _matches(repair: CompletedRepair, finding: BrokenFinding) -> bool:
    # Resource declarations are typed controller facts; prose and target strings
    # cannot prove that an action addressed a particular finding.
    return any(
        _resource_key(resource) == _resource_key(finding.primary_resource)
        for resource in repair.receipt.resources or []
    )


def _completed_at(repair: CompletedRepair) -> datetime:
    try:
        stamp = datetime.fromisoformat(repair.receipt.completed_at.replace("Z", "+00:00"))
        return stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


_DONE = frozenset({"success", "applied"})


def _status(repair: CompletedRepair) -> str:
    if repair.rolled_back:
        return "rolled-back"
    # FAILED needs a failure_reason; an applied action left to incident-level verification is progress.
    if repair.error or repair.receipt.failure_reason:
        return "failed"
    return "success" if repair.receipt.success else "applied"


def _action(repair: CompletedRepair, width: int) -> str:
    detail = repair.receipt.failure_reason or repair.error or repair.summary or repair.receipt.summary
    detail_width = max(width, 32) if repair.receipt.failure_reason else width
    return f"{repair.action_id} {_status(repair)} {_brief(detail, detail_width)}"


def continuation_prompt(payload: str) -> RenderedPrompt:
    """Validate, retain full evidence, and render only bounded factual progress."""
    feedback = ContinuationFeedback.model_validate_json(payload)
    path = _evidence_path(payload)
    repairs = feedback.already_done
    matched = {
        repair.action_id for repair in repairs if any(_matches(repair, finding) for finding in feedback.still_broken)
    }
    done = [repair for repair in repairs if _status(repair) in _DONE and repair.action_id not in matched]
    unmatched = [repair for repair in repairs if _status(repair) not in _DONE and repair.action_id not in matched]
    rows = []
    for finding in feedback.still_broken:
        attempts = [repair for repair in repairs if _matches(repair, finding)]
        last = max(enumerate(attempts), key=lambda item: (_completed_at(item[1]), item[0]))[1] if attempts else None
        rows.append((finding, last))

    def body(width: int, broken_count: int, done_count: int, unmatched_count: int, blocker_count: int) -> str:
        broken_lines = [
            f"- {finding.fingerprint} {_brief(finding.summary, width)}; "
            + ("last=" + _action(last, width) if last else "last=none")
            for finding, last in rows[:broken_count]
        ]
        other_lines = ["- attempt " + _action(repair, width) for repair in unmatched[:unmatched_count]]
        done_lines = ["- " + _action(repair, width) for repair in done[:done_count]]
        blocker_lines = [
            f"- {_brief(blocker.detector_id, 64)}: {_brief(blocker.reason, max(width, 32))}"
            for blocker in feedback.verification_blockers[:blocker_count]
        ]
        omitted = (
            len(rows)
            + len(done)
            + len(unmatched)
            + len(feedback.verification_blockers)
            - broken_count
            - done_count
            - unmatched_count
            - blocker_count
        )
        return "\n".join(
            [
                f"attempt={feedback.attempt} healthy={str(feedback.healthy).lower()} {_brief(feedback.summary, 64)}",
                *(["verification_error: " + _brief(feedback.error, 100)] if feedback.error else []),
                f"still_broken: ({len(rows)})",
                *broken_lines,
                *other_lines,
                *(
                    [f"verification_blockers: ({len(feedback.verification_blockers)})", *blocker_lines]
                    if feedback.verification_blockers
                    else []
                ),
                *(
                    [f"source_differences (information, not blocking): ({len(feedback.source_differences)})"]
                    if feedback.source_differences
                    else []
                ),
                f"already_done: ({len(done)})",
                *done_lines,
                *([f"Omitted {omitted} rows; read the full evidence file for them."] if omitted else []),
                "Full JSON: " + str(path),
            ]
        )

    limit = budget_for("continuation.incident_data")
    counts = [len(rows), len(done), len(unmatched), len(feedback.verification_blockers)]
    # Degrade deterministically: shorten row text, then drop rows least relevant first (completed work,
    # other attempts, blockers, and last the still-broken findings). Counts and the evidence path stay.
    text = ""
    for width in _WIDTHS:
        text = body(width, *counts)
        if len(text) <= limit:
            break
    else:
        for slot in (1, 2, 3, 0):
            while counts[slot] and len(text) > limit:
                counts[slot] -= 1
                text = body(_WIDTHS[-1], *counts)
    return render_prompt(
        "responder",
        "continuation",
        part="continuation.instructions",
        feedback=named_prompt("continuation.incident_data", text),
    )
