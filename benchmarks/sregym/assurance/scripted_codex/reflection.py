"""Scripted reflection returns logical artifacts for the Go memory edge to validate."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

from .faults import PLANS, Facts

if TYPE_CHECKING:
    from pathlib import Path

    from .directive import Binding
    from .store import Store
    from .turn import Turn

_IDEMPOTENCY = re.compile(r"Idempotency key: reflection:(?P<incident>.+):(?P<commit>[0-9a-f]{7,40})\s*$", re.MULTILINE)
RETRY_MARKER = "You are correcting an operational-memory proposal"


class ReflectionCrash(RuntimeError):
    """The directive asks this reflection turn to crash like a dying CLI."""


def idempotency(prompt: str) -> tuple[str, str]:
    match = _IDEMPOTENCY.search(prompt)
    if match is None:
        raise ValueError("reflection prompt has no idempotency key")
    return match.group("incident"), match.group("commit")


def _known_artifact_ids(prompt: str) -> set[str]:
    marker = "Operational memory snapshot:\n"
    if marker not in prompt:
        return set()
    document, _ = json.JSONDecoder().raw_decode(prompt.split(marker, 1)[1].lstrip())
    if not isinstance(document, dict):
        raise ValueError("operational memory snapshot must be an object")
    return {
        str(item["id"])
        for field in ("detectors", "playbooks")
        for item in document.get(field, [])
        if isinstance(item, dict) and item.get("id")
    }


def _no_change(reason: str) -> dict[str, Any]:
    return {
        "summary": reason,
        "learning_decision": "no_change",
        "no_change_reason": reason,
        "proposed_changes": [],
        "artifact_proposal": None,
    }


def reflect(turn: Turn, binding: Binding, prompt: str, worktree: Path, store: Store) -> dict[str, Any]:
    del worktree
    directive = binding.directive
    incident_id, outcome_commit = idempotency(prompt)
    retry = RETRY_MARKER in prompt
    mode = directive.reflection
    if mode == "crash_always" or (mode == "crash_once" and store.claim(f"{incident_id}:reflection-crash")):
        turn.run("git status --short")
        raise ReflectionCrash(f"scripted reflection crash for {incident_id} ({mode})")
    if mode == "no_change":
        turn.run("git status --short")
        return _no_change("scripted directive: this incident teaches nothing new")
    plan = PLANS[directive.fault]
    facts = Facts(namespace="", target=directive.target)
    claimed = directive.mitigation == "wrong_only_claimed"
    proposal = (
        plan.claimed_memory(facts, incident_id=incident_id, outcome_commit=outcome_commit)
        if claimed
        else plan.memory(facts, incident_id=incident_id, outcome_commit=outcome_commit)
    )
    if {proposal.detector_id, proposal.playbook_id} <= _known_artifact_ids(prompt):
        return _no_change(f"memory already encodes detector {proposal.detector_id} and playbook {proposal.playbook_id}")
    artifacts = proposal.artifact_proposal
    if mode == "invalid_then_learn" and not retry and store.claim(f"{incident_id}:reflection-invalid"):
        artifacts["detectors"][0]["firing"] = 5
    turn.run("git status --short")
    return {
        "summary": f"Encoded {proposal.detector_id} with playbook {proposal.playbook_id}"
        + (" (claimed cause)" if claimed else ""),
        "learning_decision": "updated",
        "no_change_reason": None,
        "proposed_changes": [],
        "artifact_proposal": artifacts,
    }
