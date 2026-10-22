"""The scripted incident responder: diagnose, repair per the directive, verify, and answer.

Stdlib-only: this runs inside the responder image.
"""

from __future__ import annotations

import os
import re
import time
from typing import TYPE_CHECKING, Any

from .faults import PLANS, Facts, FaultPlan, PlanError
from .turn import Turn, utc_now

if TYPE_CHECKING:
    from pathlib import Path

    from .directive import Directive

#: Unit tests outside a cluster point this at a stub; in a pod the check is kubectl's own availability wait.
_STATUS_OVERRIDE = os.environ.get("SDO_SCRIPTED_STATUS_COMMAND")
STATUS_POLL_SECONDS = float(os.environ.get("SDO_SCRIPTED_STATUS_POLL_SECONDS", "5"))
WARM_MARKER = "Warm path: validated incident memory matches this incident."
_WARM_PLAYBOOK = re.compile(r"--- Playbook `([^`]+)` \(inlined")


def warm_playbooks(prompt: str) -> list[str]:
    """Logical playbook IDs the prompt inlined for the warm path."""

    return _WARM_PLAYBOOK.findall(prompt) if WARM_MARKER in prompt else []


def status_command(namespace: str) -> str:
    """The shell command that exits 0 once every Deployment in the incident namespace is available."""

    return _STATUS_OVERRIDE or f"kubectl -n {namespace} wait --for=condition=Available deployment --all --timeout=60s"


def _poll_status(turn: Turn, namespace: str, attempts: int) -> tuple[bool, str]:
    output = ""
    status = status_command(namespace)
    for attempt in range(attempts):
        command = status if attempt == 0 else f"sleep {STATUS_POLL_SECONDS}; {status}"
        code, output = turn.run(command, timeout=120)
        if code == 0:
            return True, output
    return False, output


def _action(
    action_id: str,
    target: str,
    summary: str,
    started: str,
    success: bool,
    details: str,
    resources: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "action_id": action_id,
        "kind": "kubectl",
        "target": target,
        # SDO credits a root cause only to a repair that mutated its resources.
        "resources": [resource for resource in resources if resource.get("name")],
        "summary": summary,
        "details": details.strip()[:2000] or summary,
        "started_at": started,
        "completed_at": utc_now(),
        "success": success,
        "reversible": False,
    }


def _verification(healthy: bool, output: str, *, claimed: bool = False) -> dict[str, Any]:
    details = output.strip()[:2000] or ("healthy" if healthy else "unhealthy")
    if claimed:
        details = "responder believed the restart fixed the incident; last status output: " + details
    return {"name": "deployments-available", "passed": healthy or claimed, "details": details, "observed_at": utc_now()}


def respond(turn: Turn, directive: Directive, request: dict[str, Any], prompt: str, worktree: Path) -> dict[str, Any]:
    started = utc_now()
    plan = PLANS[directive.fault]
    namespace = str(request["namespace"])
    result: dict[str, Any] = {
        "incident_id": str(request["incident_id"]),
        "status": "failed",
        "confirmed_root_causes": [],
        "applied_playbooks": [],
        "repair_changes": [],
        "repair_actions": [],
        "verification_evidence": [],
    }
    warm = [playbook_id for playbook_id in warm_playbooks(prompt) if playbook_id == plan.playbook_id]
    try:
        if directive.mitigation == "healed_noop":
            _healed_noop(turn, directive, namespace, result)
        elif warm and directive.mitigation == "correct":
            _warm(turn, directive, plan, request, namespace, worktree, result, warm[0])
        else:
            _cold(turn, directive, plan, request, namespace, worktree, result)
    except PlanError as exc:
        result["status"] = "failed"
        result["repair_changes"] = [f"scripted plan stopped: {exc}"]
    if directive.pause_before_result_seconds:
        time.sleep(directive.pause_before_result_seconds)
    result["timing"] = {"started_at": started, "completed_at": utc_now()}
    return result


def _healed_noop(turn: Turn, directive: Directive, namespace: str, result: dict[str, Any]) -> None:
    """Wait for the finding to heal without touching anything, then claim ``completed`` with no action.

    This is the shape a real responder gives a stray that self-healed: nothing
    was repaired and nothing is blamed. SDO must close it as cancelled.
    """

    healthy, status_output = _poll_status(turn, namespace, directive.status_attempts)
    result["verification_evidence"].append(_verification(healthy, status_output))
    if healthy:
        result["status"] = "completed"
        result["repair_changes"] = ["health recovered before any repair; nothing was changed"]


def _warm(
    turn: Turn,
    directive: Directive,
    plan: FaultPlan,
    request: dict[str, Any],
    namespace: str,
    worktree: Path,
    result: dict[str, Any],
    playbook: str,
) -> None:
    facts = plan.warm_facts(namespace, directive.target, request, worktree)
    sanity, repair, verify = plan.warm_commands(facts)
    code, output = turn.run(sanity)
    if code == 0:
        # The playbook's precondition does not hold: fall back to a full investigation.
        _cold(turn, directive, plan, request, namespace, worktree, result)
        return
    if directive.pause_before_repair_seconds:
        time.sleep(directive.pause_before_repair_seconds)
    repaired = plan.root_cause(_warm_diagnosis_facts(facts, request), request)["resources"]
    action_started = utc_now()
    code, output = turn.run(repair)
    result["repair_actions"].append(
        _action(
            "playbook-repair",
            facts.target,
            f"applied playbook {playbook}",
            action_started,
            code == 0,
            output,
            repaired,
        )
    )
    healthy, status_output = False, output
    for attempt in range(directive.status_attempts):
        command = verify if attempt == 0 else f"sleep {STATUS_POLL_SECONDS}; {verify}"
        code, status_output = turn.run(command, timeout=240)
        if code == 0:
            healthy = True
            break
    result["verification_evidence"].append(_verification(healthy, status_output))
    result["applied_playbooks"] = [{"playbook_id": playbook}]
    if healthy:
        result["status"] = "completed"
        diagnosis = plan.root_cause(_warm_diagnosis_facts(facts, request), request)
        result["confirmed_root_causes"] = [diagnosis]


def _warm_diagnosis_facts(facts: Facts, request: dict[str, Any]) -> Facts:
    del request
    objects = dict(facts.objects)
    objects.setdefault("manifest", "kubernetes/")
    return Facts(namespace=facts.namespace, target=facts.target, objects=objects, observations=facts.observations)


def _cold(
    turn: Turn,
    directive: Directive,
    plan: FaultPlan,
    request: dict[str, Any],
    namespace: str,
    worktree: Path,
    result: dict[str, Any],
) -> None:
    facts = plan.diagnose(turn.run, namespace, directive.target, request, worktree)
    turn.run(status_command(namespace), timeout=120)
    if directive.pause_before_repair_seconds:
        time.sleep(directive.pause_before_repair_seconds)
    mode = directive.mitigation
    healthy, status_output = False, ""
    if mode != "correct":
        command, summary = plan.wrong_repair(facts)
        action_started = utc_now()
        code, output = turn.run(command)
        healthy, status_output = _poll_status(turn, namespace, 3)
        claimed = mode == "wrong_only_claimed"
        result["repair_actions"].append(
            _action(
                "wrong-repair",
                facts.target,
                summary,
                action_started,
                code == 0 and (healthy or claimed),
                output,
                plan.claimed_root_cause(facts, request)["resources"],
            )
        )
        if claimed:
            result["status"] = "completed"
            result["confirmed_root_causes"] = [plan.claimed_root_cause(facts, request)]
            result["verification_evidence"].append(_verification(healthy, status_output, claimed=True))
            return
        if mode == "wrong_only_honest":
            result["verification_evidence"].append(_verification(healthy, status_output))
            result["repair_changes"] = ["the attempted restart did not restore health; the cause is still unknown"]
            return
    repaired = plan.root_cause(facts, request)["resources"]
    for index, (command, summary) in enumerate(plan.correct_repair(facts)):
        action_started = utc_now()
        code, output = turn.run(command)
        result["repair_actions"].append(
            _action(f"repair-{index + 1}", facts.target, summary, action_started, code == 0, output, repaired)
        )
    healthy, status_output = _poll_status(turn, namespace, directive.status_attempts)
    result["verification_evidence"].append(_verification(healthy, status_output))
    if healthy:
        result["status"] = "completed"
        result["confirmed_root_causes"] = [plan.root_cause(facts, request)]
