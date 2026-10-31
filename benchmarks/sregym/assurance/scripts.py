"""Compile assurance cases to editable agent scripts, before a worker starts.

Only the compiler knows the case menus. Workers execute the resulting public
agent-script steps and literal results, with task-bound references and clocks.
"""

from __future__ import annotations

import os
import shlex
from typing import Any

from benchmarks.sregym.agent_script.model import AgentScript
from benchmarks.sregym.assurance.faults import PLANS, Facts


def assurance_script(
    *,
    fault: str,
    target: str,
    mitigation: str = "correct",
    reflection: str = "learn",
    pause_before_repair_seconds: float = 0,
    pause_before_result_seconds: float = 0,
    status_attempts: int = 24,
    warm: bool = False,
) -> AgentScript:
    if mitigation not in {"correct", "wrong_then_correct", "wrong_only_honest", "wrong_only_claimed", "healed_noop"}:
        raise ValueError(f"unknown mitigation {mitigation}")
    if reflection not in {"learn", "no_change", "crash_once", "crash_always", "invalid_then_learn"}:
        raise ValueError(f"unknown reflection {reflection}")
    if fault not in PLANS or not target or not target.replace("-", "").isalnum():
        raise ValueError("unknown fault or unsafe target")
    if status_attempts < 1 or status_attempts > 60:
        raise ValueError("status_attempts must be between 1 and 60")
    plan = PLANS[fault]
    ns = "{{request.namespace}}"
    configmap = f"mongo-{target.removeprefix('mongodb-')}-script"
    facts = Facts(
        namespace=ns,
        target=target,
        objects={
            "policy": f"deny-all-{target}",
            "configmap": configmap,
            "manifest": f"k8s-{target.removeprefix('mongodb-')}-mongo.sh",
        },
    )
    cause = plan.root_cause(facts, {})
    cause["explained_detectors"] = ["{{request.findings.0.detector_id}}"]
    cause["evidence"] = [
        {
            "kind": "detector-finding",
            "source": "{{request.findings.0.detector_id}}",
            "observation": "{{request.findings.0.summary}}",
        }
    ]
    claimed = plan.claimed_root_cause(facts, {})
    claimed["explained_detectors"] = ["{{request.findings.0.detector_id}}"]
    steps: list[dict[str, Any]] = []

    def command(text: str, **options: Any) -> int:
        index = len(steps)
        steps.append({"kind": "command", "command": text, "timeout": 300.0, **options})
        return index

    def pause(seconds: float) -> None:
        if seconds < 0:
            raise ValueError("pauses must not be negative")
        while seconds:
            chunk = min(seconds, 60)
            steps.append({"kind": "wait", "seconds": float(chunk)})
            seconds -= chunk

    status = os.environ.get("SDO_SCRIPTED_STATUS_COMMAND", "python3 -m sdo.agents.responder.status_cli")
    interval = float(os.environ.get("SDO_SCRIPTED_STATUS_POLL_SECONDS", "5"))
    actions = []
    result: dict[str, Any] = {
        "incident_id": "{{incident_id}}",
        "status": "completed",
        "confirmed_root_causes": [],
        "applied_playbooks": [],
        "repair_changes": [],
        "repair_actions": actions,
        "verification_evidence": [],
        "timing": {"started_at": "{{turn.started_at}}", "completed_at": "{{now}}"},
    }
    # Compile the source-grounded repair shell from the same templates reflection
    # publishes. Object identity is checked against live state before mutation.
    proposal = plan.memory(facts, incident_id="{{incident_id}}", outcome_commit="{{request.outcome_commit}}")
    repair_path = f".sdo/playbooks/{plan.playbook_id}/scripts/repair.sh"
    if mitigation != "healed_noop":
        if fault == "network_policy_block":
            command(
                f"kubectl -n {ns} get networkpolicy -o json | python3 -c "
                + shlex.quote(
                    'import json,sys; assert any(p["metadata"]["name"] == '
                    + repr(f"deny-all-{target}")
                    + ' and p.get("spec", {}).get("podSelector", {}).get("matchLabels", {})'
                    + '.get("io.kompose.service") == '
                    + repr(target)
                    + ' and {"Ingress", "Egress"} <= set(p.get("spec", {}).get("policyTypes", []))'
                    + ' and not p["spec"].get("ingress") and not p["spec"].get("egress")'
                    + ' for p in json.load(sys.stdin)["items"])'
                )
            )
        else:
            command(
                f"kubectl -n {ns} get deployment {target} -o json | python3 -c "
                + shlex.quote(
                    'import json,sys; assert any(v.get("configMap", {}).get("name") == '
                    + repr(configmap)
                    + ' for v in json.load(sys.stdin)["spec"]["template"]["spec"].get("volumes", []))'
                )
            )
            command(f"! kubectl -n {ns} get configmap {configmap} -o name")
        pause(pause_before_repair_seconds)

        def action(text: str, identity: str, resources: list[dict[str, Any]], success: bool = True) -> None:
            index = command(text)
            actions.append(
                {
                    "action_id": identity,
                    "kind": "kubectl",
                    "target": target,
                    "resources": resources,
                    "summary": identity,
                    "details": text,
                    "started_at": f"{{{{steps.{index}.started_at}}}}",
                    "completed_at": f"{{{{steps.{index}.completed_at}}}}",
                    "success": success,
                    "reversible": False,
                }
            )

        if mitigation != "correct":
            action(
                plan.wrong_repair(facts)[0], "wrong-repair", claimed["resources"], mitigation == "wrong_only_claimed"
            )
            command(f"! ( {status} )", attempts=3, interval=interval)
        if mitigation in {"correct", "wrong_then_correct"}:
            if warm:
                repair = (
                    f"sh {repair_path} {ns} "
                    + (f"deny-all-{target}" if fault == "network_policy_block" else target)
                    + (" -" if fault == "missing_configmap" else "")
                )
                result["applied_playbooks"] = [{"playbook_id": plan.playbook_id}]
            elif fault == "network_policy_block":
                repair = f"kubectl -n {ns} delete networkpolicy deny-all-{target}"
            else:
                # Run an embedded copy, leaving no helper files in responder-owned source.
                shell = proposal.files[repair_path]
                repair = f"sh -c {shlex.quote(shell)} -- {ns} {target} {configmap}"
            action(repair, "playbook-repair" if warm else "repair-1", cause["resources"])
            result["confirmed_root_causes"] = [cause]
        elif mitigation == "wrong_only_claimed":
            result["confirmed_root_causes"] = [claimed]
        else:
            result["status"] = "failed"
            result["repair_changes"] = ["the attempted restart did not restore health; the cause is still unknown"]
    if mitigation in {"correct", "wrong_then_correct", "healed_noop"}:
        command(status, attempts=status_attempts, interval=interval)
    result["verification_evidence"] = [
        {
            "name": "sdo-incident-status",
            "passed": mitigation != "wrong_only_honest",
            "details": "status checked against live state",
            "observed_at": "{{now}}",
        }
    ]
    pause(pause_before_result_seconds)
    turns: list[dict[str, Any]] = [{"role": "responder", "index": 0, "steps": steps, "result": result}]
    if mitigation == "wrong_only_claimed":
        proposal = plan.claimed_memory(
            facts, incident_id="{{incident_id}}", outcome_commit="{{request.outcome_commit}}"
        )
    writes = [
        {"kind": "write", "path": path, "text": text, "executable": path in proposal.executable_paths}
        for path, text in proposal.files.items()
    ]
    writes.append(
        {
            "kind": "command",
            "command": "python3 -c "
            + shlex.quote(
                "from pathlib import Path; p=Path('.sdo/playbooks'); "
                "(p/'README.md').write_text('# Operational playbooks\\n\\n' + ''.join("
                "f'- [{d.name}]({d.name}/README.md)\\n' for d in sorted(p.iterdir()) if d.is_dir()))"
            ),
        }
    )
    learned = {
        "summary": f"Encoded {proposal.detector_id}",
        "learning_decision": "updated",
        "no_change_reason": None,
        "playbook_ids": [proposal.playbook_id],
    }
    no_change = {
        "summary": "No new learning",
        "learning_decision": "no_change",
        "no_change_reason": "This incident teaches nothing new",
        "playbook_ids": [],
    }
    for index in range(4):
        reflection_steps: list[dict[str, Any]] = list(writes)
        answer = learned
        if reflection == "crash_always" or (reflection == "crash_once" and index == 0):
            reflection_steps = [{"kind": "command", "command": "git status --short"}, {"kind": "exit", "code": 1}]
        elif reflection == "no_change":
            reflection_steps = [{"kind": "command", "command": "git status --short"}]
            answer = no_change
        else:
            if reflection == "invalid_then_learn" and index == 0:
                reflection_steps.append(
                    {
                        "kind": "write",
                        "path": f".sdo/playbooks/{proposal.playbook_id}/README.md",
                        "text": "malformed README\n",
                    }
                )
                reflection_steps.append({"kind": "tool", "argv": ["sdo-operator", "memory", "check"], "exit_code": 1})
                # Publish the invalid proposal to exercise broker rejection and a
                # separate correction turn, rather than quietly fixing it locally.
            else:
                reflection_steps.append({"kind": "tool", "argv": ["sdo-operator", "memory", "check"]})
        turns.append({"role": "reflector", "index": index, "steps": reflection_steps, "result": answer})
    return AgentScript.model_validate({"version": 1, "turns": turns})
