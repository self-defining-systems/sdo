"""Generate ordinary agent-script commands for live responder decisions.

Generation is pure. Each phase reads live tools, persists only transient exchange
state, and is audited by the normal script runner. No legacy worker is imported.
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import shlex
from pathlib import Path

from google.protobuf import json_format

from benchmarks.sregym.agent_script.model import AgentScript
from benchmarks.sregym.fastloop import responder_steps
from sdo.contracts import AgentTask, IncidentRequest

PLAN = r"""
started = _now()
diagnosis = _tool("diagnose")
plan_ids, programs = _matched_plans(diagnosis)
if plan_ids:
    _tool("apply", *plan_ids)
"""

DRIFT = r"""
repairs: list[dict[str, Any]] = []
labels: list[DriftEvent] = []
unsure: set[tuple[str, str]] = set()
for event in request.drift_events:
    if not any(
        finding.primary_resource == event.ref and finding.detector_id.startswith("drift.")
        for finding in request.findings
    ):
        continue
    label = _assess_drift(request, event)
    labels.append(label)
    if label.investigation_verdict != "bug":
        unsure.add((event.ref.kind, event.ref.name))
        continue
    ref = json_format.MessageToDict(event.ref, preserving_proto_field_name=True)
    action_start = _now()
    if ref["kind"] == "NetworkPolicy":
        _command(["kubectl", "-n", request.namespace, "delete", "networkpolicy", ref["name"]])
    else:
        _patch_from_source(request, ref)
    repairs.append(
        {
            "action_id": f"source-{len(repairs) + 1}",
            "kind": "kubectl-delete" if ref["kind"] == "NetworkPolicy" else "kubectl-patch",
            "target": f"{event.ref.kind}/{event.ref.name}",
            "resources": [ref],
            "summary": f"Removed source-absent NetworkPolicy/{event.ref.name}"
            if ref["kind"] == "NetworkPolicy"
            else f"Restored Service/{event.ref.name} from accepted source",
            "details": label.investigation_reason,
            "started_at": action_start,
            "completed_at": _now(),
            "success": True,
            "reversible": ref["kind"] != "NetworkPolicy",
        }
    )
"""

REPAIR = r"""
first = _tool("verify")
if not first.get("healthy"):
    seen: set[tuple[str, str]] = set(unsure)
    seen.update((ref["kind"], ref["name"]) for repair in repairs for ref in repair["resources"])
    changed = request.state_changes.changes if request.state_changes else []
    candidates = [
        {
            "api_version": "apps/v1" if change.kind == "Deployment" else "v1",
            "kind": change.kind,
            "namespace": request.namespace,
            "name": change.name,
        }
        for change in changed
        if change.kind in {"Service", "Deployment"} and change.change == "modified"
    ]
    candidates.extend(
        json_format.MessageToDict(finding.primary_resource, preserving_proto_field_name=True)
        for finding in request.findings
    )
    for ref in candidates:
        if ref["kind"] not in {"Service", "Deployment"}:
            continue
        key = (ref["kind"], ref["name"])
        if key in seen:
            continue
        seen.add(key)
        action_start = _now()
        _patch_from_source(request, ref)
        repairs.append(
            {
                "action_id": f"source-{len(repairs) + 1}",
                "kind": "kubectl-patch",
                "target": f"{ref['kind']}/{ref['name']}",
                "resources": [ref],
                "summary": f"Restored {ref['kind']}/{ref['name']} from accepted source",
                "details": "Applied accepted source fields and waited for the rollout",
                "started_at": action_start,
                "completed_at": _now(),
                "success": True,
                "reversible": True,
            }
        )
"""

VERIFY = r"""
verification = first
for _ in range(0 if unsure else 24):
    verification = _tool("verify")
    if verification.get("healthy"):
        break
    time.sleep(5)
healthy = bool(verification.get("healthy"))
causes = [
    {
        "summary": finding.summary,
        "resources": [json_format.MessageToDict(finding.primary_resource, preserving_proto_field_name=True)],
        "evidence": [{"kind": "detector-finding", "source": finding.detector_id, "observation": finding.evidence}],
        "explained_detectors": [finding.detector_id],
    }
    for finding in request.findings
    if finding.detector_id and finding.evidence
]
if request.state_changes:
    causes.extend(
        {
            "summary": f"{change.kind}/{change.name} differed from the healthy baseline",
            "resources": [
                {
                    "api_version": "apps/v1" if change.kind == "Deployment" else "v1",
                    "kind": change.kind,
                    "namespace": request.namespace,
                    "name": change.name,
                }
            ],
            "evidence": [
                {
                    "kind": "state-change",
                    "source": f"{change.kind}/{change.name}",
                    "observation": f"{change.change} against the healthy baseline",
                }
            ],
            "explained_detectors": [finding.detector_id for finding in request.findings],
        }
        for change in request.state_changes.changes
        if change.kind in {"Service", "Deployment"} and change.change == "modified"
    )
completed = _now()
result = json_format.ParseDict(
    {
        "schema_version": "sdo.dev/v1alpha1",
        "incident_id": request.incident_id,
        "status": "completed" if healthy else "failed",
        "confirmed_root_causes": causes if repairs or plan_ids else [],
        "drift_events": [json_format.MessageToDict(label, preserving_proto_field_name=True) for label in labels],
        "applied_playbooks": [{"playbook_id": program_id} for program_id in programs],
        "repair_actions": repairs,
        "repair_changes": [item["summary"] for item in repairs],
        "verification_evidence": [
            {
                "name": "sdo verify",
                "passed": healthy,
                "details": verification.get("summary") or "detector verification",
                "observed_at": completed,
            }
        ],
        "usage": {"llm_calls": 0, "input_tokens": 0, "output_tokens": 0},
        "timing": {"started_at": started, "completed_at": completed},
        "responder_session_id": "scripted-fastloop",
    },
    IncidentResult(),
)
"""


def responder_script(request: IncidentRequest) -> AgentScript:
    """Return a script bound to this typed task, with live clocks and tool reads."""
    common = (responder_steps._now, responder_steps._command, responder_steps._tool)
    dependencies = (
        (*common, responder_steps._binding_args, responder_steps._matched_plans),
        (
            *common,
            responder_steps._manifest,
            responder_steps._patch_from_source,
            responder_steps._assess_drift,
            responder_steps._assess_policy_drift,
        ),
        (*common, responder_steps._patch_from_source),
        common,
    )
    imports = inspect.getsource(responder_steps).split("def _now", 1)[0]
    # The future import must remain at the beginning of each generated command.
    prelude = (
        imports
        + "\nimport os, time\nfrom google.protobuf import json_format\nfrom sdo.contracts import IncidentResult\n"
    )
    prelude += (
        "import sys\nraw_request = json.load(sys.stdin)\nraw_request.pop('@type', None)\n"
        "request = json_format.ParseDict(raw_request, IncidentRequest())\n"
    )
    prelude += "request.repository_worktree = str(Path.cwd())\n"
    key = hashlib.sha256(request.incident_id.encode()).hexdigest()[:16]
    prelude += f"state_path = Path(os.environ['SDO_TOOLS_DIR']) / 'responder-{key}.json'\n"
    save = "\nstate_path.write_text(json.dumps(state))\n"
    load = "state = json.loads(state_path.read_text())\n"
    phases = [
        PLAN + "\nstate = dict(started=started, plan_ids=plan_ids, programs=programs)" + save,
        load + DRIFT + "\nstate.update(repairs=repairs, labels=[json_format.MessageToDict(x, "
        "preserving_proto_field_name=True) for x in labels], unsure=list(unsure))" + save,
        load
        + "repairs = state['repairs']\nunsure = {tuple(x) for x in state['unsure']}\n"
        + REPAIR
        + "\nstate.update(repairs=repairs, first=first)"
        + save,
        load + "started, plan_ids, programs, repairs, first, unsure = (state[k] for k in ('started', "
        "'plan_ids', 'programs', 'repairs', 'first', 'unsure'))\nlabels = "
        "[json_format.ParseDict(x, DriftEvent()) for x in state['labels']]\n"
        + VERIFY
        + "\nstate_path.unlink()\nprint(json_format.MessageToJson(result, "
        "preserving_proto_field_name=True, always_print_fields_with_no_presence=True))\n",
    ]
    steps = [
        {
            "kind": "command",
            "command": shlex.join(
                ["python3", "-c", prelude + "\n".join(inspect.getsource(fn) for fn in dependencies[index]) + phase]
            ),
            "timeout": 300.0,
            "capture_json": index == 3,
            "input_json": "{{request}}",
        }
        for index, phase in enumerate(phases)
    ]
    fields = (
        "schema_version",
        "incident_id",
        "status",
        "confirmed_root_causes",
        "drift_events",
        "applied_playbooks",
        "repair_actions",
        "repair_changes",
        "verification_evidence",
        "usage",
        "timing",
        "responder_session_id",
    )
    return AgentScript.model_validate(
        {
            "version": 1,
            "turns": [
                {
                    "role": "responder",
                    "incident": request.incident_id,
                    "steps": steps,
                    "result": {field: "{{steps.3.output." + field + "}}" for field in fields},
                }
            ],
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    task = json_format.Parse(args.task.read_text(), AgentTask())
    request = IncidentRequest()
    if not task.request.Unpack(request):
        raise ValueError("task must carry an IncidentRequest")
    args.out.write_text(responder_script(request).model_dump_json(indent=2) + "\n")


if __name__ == "__main__":
    main()
