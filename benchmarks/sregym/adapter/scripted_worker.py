"""Deterministic responder and reflection workers for seeded fastloop runs."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.protobuf import json_format

from sdo.agents.responder import KubernetesResultPublisher, ReflectionTurn, run_reflection_task, run_worker_task
from sdo.agents.views import IncidentRequest, IncidentResult, OperationalArtifactDraft
from sdo.contracts import OperationalArtifactProposal

PROGRAM_ID = "service-targetport-mismatch"
PROGRAM_SOURCE = Path("/opt/sdo-scripted/targetport")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _command(argv: list[str], *, timeout: int = 180) -> str:
    result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError(f"{' '.join(argv[:3])}: {result.stderr.strip() or result.stdout.strip()[:500]}")
    return result.stdout


def _tool(*args: str) -> dict[str, Any]:
    output = _command(["sdo", *args])
    response = json.loads(output.splitlines()[0])
    if not isinstance(response, dict) or response.get("error"):
        raise RuntimeError(f"sdo {' '.join(args)}: {response.get('error') if isinstance(response, dict) else output}")
    return response


def _patch_from_source(request: IncidentRequest, ref: dict[str, str]) -> None:
    kind, name = ref["kind"], ref["name"]
    source = _tool("source", f"{kind}/{request.namespace}/{name}")["source"]
    if kind == "Service":
        patch = {"spec": {"ports": source["spec"]["ports"]}}
    elif kind == "Deployment":
        containers = source["spec"]["template"]["spec"]["containers"]
        patch = {
            "spec": {
                "template": {
                    "spec": {"containers": [{"name": item["name"], "image": item["image"]} for item in containers]}
                }
            }
        }
    else:
        raise RuntimeError(f"scripted fastloop has no source repair for {kind}")
    _command(
        ["kubectl", "-n", request.namespace, "patch", kind.lower(), name, "--type=strategic", "-p", json.dumps(patch)]
    )
    if kind == "Deployment":
        _command(
            ["kubectl", "-n", request.namespace, "rollout", "status", f"deployment/{name}", "--timeout=120s"],
            timeout=130,
        )


def _binding_args(bindings: dict[str, Any]) -> list[str]:
    args: list[str] = []
    for name, ref in sorted(bindings.items()):
        args.extend(["--bind", f"{name}={ref['kind']}/{ref['namespace']}/{ref['name']}"])
    return args


def _matched_plans(diagnosis: dict[str, Any]) -> tuple[list[str], list[str]]:
    plan_ids: list[str] = []
    programs: list[str] = []
    seen: set[tuple[str, str]] = set()
    for run in diagnosis.get("program_runs", []):
        if run.get("verdict", {}).get("result") != "Match":
            continue
        program_id = run.get("program_id", "")
        bindings = run.get("bindings", {})
        key = (program_id, json.dumps(bindings, sort_keys=True))
        if not program_id or not isinstance(bindings, dict) or key in seen:
            continue
        seen.add(key)
        planned = _tool("plan", program_id, *_binding_args(bindings))
        if planned.get("plan_id"):
            plan_ids.append(planned["plan_id"])
            programs.append(program_id)
    return plan_ids, programs


def scripted_responder(request: IncidentRequest) -> IncidentResult:
    started = _now()
    diagnosis = _tool("diagnose")
    plan_ids, programs = _matched_plans(diagnosis)
    if plan_ids:
        _tool("apply", *plan_ids)
    repairs: list[dict[str, Any]] = []
    first = _tool("verify")
    if not first.get("healthy"):
        seen: set[tuple[str, str]] = set()
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
        candidates.extend(finding.primary_resource.model_dump(mode="json") for finding in request.findings)
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
    verification = first
    for _ in range(24):
        verification = _tool("verify")
        if verification.get("healthy"):
            break
        time.sleep(5)
    healthy = bool(verification.get("healthy"))
    causes = [
        {
            "summary": finding.summary,
            "resources": [finding.primary_resource.model_dump(mode="json")],
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
    return IncidentResult.model_validate(
        {
            "incident_id": request.incident_id,
            "status": "completed" if healthy else "failed",
            "confirmed_root_causes": causes,
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
        }
    )


def _captured_pair(capture: Path) -> tuple[str, str] | None:
    open_path = capture / "open@pre-apply.json"
    fixed_path = capture / "fixed.json"
    if not open_path.is_file() or not fixed_path.is_file():
        return None
    opened, fixed = (json.loads(path.read_text(encoding="utf-8")) for path in (open_path, fixed_path))
    fixed_services = {item["metadata"]["name"]: item for item in fixed.get("services", [])}
    for item in opened.get("services", []):
        name = item["metadata"]["name"]
        after = fixed_services.get(name)
        if after is None or item.get("spec", {}).get("ports") == after.get("spec", {}).get("ports"):
            continue
        for peer in fixed.get("services", []):
            peer_name = peer["metadata"]["name"]
            selector = peer.get("spec", {}).get("selector", {})
            if peer_name == name or not selector:
                continue
            pods = [
                pod
                for pod in fixed.get("pods", [])
                if all(pod.get("metadata", {}).get("labels", {}).get(key) == value for key, value in selector.items())
            ]
            if not pods:
                continue
            ports = peer.get("spec", {}).get("ports", [])
            if ports and all(
                any(
                    (exposed.get("containerPort") if isinstance(target, int) else exposed.get("name")) == target
                    for pod in pods
                    for container in pod.get("spec", {}).get("containers", [])
                    for exposed in container.get("ports", [])
                )
                for port in ports
                for target in (port.get("targetPort") or port.get("port"),)
            ):
                return name, peer_name
    return None


class ScriptedReflector:
    def resume(self, **kwargs: Any) -> ReflectionTurn:
        worktree = Path(kwargs["worktree"])
        incident = str(kwargs["incident_id"])
        if (worktree / ".sdo" / "playbooks" / PROGRAM_ID / "diagnose.go").is_file():
            return ReflectionTurn(
                summary="Target port program is already accepted",
                learning_decision="no_change",
                no_change_reason="Accepted program covers this class",
                proposed_changes=[],
            )
        capture = Path(os.environ.get("SDO_CAPTURED_CORPUS_DIR", "/sdo/session/corpus"))
        pair = _captured_pair(capture)
        if pair is None:
            return ReflectionTurn(
                summary="No captured target port repair",
                learning_decision="no_change",
                no_change_reason="Captured incident has no target port transition and healthy peer",
                proposed_changes=[],
            )
        with tempfile.TemporaryDirectory(prefix="sdo-scripted-program-") as temporary:
            target = Path(temporary) / PROGRAM_ID
            _command(
                [
                    "sdo-operator",
                    "program",
                    "scaffold",
                    "--fault-class",
                    PROGRAM_ID,
                    "--param",
                    "service=Service",
                    "--mitigate",
                    "--out",
                    str(target),
                ]
            )
            for name in ("README.md", "diagnose.go", "mitigate.go"):
                (target / name).write_bytes((PROGRAM_SOURCE / name).read_bytes())
            (target / "diagnose_test.go").unlink()
            output = _command(
                [
                    "sdo-operator",
                    "program",
                    "propose",
                    "--dir",
                    str(target),
                    "--incident",
                    incident,
                    "--corpus-from-incident",
                    incident,
                    "--open-at",
                    "pre-apply",
                    "--bind",
                    f"service=Service/{kwargs['closure'].request.namespace}/{pair[0]}",
                    "--peer-bind",
                    f"service=Service/{kwargs['closure'].request.namespace}/{pair[1]}",
                    "--app",
                    str(worktree),
                    "--operator-root",
                    "/opt/sdo/sdo/operator",
                ],
                timeout=120,
            )
            typed = json_format.Parse(output, OperationalArtifactProposal())
            proposal = OperationalArtifactDraft.model_validate(
                json_format.MessageToDict(typed, preserving_proto_field_name=True)
            )
        return ReflectionTurn(
            summary="Compiled target port program from captured repair",
            learning_decision="updated",
            proposed_changes=[],
            artifact_proposal=proposal,
        )


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if len(args) != 1 or args[0] not in {"responder", "reflection"}:
        print("usage: scripted_worker responder|reflection", file=sys.stderr)
        return 2
    namespace = os.environ.get("SDO_NAMESPACE", "")
    result_name = os.environ.get("SDO_RESULT_CONFIGMAP", "")
    if not namespace or not result_name:
        print("SDO_NAMESPACE and SDO_RESULT_CONFIGMAP are required", file=sys.stderr)
        return 2
    publisher = KubernetesResultPublisher(namespace=namespace)
    try:
        if args[0] == "responder":
            run_worker_task(
                Path(os.environ.get("SDO_TASK_PATH", "/sdo/request/agent-task.json")),
                result_name,
                publisher,
                executor=scripted_responder,
            )
        else:
            run_reflection_task(
                Path(os.environ.get("SDO_TASK_PATH", "/sdo/request/agent-task.json")),
                result_name,
                publisher,
                reflector=ScriptedReflector(),
            )
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
