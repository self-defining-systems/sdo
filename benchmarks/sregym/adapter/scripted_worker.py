"""Deterministic responder and reflection workers for seeded fastloop runs."""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from google.protobuf import json_format

from sdo.agents.responder import (
    KubernetesResultPublisher,
    ReflectionTurn,
    run_drift_task,
    run_reflection_task,
    run_worker_task,
)
from sdo.agents.views import IncidentRequest, IncidentResult, OperationalArtifactDraft
from sdo.contracts import DriftInvestigationResult, DriftInvestigationTask, OperationalArtifactProposal

PROGRAM_ID = "service-targetport-mismatch"
PROGRAM_SOURCE = Path("/opt/sdo-scripted/targetport")
LOG = logging.getLogger(__name__)


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


def _manifest(root: Path, kind: str, name: str) -> dict[str, Any]:
    manifest_root = root / "kubernetes" if (root / "kubernetes").is_dir() else root
    for path in sorted((*manifest_root.rglob("*.yaml"), *manifest_root.rglob("*.yml"))):
        if "templates" in path.relative_to(manifest_root).parts:
            continue
        try:
            documents = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
        except (OSError, UnicodeError, yaml.YAMLError) as exc:
            LOG.warning("skipping unparseable source manifest %s: %s", path, exc)
            continue
        for document in documents:
            if not isinstance(document, dict) or document.get("kind") != kind:
                continue
            metadata = document.get("metadata")
            if isinstance(metadata, dict) and metadata.get("name") == name:
                return document
    raise RuntimeError(f"accepted source has no {kind}/{name} manifest")


def scripted_drift_investigation(request: DriftInvestigationTask) -> DriftInvestigationResult:
    result = DriftInvestigationResult()
    for index, event in enumerate(request.events):
        verdict = result.events.add(event_index=index, verdict="unsure", reason="No supported evidence rule matched")
        if event.ref.kind != "Service" or not event.fields:
            continue
        field = event.fields[0]
        match = re.fullmatch(r"spec\.ports\[(\d+)\]\.targetPort", field.path)
        if match is None:
            continue
        try:
            source = _manifest(Path(request.repository_worktree), "Service", event.ref.name)
            source_spec = source["spec"]["ports"][int(match.group(1))]
            source_port = source_spec.get("targetPort", source_spec["port"])
            live = json.loads(
                _command(["kubectl", "-n", request.namespace, "get", "service", event.ref.name, "-o", "json"])
            )
            pods = json.loads(_command(["kubectl", "-n", request.namespace, "get", "pods", "-o", "json"]))
            live_spec = live["spec"]["ports"][int(match.group(1))]
            live_port = live_spec.get("targetPort", live_spec["port"])
            selector = live["spec"].get("selector", {})
            selected = [
                pod
                for pod in pods.get("items", [])
                if selector
                and all(pod.get("metadata", {}).get("labels", {}).get(key) == value for key, value in selector.items())
            ]
        except (KeyError, IndexError, TypeError, ValueError, RuntimeError):
            continue
        if not selected or source_port == live_port or str(live_port) != field.new:
            continue
        served = any(
            port.get("containerPort") == live_port or port.get("name") == live_port
            for pod in selected
            for container in pod.get("spec", {}).get("containers", [])
            for port in container.get("ports", [])
        )
        if served:
            continue
        verdict.verdict = "bug"
        verdict.reason = "Service targetPort differs from source and no selected pod serves it"
        verdict.evidence.add(
            kind="source_contradiction", observation=f"source targetPort {source_port}, live targetPort {live_port}"
        )
        verdict.evidence.add(
            kind="violated_relation", observation=f"none of {len(selected)} selected pods serves targetPort {live_port}"
        )
        verdict.mitigation_request = f"restore Service/{event.ref.name} targetPort from accepted source"
    return result


def _patch_from_source(request: IncidentRequest, ref: dict[str, str]) -> None:
    kind, name = ref["kind"], ref["name"]
    source = _manifest(Path(request.repository_worktree), kind, name)
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
    for event in request.drift_events:
        if event.investigation_verdict != "bug" or event.ref.kind != "Service":
            continue
        ref = event.ref.model_dump(mode="json")
        action_start = _now()
        _patch_from_source(request, ref)
        repairs.append(
            {
                "action_id": f"source-{len(repairs) + 1}",
                "kind": "kubectl-patch",
                "target": f"Service/{event.ref.name}",
                "resources": [ref],
                "summary": f"Restored Service/{event.ref.name} from accepted source",
                "details": "Applied accepted source ports after investigator evidence",
                "started_at": action_start,
                "completed_at": _now(),
                "success": True,
                "reversible": True,
            }
        )
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


def _captured_pair(capture: Path, open_at: str = "pre-apply") -> tuple[str, str] | None:
    open_path = capture / f"open@{open_at}.json"
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
        open_at = (
            "detect"
            if any(event.investigation_verdict == "bug" for event in kwargs["closure"].request.drift_events)
            else "pre-apply"
        )
        pair = _captured_pair(capture, open_at)
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
                    open_at,
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
    if len(args) != 1 or args[0] not in {"responder", "reflection", "drift"}:
        print("usage: scripted_worker responder|reflection|drift", file=sys.stderr)
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
        elif args[0] == "reflection":
            run_reflection_task(
                Path(os.environ.get("SDO_TASK_PATH", "/sdo/request/agent-task.json")),
                result_name,
                publisher,
                reflector=ScriptedReflector(),
            )
        else:
            run_drift_task(
                Path(os.environ.get("SDO_TASK_PATH", "/sdo/request/agent-task.json")),
                result_name,
                publisher,
                investigator=scripted_drift_investigation,
            )
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
