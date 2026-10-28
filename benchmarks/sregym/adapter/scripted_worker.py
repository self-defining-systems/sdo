"""Deterministic responder and reflection workers for seeded fastloop runs."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from google.protobuf import json_format

from sdo.agents.health_judge import run_judge_task
from sdo.agents.responder import (
    KubernetesResultPublisher,
    ReflectionTurn,
    await_standby_claim,
    recorded_actions,
    run_reflection_task,
    run_worker_task,
)
from sdo.contracts import (
    DriftEvent,
    HealthJudgeRequest,
    HealthJudgeResult,
    IncidentRequest,
    IncidentResult,
)

PROGRAM_ID = "service-targetport-mismatch"
PROGRAM_SOURCE = Path("/opt/sdo-scripted/targetport")
NETWORKPOLICY_PROGRAM_ID = "networkpolicy-service-isolation"
NETWORKPOLICY_PROGRAM_SOURCE = Path("/opt/sdo-scripted/networkpolicy")
#: Kinds the scripted responder restores from accepted source, and the field that carries their state.
SOURCE_RESTORED_FIELDS = {"ConfigMap": "data", "Ingress": "spec", "HorizontalPodAutoscaler": "spec"}
#: Kinds a fault adds that accepted source never declares; the scripted responder deletes them.
REMOVED_KINDS = frozenset(
    {
        "NetworkPolicy",
        "ResourceQuota",
        "LimitRange",
        "PodDisruptionBudget",
        "MutatingWebhookConfiguration",
        "ValidatingWebhookConfiguration",
    }
)
CLUSTER_SCOPED_KINDS = frozenset({"MutatingWebhookConfiguration", "ValidatingWebhookConfiguration"})
LOG = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _command(argv: list[str], *, timeout: int = 180) -> str:
    result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError(f"{' '.join(argv[:3])}: {result.stderr.strip() or result.stdout.strip()[:500]}")
    return result.stdout


def _tool(*args: str) -> dict[str, Any]:
    output = _command(["sdo", *args, "--json"])
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


def _patch_from_source(request: IncidentRequest, ref: dict[str, str]) -> None:
    kind, name = ref["kind"], ref["name"]
    source = _tool("source", f"{kind}/{request.namespace}/{name}")["source"]
    if kind == "Service":
        patch = {"spec": {"ports": source["spec"]["ports"]}}
    elif kind == "Deployment":
        containers = source["spec"]["template"]["spec"]["containers"]
        baseline_images = {
            field.field: field.before
            for change in (request.state_changes.changes if request.state_changes else [])
            if change.kind == kind and change.name == name
            for field in change.fields or []
            if field.before
        }
        patch = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": item["name"],
                                "image": item["image"]
                                if "@sha256:" in item["image"]
                                else baseline_images.get(f"container[{item['name']}].image", item["image"]),
                                **{key: item.get(key) for key in ("readinessProbe", "livenessProbe", "startupProbe")},
                            }
                            for item in containers
                        ]
                    }
                }
            }
        }
    elif kind in SOURCE_RESTORED_FIELDS:
        field = SOURCE_RESTORED_FIELDS[kind]
        patch = {field: source[field]}
    else:
        raise RuntimeError(f"scripted fastloop has no source repair for {kind}")
    patch_type = "merge" if kind in SOURCE_RESTORED_FIELDS else "strategic"
    _command(
        [
            "kubectl",
            "-n",
            request.namespace,
            "patch",
            kind.lower(),
            name,
            f"--type={patch_type}",
            "-p",
            json.dumps(patch),
        ]
    )
    if kind == "Deployment":
        _command(
            ["kubectl", "-n", request.namespace, "rollout", "status", f"deployment/{name}", "--timeout=600s"],
            timeout=610,
        )


def _remove_added(request: IncidentRequest, ref: dict[str, str]) -> None:
    """Delete an object a fault added that accepted source does not declare."""

    scope = [] if ref["kind"] in CLUSTER_SCOPED_KINDS else ["-n", request.namespace]
    _command(["kubectl", *scope, "delete", ref["kind"].lower(), ref["name"], "--ignore-not-found"])


def _binding_args(bindings: dict[str, Any]) -> list[str]:
    args: list[str] = []
    if bindings.get("values"):
        raise ValueError("scripted plan CLI does not support scalar value bindings")
    objects = bindings.get("objects", {}) if "objects" in bindings or "values" in bindings else bindings
    for name, ref in sorted(objects.items()):
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


def _assess_drift(request: IncidentRequest, event: DriftEvent) -> DriftEvent:
    label = DriftEvent()
    label.CopyFrom(event)
    label.investigation_verdict = "unsure"
    label.investigation_reason = "No supported live relation evidence"
    del label.investigation_evidence[:]
    if event.ref.kind == "NetworkPolicy":
        return _assess_policy_drift(request, event, label)
    if event.ref.kind != "Service" or len(event.fields) != 1:
        return label
    match = re.fullmatch(r"spec\.ports\[(\d+)\]\.targetPort", event.fields[0].path)
    if match is None:
        return label
    try:
        source = _manifest(Path(request.repository_worktree), "Service", event.ref.name)
        source_port = source["spec"]["ports"][int(match.group(1))].get("targetPort")
        live = json.loads(
            _command(["kubectl", "-n", request.namespace, "get", "service", event.ref.name, "-o", "json"])
        )
        live_port = live["spec"]["ports"][int(match.group(1))].get("targetPort")
        pods = json.loads(_command(["kubectl", "-n", request.namespace, "get", "pods", "-o", "json"]))
        selector = live["spec"].get("selector", {})
        selected = [
            pod
            for pod in pods.get("items", [])
            if selector
            and all(pod.get("metadata", {}).get("labels", {}).get(key) == value for key, value in selector.items())
        ]
        if not selected or source_port == live_port or str(live_port) != event.fields[0].new:
            return label
        if not isinstance(source_port, int) or not isinstance(live_port, int):
            return label
        # Probe Pod IPs directly: scratch containers need no exec utilities.
        # A timeout or another ambiguous network error leaves this unresolved.
        for pod in selected:
            address = pod["status"]["podIP"]
            with socket.create_connection((address, source_port), timeout=2):
                pass
            try:
                with socket.create_connection((address, live_port), timeout=2):
                    return label
            except ConnectionRefusedError:
                pass
    except (KeyError, IndexError, TypeError, ValueError, RuntimeError, OSError):
        return label
    label.investigation_verdict = "bug"
    label.investigation_reason = "Service contradicts accepted source and targets an observed non-listening port"
    label.investigation_evidence.add(kind="source_contradiction", observation=f"source {source_port}, live {live_port}")
    label.investigation_evidence.add(
        kind="violated_relation",
        observation=(f"all {len(selected)} selected Pod IPs accept TCP on {source_port} and refuse TCP on {live_port}"),
    )
    return label


def _assess_policy_drift(request: IncidentRequest, event: DriftEvent, label: DriftEvent) -> DriftEvent:
    """Recognize complete ingress isolation; abstain from partial-rule reasoning."""
    if event.source != "absent from accepted source" or len(event.fields) != 1 or event.fields[0].path != "spec":
        return label
    try:

        def objects(kind: str) -> list[dict[str, Any]]:
            return json.loads(_command(["kubectl", "-n", request.namespace, "get", kind, "-o", "json"]))["items"]

        policies = objects("networkpolicies")
        policy = next(item for item in policies if item["metadata"]["name"] == event.ref.name)
        spec = policy["spec"]
        if (
            spec != json.loads(event.fields[0].new)
            or spec.get("ingress")
            or "Ingress" not in spec.get("policyTypes", ["Ingress"])
        ):
            return label
        if any(item["spec"]["podSelector"].get("matchExpressions") for item in policies):
            return label

        def selects(item: dict[str, Any], pod: dict[str, Any]) -> bool:
            return all(
                pod["metadata"].get("labels", {}).get(key) == value
                for key, value in item["spec"]["podSelector"].get("matchLabels", {}).items()
            )

        def ports(item: dict[str, Any]) -> list[tuple[str, int, int | str, str]]:
            return [
                (port.get("name", ""), port["port"], port.get("targetPort", port["port"]), port.get("protocol", "TCP"))
                for port in item["spec"].get("ports", [])
            ]

        pods = objects("pods")
        for service in objects("services"):
            selector = service["spec"].get("selector", {})
            if not selector or not service["spec"].get("ports"):
                continue
            name = service["metadata"]["name"]
            accepted = _tool("source", f"Service/{request.namespace}/{name}")["source"]
            if selector != accepted["spec"].get("selector") or ports(service) != ports(accepted):
                continue
            for pod in pods:
                if not all(pod["metadata"].get("labels", {}).get(key) == value for key, value in selector.items()):
                    continue
                if not selects(policy, pod) or any(
                    selects(other, pod)
                    and "Ingress" in other["spec"].get("policyTypes", ["Ingress"])
                    and other["spec"].get("ingress")
                    for other in policies
                ):
                    continue
                label.investigation_verdict = "bug"
                label.investigation_reason = "Source-absent policy fully isolates an accepted Service pod"
                label.investigation_evidence.add(kind="source_contradiction", observation=event.source)
                label.investigation_evidence.add(
                    kind="violated_relation",
                    observation=(
                        f"Service/{name} selects Pod/{pod['metadata']['name']} with no selecting ingress allow rules"
                    ),
                )
                return label
    except (KeyError, StopIteration, TypeError, ValueError, RuntimeError):
        return label
    return label


def _health_related(request: IncidentRequest, objects: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """Objects a non-drift health finding names directly or selects through an accepted Service."""

    related: set[tuple[str, str]] = set()
    for finding in request.findings:
        if finding.detector_id.startswith("drift."):
            continue
        ref = finding.primary_resource
        if (ref.kind, ref.name) in objects:
            related.add((ref.kind, ref.name))
        workloads = {
            (kind, name) for kind, name in objects - related if kind in {"Deployment", "StatefulSet", "DaemonSet"}
        }
        if ref.kind != "Service" or not workloads:
            continue
        try:
            selector = _tool("source", f"Service/{request.namespace}/{ref.name}")["source"]["spec"].get("selector")
            for kind, name in sorted(workloads) if selector else ():
                source = _tool("source", f"{kind}/{request.namespace}/{name}")["source"]
                labels = source["spec"]["template"]["metadata"].get("labels") or {}
                if all(labels.get(key) == value for key, value in selector.items()):
                    related.add((kind, name))
        except (KeyError, TypeError, RuntimeError):
            continue
    return related


def scripted_responder(request: IncidentRequest) -> IncidentResult:
    if os.environ.get("SDO_SCRIPTED_REPLAY_SLOT") is not None:
        from .scripted_replay import replay_recorded_repair as replay

        def run(command: str, *, timeout: float = 300) -> tuple[int, str]:
            completed = subprocess.run(
                ["/bin/sh", "-c", command],
                cwd=request.repository_worktree,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            return completed.returncode, completed.stdout + completed.stderr

        recorded_actions.reset()
        result = replay(
            run,
            int(os.environ["SDO_SCRIPTED_REPLAY_SLOT"]),
            json_format.MessageToDict(request, preserving_proto_field_name=True),
            Path(request.repository_worktree),
        )
        result["schema_version"] = "sdo.dev/v1alpha1"
        response = json_format.ParseDict(result, IncidentResult())
        recorded_actions.merge_into(response)
        return response
    started = _now()
    diagnosis = _tool("diagnose")
    plan_ids, programs = _matched_plans(diagnosis)
    if plan_ids:
        _tool("apply", *plan_ids)
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
    first = _tool("verify")
    if not first.get("healthy"):
        # Unknown drift alone is not repair authority. A separate health
        # finding on the object, or on a Service selecting its pods, still
        # authorizes source-backed recovery.
        seen: set[tuple[str, str]] = unsure - _health_related(request, unsure)
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
            if (change.kind in {"Service", "Deployment", *SOURCE_RESTORED_FIELDS} and change.change == "modified")
            or (change.kind in REMOVED_KINDS and change.change == "added")
        ]
        candidates.extend(
            json_format.MessageToDict(finding.primary_resource, preserving_proto_field_name=True)
            for finding in request.findings
        )
        for ref in candidates:
            removed = ref["kind"] in REMOVED_KINDS
            if ref["kind"] not in {"Service", "Deployment", *SOURCE_RESTORED_FIELDS} and not removed:
                continue
            key = (ref["kind"], ref["name"])
            if key in seen:
                continue
            seen.add(key)
            unsure.discard(key)
            action_start = _now()
            if removed:
                _remove_added(request, ref)
            else:
                _patch_from_source(request, ref)
            repairs.append(
                {
                    "action_id": f"source-{len(repairs) + 1}",
                    "kind": "kubectl-delete" if removed else "kubectl-patch",
                    "target": f"{ref['kind']}/{ref['name']}",
                    "resources": [ref],
                    "summary": f"Removed source-absent {ref['kind']}/{ref['name']}"
                    if removed
                    else f"Restored {ref['kind']}/{ref['name']} from accepted source",
                    "details": "Deleted the object the fault added"
                    if removed
                    else "Applied accepted source fields and waited for the rollout",
                    "started_at": action_start,
                    "completed_at": _now(),
                    "success": True,
                    "reversible": not removed,
                }
            )
    verification = first
    for _ in range(0 if unsure else 24):
        verification = _tool("verify")
        if verification.get("healthy"):
            break
        time.sleep(5)
    healthy = bool(verification.get("healthy"))
    if healthy and os.environ.get("SDO_SCRIPTED_SOURCE_REPAIR") == "1":
        # Refine an existing source object: source publication must not invent
        # topology that the independent toy judge correctly refuses.
        marker = "sdo.dev/scripted-verification"
        candidate = None
        present = False
        for path in sorted((Path(request.repository_worktree) / "kubernetes").rglob("*.yaml")):
            documents = list(yaml.safe_load_all(path.read_text()))
            for obj in documents:
                if not isinstance(obj, dict) or obj.get("kind") not in {
                    "Service",
                    "Deployment",
                    "StatefulSet",
                    "DaemonSet",
                    "ConfigMap",
                }:
                    continue
                metadata = obj.get("metadata", {})
                if (
                    not obj.get("apiVersion")
                    or not metadata.get("name")
                    or metadata.get("namespace", request.namespace) != request.namespace
                ):
                    continue
                if metadata.get("annotations", {}).get(marker) == "independent-health":
                    present = True
                if candidate is None:
                    candidate = path, documents, obj
        if not present:
            if candidate is None:
                raise ValueError("scripted source repair requires an existing deployment manifest")
            path, documents, obj = candidate
            obj["metadata"].setdefault("annotations", {})[marker] = "independent-health"
            path.write_text(yaml.safe_dump_all(documents, sort_keys=False))
            action_start = _now()
            _command(["kubectl", "-n", request.namespace, "apply", "-f", str(path)])
            repairs.append(
                {
                    "action_id": f"source-{len(repairs) + 1}",
                    "kind": "kubectl-apply",
                    "target": f"{obj['kind']}/{obj['metadata']['name']}",
                    "resources": [
                        {
                            "api_version": obj["apiVersion"],
                            "kind": obj["kind"],
                            "namespace": request.namespace,
                            "name": obj["metadata"]["name"],
                        }
                    ],
                    "summary": "Recorded scripted verification policy in source",
                    "details": "Applied a source-backed annotation without changing topology",
                    "started_at": action_start,
                    "completed_at": _now(),
                    "success": True,
                    "reversible": True,
                }
            )
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
    return json_format.ParseDict(
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


def _captured_pair(capture: Path, open_at: str = "detect") -> tuple[str, str] | None:
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


def _captured_policy_pair(capture: Path, open_at: str) -> tuple[str, str] | None:
    """Find a removed policy and a retained same-kind healthy peer in captures."""
    opened_path, fixed_path = capture / f"open@{open_at}.json", capture / "fixed.json"
    if not opened_path.is_file() or not fixed_path.is_file():
        return None
    opened, fixed = (json.loads(path.read_text(encoding="utf-8")) for path in (opened_path, fixed_path))
    fixed_policies = {item["metadata"]["name"]: item for item in (fixed.get("networkPolicies") or [])}
    for item in opened.get("networkPolicies") or []:
        name = item["metadata"]["name"]
        if name not in fixed_policies and fixed_policies:
            return name, sorted(fixed_policies)[0]
    return None


def _check_reflection(worktree: Path, playbook_ids: list[str]) -> None:
    with tempfile.TemporaryDirectory(prefix="sdo-scripted-decision-") as directory:
        decision = Path(directory) / "decision.json"
        decision.write_text(
            json.dumps({"learning_decision": "updated", "proposed_artifact_ids": playbook_ids}), encoding="utf-8"
        )
        _command(["sdo-operator", "memory", "check", "--root", str(worktree), "--decision", str(decision)], timeout=180)


def _checkout_reflection(worktree: Path, summary: str, reason: str) -> ReflectionTurn:
    """Report the actual publication diff, including Go-prepared corpus edits."""
    base = os.environ.get("SDO_BASE_COMMIT", "HEAD")
    git = ["git", "-C", str(worktree)]
    paths: set[str] = set()
    for args in (
        [
            "-c",
            "diff.external=",
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--name-only",
            "-z",
            base,
            "--",
        ],
        ["ls-files", "--others", "--exclude-standard", "-z"],
        ["ls-files", "--others", "-z", "--", ".sdo/lib", ".sdo/corpus", ".sdo/rendered"],
    ):
        paths.update(path for path in _command([*git, *args]).split("\0") if path)
    ids = sorted(
        {parts[2] for path in paths if len(parts := Path(path).parts) >= 4 and parts[:2] == (".sdo", "playbooks")}
    )
    if paths:
        _check_reflection(worktree, ids)
        return ReflectionTurn(
            summary="Publish existing checkout edits: " + summary,
            learning_decision="updated",
            playbook_ids=ids,
        )
    return ReflectionTurn(summary=summary, learning_decision="no_change", no_change_reason=reason, playbook_ids=[])


class ScriptedReflector:
    def resume(self, **kwargs: Any) -> ReflectionTurn:
        worktree = Path(kwargs["worktree"])
        if os.environ.get("SDO_SCRIPTED_REPLAY_SLOT") is not None:
            return _checkout_reflection(
                worktree,
                "Recorded source restoration has no generalized program fixture",
                "Rehearsal verifies closure without inventing reusable memory",
            )
        incident = str(kwargs["incident_id"])
        outcome = kwargs.get("outcome")
        if outcome is not None and outcome.classification != "success" and not kwargs.get("cause_work"):
            return _checkout_reflection(worktree, "No independently eligible repair to save", "no recorded repair")
        capture = Path(os.environ.get("SDO_CAPTURED_CORPUS_DIR", "/sdo/session/corpus"))
        open_at = "detect"
        policy_pair = _captured_policy_pair(capture, open_at)
        program_id = NETWORKPOLICY_PROGRAM_ID if policy_pair else PROGRAM_ID
        program_source = NETWORKPOLICY_PROGRAM_SOURCE if policy_pair else PROGRAM_SOURCE
        parameter = "policy=NetworkPolicy" if policy_pair else "service=Service"
        if (worktree / ".sdo" / "playbooks" / program_id / "diagnose.go").is_file():
            return _checkout_reflection(
                worktree, f"{program_id} program is already accepted", "Accepted program covers this class"
            )
        pair = policy_pair or _captured_pair(capture, open_at)
        if pair is None:
            if os.environ.get("SDO_SCRIPTED_MEMORY_CHANGE") == "1":
                # Exercise real memory publication even when this repair has
                # no program fixture. This adds no diagnosis or trust credit.
                notes_id = "verified-repair-notes"
                books = worktree / ".sdo/playbooks"
                notes = books / notes_id / "README.md"
                notes.parent.mkdir(parents=True, exist_ok=True)
                if not notes.exists():
                    notes.write_text(
                        f"---\nschema_version: 1\nowner: responder\nfault_class: {notes_id}\n"
                        f"originating_incident: {incident}\n---\n# Verification observations\n\n"
                        "Verify <RESOURCE_NAME> against accepted source after each repair.\n",
                        encoding="utf-8",
                    )
                with notes.open("a", encoding="utf-8") as stream:
                    stream.write(f"\n- Independent health verification completed for incident {incident}.\n")
                index = books / "README.md"
                body = index.read_text(encoding="utf-8") if index.exists() else "# Operational playbooks\n"
                link = f"- [{notes_id}]({notes_id}/README.md)"
                if link not in body:
                    index.write_text(body.rstrip() + "\n\n" + link + "\n", encoding="utf-8")
                _check_reflection(worktree, [notes_id])
                return ReflectionTurn(
                    summary="Recorded an independently verified repair without a program fixture",
                    learning_decision="updated",
                    playbook_ids=[notes_id],
                )
            return _checkout_reflection(
                worktree,
                "No captured target port repair",
                "Captured incident has no target port transition and healthy peer",
            )
        target = worktree / ".sdo/playbooks" / program_id
        target.parent.mkdir(parents=True, exist_ok=True)
        _command(
            [
                "sdo-operator",
                "program",
                "scaffold",
                "--fault-class",
                program_id,
                "--param",
                parameter,
                *([] if policy_pair else ["--mitigate"]),
                "--out",
                str(target),
            ]
        )
        files = ("README.md", "diagnose.go") if policy_pair else ("README.md", "diagnose.go", "mitigate.go")
        for name in files:
            (target / name).write_bytes((program_source / name).read_bytes())
        body = (target / "README.md").read_text(encoding="utf-8")
        if body.startswith("---\n"):
            body = body.split("\n---\n", 1)[1]
        (target / "README.md").write_text(
            f"---\nschema_version: 1\nowner: responder\nfault_class: {program_id}\n"
            f"originating_incident: {incident}\n---\n{body}",
            encoding="utf-8",
        )
        (target / "diagnose_test.go").unlink()
        case = worktree / ".sdo/corpus" / (program_id + "-" + hashlib.sha256(incident.encode()).hexdigest()[:8])
        case.mkdir(parents=True)
        opened = (capture / f"open@{open_at}.json").read_bytes()
        fixed = (capture / "fixed.json").read_bytes()
        for name, data in {
            "open.json": opened,
            "fixed.json": fixed,
            "healthy.json": fixed,
            "recordings.json": (capture / f"open@{open_at}-recordings.json").read_bytes(),
        }.items():
            (case / name).write_bytes(data)
        binding_name, kind = parameter.split("=", 1)

        def binding(name: str) -> dict[str, str]:
            return {
                "api_version": "networking.k8s.io/v1" if kind == "NetworkPolicy" else "v1",
                "kind": kind,
                "namespace": kwargs["closure"].request.namespace,
                "name": name,
            }

        labels = {
            "root_cause_program_ids": [program_id],
            "bindings": {binding_name: binding(pair[0])},
            "peer_bindings": {binding_name: binding(pair[1])},
            "provenance": {
                "incident_id": incident,
                "open_at": open_at,
                "open_sha256": hashlib.sha256(opened).hexdigest(),
                "fixed_sha256": hashlib.sha256(fixed).hexdigest(),
            },
        }
        (case / "labels.json").write_text(json.dumps(labels), encoding="utf-8")
        books = target.parent
        (books / "README.md").write_text(
            "# Operational playbooks\n\n"
            + "".join(
                f"- [{directory.name}]({directory.name}/README.md)\n"
                for directory in sorted(books.iterdir())
                if directory.is_dir()
            ),
            encoding="utf-8",
        )
        if not policy_pair:
            # The port fixture must supply detector coverage as well as the
            # program and recorded replay required by verified repair learning.
            module = next(
                line.split()[1]
                for line in (worktree / ".sdo/diagnostics/go.mod").read_text().splitlines()
                if line.startswith("module ")
            )
            detector = worktree / ".sdo/diagnostics/detectors/incidents" / program_id
            detector.mkdir(parents=True, exist_ok=True)
            for name in ("detector.go", "manifest.yaml"):
                template = (program_source / f"{name}.tmpl").read_text()
                (detector / name).write_text(template.replace("MODULE", module).replace("INCIDENT", incident))
        _check_reflection(worktree, [program_id])
        return ReflectionTurn(
            summary=f"Compiled {program_id} program from captured repair",
            learning_decision="updated",
            playbook_ids=[program_id],
        )


TOY_SEED = Path(__file__).resolve().parents[3] / "apps/toy/seed/.sdo"
REQUIRED_HEALTH_DETECTORS = frozenset(
    {"health-objective", "service-endpoints", "service-reachability", "workload-health"}
)


@dataclass(frozen=True)
class ScriptedJudgeSeed:
    """Everything the scripted health judge reads from the accepted checkout."""

    toy: bool
    objective: str
    detector_ids: tuple[str, ...] = ()


def _read_seed(path: Path, what: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"scripted health refresh requires {what} ({path.name}: {exc.strerror})") from exc


def _judge_owned_ids(accepted: Path, toy: bool = False) -> tuple[str, ...]:
    label = "scripted toy health refresh" if toy else "scripted health refresh"
    ids = []
    for path in sorted((accepted / "diagnostics/detectors/health").glob("*/manifest.yaml")):
        manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
        if (
            not isinstance(manifest, dict)
            or manifest.get("class") != "health"
            or manifest.get("owner") != "health_judge"
        ):
            raise ValueError(f"{label} requires judge-owned health detectors")
        ids.append(manifest["id"])
    return tuple(ids)


def scripted_judge_seed(checkout: Path) -> ScriptedJudgeSeed:
    """Load the accepted-seed prerequisites of the scripted health judge.

    The judge only retains an already accepted bundle, so a fresh seed without
    a full judge-owned bundle cannot pass the scripted rehearsal. The eval dry
    run calls this before touching a lane.
    """
    accepted = checkout / ".sdo"
    if (accepted / "diagnostics/detectors/health/toy-endpoints").is_dir():
        for name in ("goal.md", "lifecycle-provenance.yaml"):
            if not (accepted / name).is_file() or (accepted / name).read_bytes() != (TOY_SEED / name).read_bytes():
                raise ValueError("scripted toy health refresh requires the unchanged toy seed")
        health = Path("diagnostics/detectors/health")
        for path in (TOY_SEED / health).rglob("*"):
            if path.is_file():
                current = accepted / path.relative_to(TOY_SEED)
                if not current.is_file() or current.read_bytes() != path.read_bytes():
                    raise ValueError("scripted toy health refresh requires the complete unchanged toy detector")
        objective = (TOY_SEED / "goal.md").read_text().split("---\n", 2)[2].strip()
        return ScriptedJudgeSeed(toy=True, objective=objective)
    parts = _read_seed(accepted / "goal.md", "an accepted goal document").split("---\n", 2)
    if len(parts) != 3 or parts[0]:
        raise ValueError("scripted health refresh requires an accepted goal document")
    # Match memory.HealthInputs: the request carries the human objective, not
    # the document header or its human-ownership metadata.
    goal = parts[2].strip().removeprefix("# Health objective\n").strip()
    ids = _judge_owned_ids(accepted)
    missing = sorted(REQUIRED_HEALTH_DETECTORS.difference(ids))
    if missing:
        raise ValueError(
            f"scripted health refresh cannot weaken the accepted full bundle (missing {', '.join(missing)})"
        )
    return ScriptedJudgeSeed(toy=False, objective=goal, detector_ids=ids)


def _scripted_toy_health_judge(
    request: HealthJudgeRequest, checkout: Path, seed: ScriptedJudgeSeed
) -> HealthJudgeResult:
    """Retain the exact hand-authored toy policy after checking its source shape."""
    accepted = checkout / ".sdo"
    objective = seed.objective
    if request.namespace != "sdo-toy" or request.health_objective.strip() != objective:
        raise ValueError("scripted toy health refresh cannot change the namespace or objective")
    expected = set()
    for path in sorted((TOY_SEED.parents[1] / "kubernetes").glob("*.yaml")):
        for obj in yaml.safe_load_all(path.read_text()):
            expected.add((obj["kind"], obj["metadata"].get("namespace", "sdo-toy"), obj["metadata"]["name"]))
    actual = {
        (row.kind, "sdo-toy" if row.namespace in ("", "default") else row.namespace, row.name)
        for row in request.source_resources
        if row.source.startswith("kubernetes/")
    }
    if not expected or actual != expected:
        raise ValueError("scripted toy health refresh cannot change the deployed topology")
    health = Path("diagnostics/detectors/health")
    # Native judge publication requires an explicit objective detector. Reuse
    # the toy's exact readiness policy; Go adds and validates generic detectors.
    objective_detector = accepted / health / "health-objective"
    objective_detector.mkdir(exist_ok=True)
    for name in ("manifest.yaml", "detector.go"):
        content = (TOY_SEED / health / "toy-endpoints" / name).read_text()
        (objective_detector / name).write_text(content.replace("toy-endpoints", "health-objective"))
    ids = list(_judge_owned_ids(accepted, toy=True))
    return HealthJudgeResult(
        authored=True,
        summary="Authored the exact toy readiness objective and retained its accepted detector",
        agent_session_id="scripted-toy-health-" + request.accepted_source_commit[:12],
        objective_digest=hashlib.sha256(objective.encode()).hexdigest(),
        detector_ids=ids,
    )


def scripted_health_judge(request: HealthJudgeRequest, checkout: Path) -> HealthJudgeResult:
    """Retain the accepted generic bundle for the rehearsal's unchanged topology.

    The production task wrapper and Go broker still validate the result and the
    complete unchanged detector bundle. New topology/objectives require a judge
    outside this explicitly scripted rehearsal.
    """
    seed = scripted_judge_seed(checkout)
    if seed.toy:
        return _scripted_toy_health_judge(request, checkout, seed)
    if "SDO_SCRIPTED_REPLAY_SLOT" in os.environ:
        from benchmarks.sregym.adapter.scripted_replay_catalog import SCRIPTED_REPLAY_SLOTS

        slot = int(os.environ["SDO_SCRIPTED_REPLAY_SLOT"])
        if request.namespace != SCRIPTED_REPLAY_SLOTS[slot].namespace:
            raise ValueError("scripted health refresh namespace differs from rehearsal")
    goal = seed.objective
    if request.health_objective.strip() != goal:
        raise ValueError("scripted health refresh cannot change the objective")
    return HealthJudgeResult(
        authored=True,
        summary="Retained complete accepted generic health bundle",
        agent_session_id="scripted-health-" + request.accepted_source_commit[:12],
        objective_digest=hashlib.sha256(goal.encode()).hexdigest(),
        detector_ids=list(seed.detector_ids),
    )


def main(argv: list[str] | None = None) -> int:
    await_standby_claim()
    args = argv if argv is not None else sys.argv[1:]
    if len(args) != 1 or args[0] not in {"responder", "reflection", "health-judge"}:
        print("usage: scripted_worker responder|reflection|health-judge", file=sys.stderr)
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
        elif args[0] == "health-judge":
            run_judge_task(
                Path(os.environ.get("SDO_TASK_PATH", "/sdo/request/agent-task.json")),
                result_name,
                publisher,
                execute=scripted_health_judge,
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
