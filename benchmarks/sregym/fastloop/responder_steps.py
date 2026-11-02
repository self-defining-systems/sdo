"""Live evidence and source-repair snippets embedded in generated agent commands.

These functions are emitted as source, so scripts need no legacy worker import.
Their script and transport contracts are tested with synthetic scenarios.
"""

from __future__ import annotations

import json
import logging
import re
import socket
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from sdo.contracts import DriftEvent, IncidentRequest

LOG = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
                                **{
                                    key: item[key]
                                    for key in ("readinessProbe", "livenessProbe", "startupProbe")
                                    if key in item
                                },
                            }
                            for item in containers
                        ]
                    }
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
