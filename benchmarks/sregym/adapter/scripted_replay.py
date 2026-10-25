"""Replay recorded repairs using accepted source, real sdo act receipts and verify.

The callable is Turn.run in scripted Codex and the existing scripted worker's
shell runner. Source edits stay in the proposal checkout; Go owns publication.
No oracle, harness recovery routine or Secret read is available here.
"""

from __future__ import annotations

import copy
import json
import re
import shlex
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import yaml

from .scripted_replay_catalog import SCRIPTED_REPLAY_SLOTS, Step


class PlanError(RuntimeError):
    """A recorded repair could not be safely executed."""


class Runner(Protocol):
    def __call__(self, command: str, *, timeout: float = ...) -> tuple[int, str]: ...


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


POLL_SECONDS = 5
ROLLOUT_SECONDS = 600


def _run(run: Runner, argv: list[str], *, timeout: float = 240) -> str:
    code, output = run(shlex.join(argv), timeout=timeout)
    if code:
        raise PlanError(f"{shlex.join(argv[:3])} failed ({code}): {output[:400]}")
    return output


def _tool(run: Runner, *args: str) -> dict[str, Any]:
    output = _run(run, ["sdo", *args, "--json"])
    try:
        response = json.loads(output.splitlines()[0])
    except (IndexError, json.JSONDecodeError) as exc:
        raise PlanError(f"sdo {args[0]} returned no JSON") from exc
    if not isinstance(response, dict) or response.get("error"):
        raise PlanError(
            f"sdo {args[0]} failed: {response.get('error') if isinstance(response, dict) else 'invalid response'}"
        )
    return response


def _ref(step: Step, namespace: str) -> str:
    return (
        f"{step.kind}/{step.name}"
        if step.kind.endswith("WebhookConfiguration")
        else f"{step.kind}/{namespace}/{step.name}"
    )


def _act(run: Runner, resource: str, summary: str, argv: list[str]) -> None:
    _run(run, ["sdo", "act", "--resource", resource, "--summary", summary, "--", *argv], timeout=300)


def _edit_source(root: Path, source: dict[str, Any], *, credentials: bool) -> None:
    """Record the restored source object in the isolated proposal, never accepted Git.

    An explicit annotation preserves the restoration even when the accepted spec
    was already correct. Credential repairs also persist the Secret references,
    rather than publishing the rotated credential or reverting to the old value.
    """
    metadata = source["metadata"]
    for path in sorted((root / "kubernetes").rglob("*.y*ml")):
        if "templates" in path.parts or path.is_symlink():
            continue
        text = path.read_text(encoding="utf-8")
        chunks = re.split(r"(?m)^---[ \t]*\n", text)
        for index, chunk in enumerate(chunks):
            # Do not deserialize Secret payloads in a multi-document manifest.
            if re.search(r"(?m)^kind:\s*Secret\s*(?:#.*)?$", chunk) or re.search(r'"kind"\s*:\s*"Secret"', chunk):
                continue
            try:
                document = yaml.safe_load(chunk)
            except yaml.YAMLError:
                continue
            if not isinstance(document, dict) or document.get("kind") != source["kind"]:
                continue
            if document.get("metadata", {}).get("name") != metadata["name"]:
                continue
            annotations = document["metadata"].setdefault("annotations", {})
            annotations["sdo.dev/source-restoration"] = "accepted-source"
            if credentials:
                _credential_env(document)
            chunks[index] = yaml.safe_dump(document, sort_keys=False)
            path.write_text("---\n".join(chunks), encoding="utf-8")
            return
    raise PlanError(f"proposal has no editable source for {source['kind']}/{metadata['name']}")


def _credential_env(source: dict[str, Any]) -> None:
    for container in source["spec"]["template"]["spec"]["containers"]:
        # Only the owning application container consumes this DB connection.
        if container["name"] != source["metadata"]["name"]:
            continue
        env = container.setdefault("env", [])
        env[:] = [item for item in env if item["name"] != "DB_CONNECTION_STRING"]
        env.append(
            {
                "name": "DB_CONNECTION_STRING",
                "valueFrom": {"secretKeyRef": {"name": "product-catalog-db-conn", "key": "DB_CONNECTION_STRING"}},
            }
        )


def _restore(run: Runner, step: Step, namespace: str, root: Path, directory: Path) -> dict[str, Any]:
    source = copy.deepcopy(_tool(run, "source", _ref(step, namespace))["source"])
    if source.get("kind") != step.kind or source.get("metadata", {}).get("name") != step.name:
        raise PlanError("accepted source returned the wrong resource")
    credentials = step.operation == "credentials"
    if credentials:
        _credential_env(source)
    _edit_source(root, source, credentials=credentials)
    if step.kind == "Service":
        live = json.loads(
            _run(run, ["kubectl", "-n", namespace, "get", "Service", step.name, "-o", "json"], timeout=60)
        )
        # Source owns the selector and ports; Kubernetes owns allocated identity.
        for key in ("clusterIP", "clusterIPs", "ipFamilies", "ipFamilyPolicy", "healthCheckNodePort"):
            if key not in source["spec"] and key in live.get("spec", {}):
                source["spec"][key] = live["spec"][key]
    field = "data" if step.kind == "ConfigMap" else "spec"
    path = directory / f"{step.kind}-{step.name}.json"
    # JSON merge/strategic merge leaves injected init containers, nodeName,
    # volumes and env value/valueFrom behind. Replace the entire accepted field.
    path.write_text(json.dumps([{"op": "add", "path": f"/{field}", "value": source[field]}]), encoding="utf-8")
    command = ["kubectl", "-n", namespace, "patch", step.kind, step.name, "--type=json", "--patch-file", str(path)]
    if step.kind == "ConfigMap":
        # A missing ConfigMap is a supported recorded repair, not a failed patch.
        code, _ = run(shlex.join(["kubectl", "-n", namespace, "get", step.kind, step.name, "-o", "name"]), timeout=60)
        if code:
            path.write_text(
                json.dumps(
                    {
                        "apiVersion": source["apiVersion"],
                        "kind": step.kind,
                        "metadata": {"name": step.name, "namespace": namespace},
                        "data": source["data"],
                    }
                ),
                encoding="utf-8",
            )
            command = ["kubectl", "-n", namespace, "create", "-f", str(path)]
    _act(run, _ref(step, namespace), f"Restore {step.kind}/{step.name} from accepted source", command)
    return source


def _converged(run: Runner, namespace: str) -> bool:
    output = _run(run, ["kubectl", "-n", namespace, "get", "deployments", "-o", "json"], timeout=60)
    for deployment in json.loads(output)["items"]:
        desired = deployment["spec"].get("replicas", 1)
        status = deployment.get("status", {})
        if (
            status.get("observedGeneration", 0) < deployment["metadata"]["generation"]
            or any(
                status.get(key, 0) != desired
                for key in ("replicas", "updatedReplicas", "readyReplicas", "availableReplicas")
            )
            or status.get("unavailableReplicas", 0)
        ):
            return False
    return True


def _wait_rollout(run: Runner, namespace: str) -> None:
    deadline = time.monotonic() + ROLLOUT_SECONDS
    for _ in range(ROLLOUT_SECONDS // POLL_SECONDS + 1):
        if _converged(run, namespace):
            return
        if time.monotonic() >= deadline:
            break
        time.sleep(POLL_SECONDS)
    raise PlanError("Deployment convergence exceeded 600 seconds")


def _special(run: Runner, step: Step, namespace: str, directory: Path, originals: dict[str, dict[str, Any]]) -> None:
    kubectl = ["kubectl", "-n", namespace]
    ref = _ref(step, namespace)
    if step.operation == "delete":
        scope = ["kubectl"] if step.kind.endswith("WebhookConfiguration") else kubectl
        _act(
            run,
            ref,
            f"Remove {step.kind}/{step.name}",
            [*scope, "delete", step.kind, step.name, "--ignore-not-found", "--wait=false"],
        )
    elif step.operation == "restart":
        _act(run, ref, f"Restart {step.name}", [*kubectl, "rollout", "restart", f"deployment/{step.name}"])
    elif step.operation in {"recreate", "rolling"}:
        if step.operation == "rolling":
            _wait_rollout(run, namespace)
        strategy = (
            {"type": "Recreate"}
            if step.operation == "recreate"
            else originals[step.name]["spec"].get("strategy", {"type": "RollingUpdate"})
        )
        patch = directory / "strategy.json"
        patch.write_text(json.dumps([{"op": "add", "path": "/spec/strategy", "value": strategy}]), encoding="utf-8")
        _act(
            run,
            ref,
            f"Restore {step.name} scheduling strategy"
            if step.operation == "rolling"
            else f"Temporarily recreate {step.name}",
            [*kubectl, "patch", "deployment", step.name, "--type=json", "--patch-file", str(patch)],
        )
    elif step.operation == "replace_pending":
        source = originals[step.name]
        selector = ",".join(f"{key}={value}" for key, value in source["spec"]["selector"]["matchLabels"].items())
        desired_affinity = source["spec"]["template"]["spec"].get("affinity", {})
        deadline = time.monotonic() + ROLLOUT_SECONDS
        for _ in range(ROLLOUT_SECONDS // POLL_SECONDS + 1):
            pods = json.loads(_run(run, [*kubectl, "get", "pods", "-l", selector, "-o", "json"], timeout=60))["items"]
            blocking = [
                pod
                for pod in pods
                if pod.get("spec", {})
                .get("affinity", {})
                .get("podAntiAffinity", {})
                .get("requiredDuringSchedulingIgnoredDuringExecution")
                and pod["spec"]["affinity"] != desired_affinity
            ]
            if not blocking:
                break
            if time.monotonic() >= deadline:
                raise PlanError("old anti-affinity Pod did not terminate within 600 seconds")
            time.sleep(POLL_SECONDS)
        else:
            raise PlanError("old anti-affinity Pod did not terminate within 600 seconds")
        for pod in pods:
            if pod.get("status", {}).get("phase") == "Pending":
                name = pod["metadata"]["name"]
                _act(
                    run,
                    f"Pod/{namespace}/{name}",
                    "Retry pending replacement scheduling",
                    [*kubectl, "delete", "pod", name, "--wait=false"],
                )
    elif step.operation == "kafka_skip":
        # The poison record is recognized from live consumer logs, not a hidden
        # injected offset or benchmark verdict. Leave topics and good data intact.
        output = _run(run, [*kubectl, "logs", "deployment/orders-validator", "--tail=100"], timeout=60)
        match = re.search(r"record validation failed at offset=(\d+)", output)
        if match is None:
            raise PlanError("consumer logs do not identify a poison record offset")
        _act(run, ref, "Stop paused consumer", [*kubectl, "scale", "deployment/orders-validator", "--replicas=0"])
        _run(
            run, [*kubectl, "wait", "--for=delete", "pod", "-l", "app=orders-validator", "--timeout=120s"], timeout=150
        )
        _act(
            run,
            f"Deployment/{namespace}/kafka",
            "Advance stopped consumer past invalid record",
            [
                *kubectl,
                "exec",
                "deployment/kafka",
                "--",
                "env",
                "KAFKA_HEAP_OPTS=-Xms32m -Xmx64m",
                "KAFKA_OPTS=",
                "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server",
                "localhost:9092",
                "--group",
                "orders-validator",
                "--topic",
                "orders-fulfillment:0",
                "--reset-offsets",
                "--to-offset",
                str(int(match.group(1)) + 1),
                "--execute",
            ],
        )
        _act(run, ref, "Resume consumer", [*kubectl, "scale", "deployment/orders-validator", "--replicas=1"])


def replay_recorded_repair(
    run: Runner, index: int, request: dict[str, Any], worktree: Path, *, attempts: int = 121
) -> dict[str, Any]:
    if type(index) is not int or not 0 <= index < len(SCRIPTED_REPLAY_SLOTS):
        raise ValueError("slot must be an integer from 0 through 9")
    if type(attempts) is not int or attempts < 1:
        raise ValueError("verification attempts must be positive")
    slot = SCRIPTED_REPLAY_SLOTS[index]
    namespace = request["namespace"]
    if namespace != slot.namespace:
        raise ValueError("replay namespace does not match the selected slot")
    started = utc_now()
    result: dict[str, Any] = {
        "incident_id": request["incident_id"],
        "status": "failed",
        "confirmed_root_causes": [],
        "applied_playbooks": [],
        "repair_actions": [],
        "repair_changes": [],
        "verification_evidence": [],
        "usage": {"llm_calls": 0, "input_tokens": 0, "output_tokens": 0},
    }
    originals: dict[str, dict[str, Any]] = {}
    try:
        with tempfile.TemporaryDirectory(prefix="sdo-replay-") as temporary:
            directory = Path(temporary)
            for step in slot.steps:
                if step.operation in {"restore", "credentials"}:
                    originals[step.name] = _restore(run, step, namespace, worktree, directory)
                else:
                    _special(run, step, namespace, directory, originals)
                result["repair_changes"].append(f"{step.operation} {step.kind}/{step.name}")
        _wait_rollout(run, namespace)
        deadline = time.monotonic() + ROLLOUT_SECONDS
        verification: dict[str, Any] = {}
        for attempt in range(attempts):
            verification = _tool(run, "verify")
            if verification.get("healthy"):
                result["status"] = "completed"
                break
            if time.monotonic() >= deadline or attempt + 1 == attempts:
                break
            time.sleep(POLL_SECONDS)
        result["verification_evidence"] = [
            {
                "name": "sdo verify",
                "passed": result["status"] == "completed",
                "details": verification.get("summary") or "independent verification",
                "observed_at": utc_now(),
            }
        ]
        if result["status"] == "completed":
            # Describe observed source restoration, never the hidden draw/oracle.
            for source in originals.values():
                ref = {
                    "api_version": source["apiVersion"],
                    "kind": source["kind"],
                    "namespace": namespace,
                    "name": source["metadata"]["name"],
                }
                result["confirmed_root_causes"].append(
                    {
                        "summary": f"Restored {ref['kind']}/{ref['name']} to accepted configuration",
                        "resources": [ref],
                        "evidence": [
                            {
                                "kind": "live-observation",
                                "source": f"{ref['kind']}/{ref['name']}",
                                "observation": "Accepted source restored; independent verification passed",
                            }
                        ],
                        "explained_detectors": [
                            finding["detector_id"]
                            for finding in request.get("findings", [])
                            if finding.get("detector_id")
                        ],
                    }
                )
    except (PlanError, KeyError, ValueError) as exc:
        result["repair_changes"].append(f"scripted plan stopped: {exc}")
    result["timing"] = {"started_at": started, "completed_at": utc_now()}
    return result
