"""The scripted program loop: diagnose with installed programs, apply their plans, record repairs with ``sdo act``.

The responder works only through SDO's own tools. When an installed program matches, it plans and applies that
program; otherwise it restores the Service's ports from accepted source with one ``sdo act``. Reflection authors the
program from the controller's captured open and fixed snapshots, and only confirms (``no_change``) when the
controller dispatched a refine-depth task.

Stdlib-only: this runs inside the controller and responder images.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import shutil
import socket
from pathlib import Path
from typing import Any

from .faults import PlanError, _finding_evidence, _object_ref, fired_detectors, state_changes
from .responder import _poll_status, _verification
from .turn import Turn, utc_now

PROGRAM_FAULT = "service_target_port"
PROGRAM_ID = "service-targetport-mismatch"
PROGRAM_SOURCE = Path(os.environ.get("SDO_SCRIPTED_PROGRAM_SOURCE", "/opt/sdo-scripted/targetport"))
REFINE_MARKER = "This is a confirm-or-refine reflection"
REQUEST_PATH = Path("/sdo/request/agent-task.json")
CAPTURE_DIR = Path(os.environ.get("SDO_CAPTURED_CORPUS_DIR", "/sdo/session/corpus"))


def _tool(turn: Turn, *args: str) -> dict[str, Any]:
    code, output = turn.run(" ".join(["sdo", *(shlex.quote(arg) for arg in args)]), timeout=240)
    first = output.splitlines()[0] if output else ""
    try:
        response = json.loads(first)
    except json.JSONDecodeError as exc:
        raise PlanError(f"sdo {args[0]} returned no JSON (exit {code}): {output[:300]!r}") from exc
    if not isinstance(response, dict) or response.get("error"):
        raise PlanError(
            f"sdo {args[0]} failed: {response.get('error') if isinstance(response, dict) else output[:300]}"
        )
    return response


def _binding_args(bindings: dict[str, Any]) -> list[str]:
    args: list[str] = []
    for name, ref in sorted(bindings.items()):
        args.extend(["--bind", f"{name}={ref['kind']}/{ref['namespace']}/{ref['name']}"])
    return args


def _faulted(request: dict[str, Any]) -> tuple[str, str]:
    """The Service, else the Deployment, the request's state changes or findings point at."""

    changes = request.get("state_changes")
    named = [
        (str(item["kind"]), str(item["name"]))
        for item in (changes.get("changes") if isinstance(changes, dict) else None) or []
        if isinstance(item, dict)
    ] + [
        (str(finding["primary_resource"]["kind"]), str(finding["primary_resource"]["name"]))
        for finding in request.get("findings") or []
        if isinstance(finding.get("primary_resource"), dict)
    ]
    for kind in ("Service", "Deployment"):
        for found_kind, name in named:
            if found_kind == kind:
                return kind, name
    raise PlanError("the incident request names no Service or Deployment to repair")


def _baseline_images(request: dict[str, Any], deployment: str) -> dict[str, str]:
    """Container name -> the image the healthy baseline ran, from the request's state changes."""

    images: dict[str, str] = {}
    for item in (request.get("state_changes") or {}).get("changes") or []:
        if item.get("kind") == "Deployment" and item.get("name") == deployment:
            for field in item.get("fields") or []:
                name = str(field.get("field", ""))
                if name.startswith("container[") and name.endswith("].image") and field.get("before"):
                    images[name[len("container[") : -len("].image")]] = str(field["before"])
    return images


_TARGET_PORT = re.compile(r"spec\.ports\[(\d+)\]\.targetPort")


def _label_drift(turn: Turn, request: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    """The event with a ``bug`` label when accepted source and a refused TCP probe both contradict the live port."""

    label = {**event, "investigation_verdict": "unsure", "investigation_reason": "No supported live relation evidence"}
    ref = event.get("ref") or {}
    fields = event.get("fields") or []
    match = _TARGET_PORT.fullmatch(str(fields[0].get("path", ""))) if len(fields) == 1 else None
    if ref.get("kind") != "Service" or match is None:
        return label
    namespace, name, index = str(request["namespace"]), str(ref["name"]), int(match.group(1))
    source_port = _tool(turn, "source", f"Service/{namespace}/{name}")["source"]["spec"]["ports"][index].get(
        "targetPort"
    )
    _, live_json = turn.run(f"kubectl -n {namespace} get service {name} -o json")
    live = json.loads(live_json)
    live_port = live["spec"]["ports"][index].get("targetPort")
    _, pods_json = turn.run(f"kubectl -n {namespace} get pods -o json")
    selector = live["spec"].get("selector") or {}
    selected = [
        pod
        for pod in json.loads(pods_json).get("items", [])
        if selector and all(pod["metadata"].get("labels", {}).get(key) == value for key, value in selector.items())
    ]
    if not selected or source_port == live_port or str(live_port) != str(fields[0].get("new")):
        return label
    if not isinstance(source_port, int) or not isinstance(live_port, int):
        return label
    for pod in selected:
        address = pod["status"]["podIP"]
        with socket.create_connection((address, source_port), timeout=2):
            pass
        try:
            with socket.create_connection((address, live_port), timeout=2):
                return label
        except ConnectionRefusedError:
            pass
    label["investigation_verdict"] = "bug"
    label["investigation_reason"] = "Service contradicts accepted source and targets an observed non-listening port"
    label["investigation_evidence"] = [
        {"kind": "source_contradiction", "observation": f"source {source_port}, live {live_port}"},
        {
            "kind": "violated_relation",
            "observation": (
                f"all {len(selected)} selected Pod IPs accept TCP on {source_port} and refuse TCP on {live_port}"
            ),
        },
    ]
    return label


def respond_program(turn: Turn, request: dict[str, Any], worktree: Path) -> dict[str, Any]:
    del worktree
    started = utc_now()
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
    try:
        drift_findings = {
            (finding["primary_resource"]["kind"], finding["primary_resource"]["name"])
            for finding in request.get("findings") or []
            if str(finding.get("detector_id", "")).startswith("drift.") and finding.get("primary_resource")
        }
        labels = [
            _label_drift(turn, request, event)
            for event in request.get("drift_events") or []
            if (event["ref"]["kind"], event["ref"]["name"]) in drift_findings
        ]
        result["drift_events"] = labels
        kind, service = _faulted(request)
        diagnosis = _tool(turn, "diagnose")
        plan_ids: list[str] = []
        programs: list[str] = []
        for run in diagnosis.get("program_runs") or []:
            if run.get("verdict", {}).get("result") != "Match":
                continue
            planned = _tool(turn, "plan", run["program_id"], *_binding_args(run.get("bindings") or {}))
            if planned.get("plan_id"):
                plan_ids.append(planned["plan_id"])
                programs.append(run["program_id"])
        if plan_ids:
            _tool(turn, "apply", *plan_ids)
        elif kind == "Deployment":
            images = _baseline_images(request, service)
            if not images:
                raise PlanError(f"no baseline image recorded for Deployment/{service}")
            assignments = " ".join(f"{name}={shlex.quote(image)}" for name, image in sorted(images.items()))
            code, output = turn.run(
                f"sdo act --resource Deployment/{namespace}/{service} "
                f"--summary {shlex.quote('restore the baseline image')}"
                f" -- kubectl -n {namespace} set image deployment/{service} {assignments}"
            )
            if code:
                raise PlanError(f"sdo act failed ({code}): {output[:300]}")
            turn.run(f"kubectl -n {namespace} rollout status deployment/{service} --timeout=120s", timeout=130)
        else:
            source = _tool(turn, "source", f"Service/{namespace}/{service}")["source"]
            patch = json.dumps({"spec": {"ports": source["spec"]["ports"]}}, separators=(",", ":"))
            code, output = turn.run(
                f"sdo act --resource Service/{namespace}/{service} "
                f"--summary {shlex.quote('restore ports from accepted source')}"
                f" -- kubectl -n {namespace} patch service {service} --type=strategic -p {shlex.quote(patch)}"
            )
            if code:
                raise PlanError(f"sdo act failed ({code}): {output[:300]}")
        _tool(turn, "verify")
        healthy, status_output = _poll_status(turn, namespace, 24)
    except PlanError as exc:
        result["repair_changes"] = [f"scripted plan stopped: {exc}"]
        result["timing"] = {"started_at": started, "completed_at": utc_now()}
        return result
    result["verification_evidence"].append(_verification(healthy, status_output))
    if healthy:
        evidence = _finding_evidence(request)
        if f"{kind}/{service}" in state_changes(request):
            evidence.append(
                {
                    "kind": "state-change",
                    "source": f"{kind}/{service}",
                    "observation": "modified since the last healthy state",
                }
            )
        result["status"] = "completed"
        result["applied_playbooks"] = [{"playbook_id": program} for program in programs]
        result["confirmed_root_causes"] = [
            {
                "summary": f"{kind} {service} deviates from the healthy baseline and breaks the application",
                "resources": [_object_ref("apps/v1" if kind == "Deployment" else "v1", kind, namespace, service)],
                "evidence": evidence,
                "explained_detectors": fired_detectors(request),
                "static_context": [],
            }
        ]
    result["timing"] = {"started_at": started, "completed_at": utc_now()}
    return result


def _no_change(reason: str) -> dict[str, Any]:
    return {"summary": reason, "learning_decision": "no_change", "no_change_reason": reason, "playbook_ids": []}


def _reflection_namespace() -> str:
    path = Path(os.environ.get("SDO_REQUEST_PATH", str(REQUEST_PATH)))
    task = json.loads(path.read_text(encoding="utf-8"))
    return str(task["request"]["closure"]["request"]["namespace"])


def _captured_pair(capture: Path) -> tuple[str, str]:
    opened = json.loads((capture / "open@pre-apply.json").read_text(encoding="utf-8"))
    fixed = json.loads((capture / "fixed.json").read_text(encoding="utf-8"))
    after = {item["metadata"]["name"]: item for item in fixed.get("services", [])}
    for item in opened.get("services", []):
        name = item["metadata"]["name"]
        if name in after and item["spec"].get("ports") != after[name]["spec"].get("ports"):
            peers = sorted(peer for peer in after if peer != name)
            if peers:
                return name, peers[0]
    raise PlanError("the captured incident has no repaired Service and healthy peer")


def reflect_program(turn: Turn, prompt: str, worktree: Path, incident_id: str) -> dict[str, Any]:
    if REFINE_MARKER in prompt:
        turn.run("git status --short")
        return _no_change(f"installed program {PROGRAM_ID} handled every confirmed fault")
    if (worktree / ".sdo/playbooks" / PROGRAM_ID / "diagnose.go").is_file():
        turn.run("git status --short")
        return _no_change(f"memory already encodes program {PROGRAM_ID}")
    namespace = _reflection_namespace()
    try:
        repaired, peer = _captured_pair(CAPTURE_DIR)
    except (PlanError, OSError, KeyError):
        turn.run("git status --short")
        return _no_change(f"no captured Service repair to compile into {PROGRAM_ID}")
    target = worktree / ".sdo/playbooks" / PROGRAM_ID
    code, output = turn.run(
        f"sdo-operator program scaffold --fault-class {PROGRAM_ID} --param service=Service --mitigate "
        f"--out {shlex.quote(str(target))}"
    )
    if code:
        raise PlanError(f"program scaffold failed ({code}): {output[:300]}")
    for name in ("README.md", "diagnose.go", "mitigate.go"):
        shutil.copyfile(PROGRAM_SOURCE / name, target / name)
    body = (target / "README.md").read_text(encoding="utf-8")
    if body.startswith("---\n"):
        body = body.split("\n---\n", 1)[1]
    (target / "README.md").write_text(
        f"---\nschema_version: 1\nowner: responder\nfault_class: {PROGRAM_ID}\n"
        f"originating_incident: {incident_id}\n---\n{body}",
        encoding="utf-8",
    )
    (target / "diagnose_test.go").unlink(missing_ok=True)
    case = worktree / ".sdo/corpus" / f"{PROGRAM_ID}-{hashlib.sha256(incident_id.encode()).hexdigest()[:8]}"
    case.mkdir(parents=True)
    opened = (CAPTURE_DIR / "open@pre-apply.json").read_bytes()
    fixed = (CAPTURE_DIR / "fixed.json").read_bytes()
    for name, data in {
        "open.json": opened,
        "fixed.json": fixed,
        "healthy.json": fixed,
        "recordings.json": (CAPTURE_DIR / "open@pre-apply-recordings.json").read_bytes(),
    }.items():
        (case / name).write_bytes(data)

    def ref(name: str) -> dict[str, str]:
        return _object_ref("v1", "Service", namespace, name)

    (case / "labels.json").write_text(
        json.dumps(
            {
                "root_cause_program_ids": [PROGRAM_ID],
                "bindings": {"service": ref(repaired)},
                "peer_bindings": {"service": ref(peer)},
                "provenance": {
                    "incident_id": incident_id,
                    "open_at": "pre-apply",
                    "open_sha256": hashlib.sha256(opened).hexdigest(),
                    "fixed_sha256": hashlib.sha256(fixed).hexdigest(),
                },
            }
        ),
        encoding="utf-8",
    )
    books = target.parent
    (books / "README.md").write_text(
        "# Operational playbooks\n\n"
        + "".join(f"- [{item.name}]({item.name}/README.md)\n" for item in sorted(books.iterdir()) if item.is_dir()),
        encoding="utf-8",
    )
    decision = worktree.parent / f"scripted-decision-{os.getpid()}.json"
    decision.write_text(json.dumps({"learning_decision": "updated", "playbook_ids": [PROGRAM_ID]}), encoding="utf-8")
    try:
        code, output = turn.run(
            f"sdo-operator memory check --root {shlex.quote(str(worktree))} --decision {shlex.quote(str(decision))}",
            timeout=180,
        )
    finally:
        decision.unlink(missing_ok=True)
    if code:
        raise PlanError(f"scripted memory check failed ({code}): {output[:500]}")
    return {
        "summary": f"Compiled {PROGRAM_ID} from the captured repair of Service {repaired}",
        "learning_decision": "updated",
        "no_change_reason": None,
        "playbook_ids": [PROGRAM_ID],
    }
