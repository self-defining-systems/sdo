"""Generate auditable agent-script steps for learning from captured repairs."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shlex
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from benchmarks.sregym.agent_script.model import AgentScript, Command, Delete, ScriptTurn, Tool, Write
from sdo.contracts import AgentTask, ReflectionRequest, parse_json

if TYPE_CHECKING:
    from sdo.contracts import IncidentClosure

PROGRAM_ID = "service-targetport-mismatch"
NETWORKPOLICY_PROGRAM_ID = "networkpolicy-service-isolation"
PROGRAM_SOURCE = Path(__file__).with_name("scripted_program")
NETWORKPOLICY_PROGRAM_SOURCE = Path(__file__).with_name("scripted_networkpolicy_program")


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


def reflection_script(
    *,
    worktree: Path,
    incident_id: str,
    closure: IncidentClosure,
    capture: Path,
    program_source: Path = PROGRAM_SOURCE,
    policy_program_source: Path = NETWORKPOLICY_PROGRAM_SOURCE,
) -> AgentScript:
    """Read a task and captures without editing or invoking a worker."""

    def write(path: Path, text: str, **_: Any) -> None:
        steps.append(Write(kind="write", path=str(path.relative_to(worktree)), text=text))

    def tool(argv: list[str], **_: Any) -> None:
        steps.append(Tool(kind="tool", argv=argv, timeout=300.0 if argv[1:3] == ["memory", "check"] else 180.0))

    def finish(**result: Any) -> AgentScript:
        result.setdefault("no_change_reason", None)
        result.setdefault("sdk_gaps", [])
        return AgentScript(
            version=1,
            turns=[
                ScriptTurn(
                    role="reflector",
                    incident=incident,
                    steps=steps,
                    result=result,
                )
            ],
        )

    steps: list[Any] = []
    incident = incident_id
    open_at = (
        "detect" if any(event.investigation_verdict == "bug" for event in closure.request.drift_events) else "pre-apply"
    )
    policy_pair = _captured_policy_pair(capture, open_at)
    program_id = NETWORKPOLICY_PROGRAM_ID if policy_pair else PROGRAM_ID
    program_source = policy_program_source if policy_pair else program_source
    parameter = "policy=NetworkPolicy" if policy_pair else "service=Service"
    if (worktree / ".sdo" / "playbooks" / program_id / "diagnose.go").is_file():
        return finish(
            summary=f"{program_id} program is already accepted",
            learning_decision="no_change",
            no_change_reason="Accepted program covers this class",
            playbook_ids=[],
        )
    pair = policy_pair or _captured_pair(capture, open_at)
    if pair is None:
        return finish(
            summary="No captured target port repair",
            learning_decision="no_change",
            no_change_reason="Captured incident has no target port transition and healthy peer",
            playbook_ids=[],
        )
    target = worktree / ".sdo/playbooks" / program_id

    tool(
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
        write(target / name, (program_source / name).read_text(encoding="utf-8"))
    body = (program_source / "README.md").read_text(encoding="utf-8")
    if body.startswith("---\n"):
        body = body.split("\n---\n", 1)[1]
    write(
        target / "README.md",
        "---\n"
        + yaml.safe_dump(
            {
                "schema_version": 1,
                "owner": "responder",
                "fault_class": program_id,
                "originating_incident": incident,
            },
            sort_keys=False,
        )
        + "---\n"
        + body,
        encoding="utf-8",
    )
    steps.append(Delete(kind="delete", path=str((target / "diagnose_test.go").relative_to(worktree))))
    case = worktree / ".sdo/corpus" / (program_id + "-" + hashlib.sha256(incident.encode()).hexdigest()[:8])

    if case.exists():
        raise ValueError(f"captured corpus already exists: {case}")
    opened = (capture / f"open@{open_at}.json").read_bytes()
    fixed = (capture / "fixed.json").read_bytes()
    for name, data in {
        "open.json": opened,
        "fixed.json": fixed,
        "healthy.json": fixed,
        "recordings.json": (capture / f"open@{open_at}-recordings.json").read_bytes(),
    }.items():
        write(case / name, data.decode("utf-8"))
    binding_name, kind = parameter.split("=", 1)

    def binding(name: str) -> dict[str, str]:
        return {
            "api_version": "networking.k8s.io/v1" if kind == "NetworkPolicy" else "v1",
            "kind": kind,
            "namespace": closure.request.namespace,
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
    write(case / "labels.json", json.dumps(labels), encoding="utf-8")
    books = target.parent
    write(
        books / "README.md",
        "# Operational playbooks\n\n"
        + "".join(
            f"- [{directory.name}]({directory.name}/README.md)\n"
            for directory in sorted(set(books.iterdir() if books.exists() else []) | {target})
            if directory == target or directory.is_dir()
        ),
        encoding="utf-8",
    )
    # The validator checks every checkout change, so the decision belongs in
    # the worker's writable tool exchange, outside repository-owned artifacts.
    tools = Path(os.environ.get("SDO_AGENT_SCRIPT_WORK_DIR", os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools")))
    decision = tools / ("reflection-decision-" + hashlib.sha256(incident.encode()).hexdigest() + ".json")
    encoded = base64.b64encode(
        json.dumps({"learning_decision": "updated", "playbook_ids": [program_id]}).encode()
    ).decode()
    steps.append(
        Command(
            kind="command",
            command=f"set -C; printf %s {shlex.quote(encoded)} | base64 -d > {shlex.quote(str(decision))}",
        )
    )
    tool(["sdo-operator", "memory", "check", "--root", str(worktree), "--decision", str(decision)])
    steps.append(Command(kind="command", command=f"rm -f -- {shlex.quote(str(decision))}"))
    return finish(
        summary=f"Compiled {program_id} program from captured repair",
        learning_decision="updated",
        playbook_ids=[program_id],
    )


def main() -> None:
    """Emit a local script; execution remains the normal agent-script launcher."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", type=Path, required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    task = parse_json(args.task.read_text(encoding="utf-8"), AgentTask())
    request = ReflectionRequest()
    if not task.request.Unpack(request):
        parser.error("task must carry a ReflectionRequest")
    script = reflection_script(
        worktree=Path(task.workspace.checkout_path),
        incident_id=request.outcome.incident_id,
        closure=request.closure,
        capture=args.capture,
    )
    args.out.write_text(script.model_dump_json(indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
