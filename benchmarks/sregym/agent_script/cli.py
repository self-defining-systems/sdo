"""Codex-compatible CLI and worker launcher for hand-authored scripts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import yaml

from benchmarks.sregym.agent_script.wire import EventStream

from .converter import convert_recording
from .invocation import Invocation
from .model import AgentScript, ScriptTurn
from .runner import ScriptExit, execute


def load(path: Path) -> AgentScript:
    return AgentScript.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))


def _role(schema: dict[str, Any]) -> str:
    properties = schema.get("properties", {})
    for key, role in (
        ("learning_decision", "reflector"),
        ("objective_digest", "health-judge"),
        ("architecture_summary_markdown", "architecture"),
        ("proposed", "deployer"),
        ("confirmed_root_causes", "responder"),
    ):
        if key in properties:
            return role
    raise ValueError("unsupported role output schema")


def expand(value: Any, values: dict[str, str]) -> Any:
    """Small literal substitutions; never evaluates code or executes a template."""
    if isinstance(value, str):

        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in values:
                raise ValueError(f"unavailable script placeholder: {name}")
            return values[name]

        return re.sub(r"\{\{([a-z0-9_.]+)\}\}", replace, value)
    if isinstance(value, list):
        return [expand(item, values) for item in value]
    if isinstance(value, dict):
        return {key: expand(item, values) for key, item in value.items()}
    return value


def codex(argv: list[str], *, stdin: str | None = None) -> int:
    if not argv or argv[0] in {"--help", "-h", "--version", "-V"}:
        print("codex-cli 0.0.0-agent-script (no model)")
        return 0
    invocation = Invocation(argv)
    prompt = invocation.prompt_argument
    if prompt is None:
        prompt = sys.stdin.read() if stdin is None else stdin
    schema_path = Path(argv[argv.index("--output-schema") + 1])
    role = _role(json.loads(schema_path.read_text(encoding="utf-8")))
    incident = os.environ.get("SDO_AGENT_SCRIPT_INCIDENT")
    task_path = Path(os.environ.get("SDO_TASK_PATH", "/sdo/request/agent-task.json"))
    request: dict[str, Any] = {}
    if task_path.is_file():
        request = json.loads(task_path.read_text(encoding="utf-8")).get("request", {})
        incident = request.get("incident_id", request.get("incidentId", incident))
    if incident is None and "Incident request:\n" in prompt:
        request = json.loads(prompt.split("Incident request:\n", 1)[1])
        incident = request.get("incident_id")
    plan_path = Path(os.environ["SDO_AGENT_SCRIPT"])
    plan = load(plan_path)
    state = Path(os.environ.get("SDO_AGENT_SCRIPT_STATE", "/sdo/session/agent-script-state"))
    state.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(json.dumps([str(plan_path.resolve()), str(Path.cwd()), role, incident]).encode()).hexdigest()
    counter = state / f"{key}.json"
    # Worker jobs serialize their own turns; state must be private to each worker.
    index = json.loads(counter.read_text()) if counter.exists() else 0
    turn = plan.select(role, incident, index)
    values = {"checkout": str(Path.cwd())}
    if incident is not None:
        values["incident_id"] = incident
    for name, value in request.items():
        if isinstance(value, (str, int, bool)):
            snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
            values[f"request.{snake}"] = str(value)
    objective = values.get("request.health_objective")
    if objective is not None:
        values["objective_digest"] = hashlib.sha256(objective.strip().encode()).hexdigest()
    turn = ScriptTurn.model_validate(expand(turn.model_dump(), values))
    session = invocation.resume or hashlib.sha256(f"{key}:{index}".encode()).hexdigest()[:32]
    stream = EventStream()
    stream.thread_started(session)
    stream.turn_started()
    access = "danger-full-access"
    for flag in ("--sandbox", "-s"):
        if flag in argv:
            access = argv[argv.index(flag) + 1]
    counter.write_text(json.dumps(index + 1), encoding="utf-8")
    try:
        execute(turn, Path.cwd(), stream, access=access)
    except ScriptExit as exc:
        return exc.code
    stream.agent_message(json.dumps(turn.result, sort_keys=True))
    stream.turn_completed({})
    return 0


def launch(command: list[str]) -> int:
    """Wrap a production worker. Script may be local or read once from a ConfigMap."""
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        raise ValueError("launch requires a production worker command")
    with tempfile.TemporaryDirectory(prefix="sdo-agent-script-") as temporary:
        directory = Path(temporary)
        env = dict(os.environ)
        if "SDO_AGENT_SCRIPT" not in env:
            namespace = env["SDO_NAMESPACE"]
            name = env.get("SDO_AGENT_SCRIPT_CONFIGMAP", "sdo-agent-script")
            result = subprocess.run(
                ["kubectl", "-n", namespace, "get", "configmap", name, "-o", "json"],
                check=True,
                capture_output=True,
                text=True,
            )
            path = directory / "script.json"
            path.write_text(json.loads(result.stdout)["data"]["script.json"], encoding="utf-8")
            env["SDO_AGENT_SCRIPT"] = str(path)
        load(Path(env["SDO_AGENT_SCRIPT"]))
        binary = directory / "codex"
        binary.write_text(
            f'#!/bin/sh\nexec {shlex.quote(sys.executable)} -m benchmarks.sregym.agent_script codex "$@"\n',
            encoding="utf-8",
        )
        binary.chmod(0o755)
        env["PATH"] = str(directory) + os.pathsep + env["PATH"]
        env.setdefault("SDO_AGENT_SCRIPT_STATE", str(directory / "state"))
        # Resolve role providers without requiring a real login or a model API.
        env["SDO_AGENT_PROVIDER"] = "codex"
        login = directory / "login"
        login.mkdir()
        (login / "auth.json").write_text('{"tokens":"agent-script-no-model"}')
        env["CODEX_HOME"] = str(login)
        return subprocess.run(command, env=env, check=False).returncode


def registrations(destination: Path, image: str) -> None:
    source = Path(__file__).resolve().parents[3] / "sdo/agents/registrations"
    destination.mkdir(parents=True, exist_ok=True)
    for role in ("deployer", "health-judge", "responder", "reflector"):
        value = json.loads((source / f"{role}.json").read_text())
        job = value["kubernetes_job"]
        job["image"] = image
        job["command"] = ["python3", "-m", "benchmarks.sregym.agent_script", "launch", "--", *job["command"]]
        (destination / f"{role}.json").write_text(json.dumps(value, indent=2) + "\n")


def main() -> int:
    try:
        args = sys.argv[1:]
        if args[:1] == ["codex"]:
            return codex(args[1:])
        if args[:1] == ["launch"]:
            return launch(args[1:])
        parser = argparse.ArgumentParser(description=__doc__)
        sub = parser.add_subparsers(dest="operation", required=True)
        convert = sub.add_parser("convert")
        convert.add_argument(
            "--role", required=True, choices=["deployer", "architecture", "health-judge", "responder", "reflector"]
        )
        convert.add_argument("--out", type=Path, required=True)
        convert.add_argument("--file-edits", type=Path)
        convert.add_argument("recordings", nargs="+", type=Path)
        validate = sub.add_parser("validate")
        validate.add_argument("script", type=Path)
        registry = sub.add_parser("registrations")
        registry.add_argument("--out", type=Path, required=True)
        registry.add_argument("--image", required=True)
        parsed = parser.parse_args(args)
        if parsed.operation == "convert":
            plan = convert_recording(parsed.recordings, role=parsed.role, file_edits=parsed.file_edits)
            parsed.out.write_text(plan.model_dump_json(indent=2) + "\n")
        elif parsed.operation == "validate":
            load(parsed.script)
        else:
            registrations(parsed.out, parsed.image)
        return 0
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as exc:
        print(f"agent script: {exc}", file=sys.stderr)
        return 1
