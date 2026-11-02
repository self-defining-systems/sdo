"""Execute actions and emit every command through the real Codex wire parser."""

import base64
import json
import os
import re
import shlex
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from benchmarks.sregym.agent_script.wire import EventStream

from .model import Command, Delete, Exit, ScriptTurn, Step, Tool, Wait, Write
from .template import TemplateContext, expand

#: Where the production `sdo` tool persists complete replies (sdo.agents.responder.tool_output.OUTPUT_ROOT).
OUTPUT_ROOT = Path("/tmp/sdo")


def _shared_directories() -> list[Path]:
    """Writable directories that outlive one command: tool output and the worker's session scratch."""
    directories = []
    scratch = os.environ.get("SDO_SESSION_SCRATCH")
    for directory in (OUTPUT_ROOT, Path(scratch) if scratch else None):
        if directory is None:
            continue
        try:
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError:
            continue
        directories.append(directory.resolve())
    return directories


class ScriptExit(Exception):
    """A planned CLI failure; the selected turn has already been consumed."""

    def __init__(self, code: int) -> None:
        self.code = code
        super().__init__(f"scripted exit {code}")


def execute(
    turn: ScriptTurn,
    root: Path,
    stream: EventStream,
    *,
    access: str = "danger-full-access",
    context: TemplateContext | None = None,
) -> dict[str, Any]:
    root = root.resolve()
    context = context or TemplateContext({}, root)
    # Check task-dependent references before side effects. Captures are validated
    # structurally at load time; their real clock values become available below.
    probe = dict(context.values)
    probe["now"] = context.values["turn.started_at"]
    for index in range(len(turn.steps)):
        for field in ("started_at", "completed_at"):
            probe[f"steps.{index}.{field}"] = probe["now"]
    # Live output references are checked when consumed, after their producer ran.
    for name in re.findall(r"\{\{(steps\.[0-9]+\.output(?:\.[a-zA-Z0-9_]+)*)\}\}", str(turn.model_dump())):
        probe[name] = None
    expand(turn.model_dump(), probe)
    adapter = TypeAdapter(Step)
    for index, raw_step in enumerate(turn.steps):
        started = context.clock()
        context.values[f"steps.{index}.started_at"] = started
        step = adapter.validate_python(context.expand(raw_step.model_dump(), now=started))
        if isinstance(step, Exit):
            raise ScriptExit(step.code)
        if isinstance(step, Wait):
            time.sleep(step.seconds)
            context.values[f"steps.{index}.completed_at"] = context.clock()
            continue
        stdin: str | None = None
        if isinstance(step, (Write, Delete)):
            target = root / step.path
            if any(path.is_symlink() for path in (target, *target.parents) if path != root and root in path.parents):
                raise ValueError(f"file step traverses symlink: {step.path}")
            quoted = shlex.quote(step.path)
            if isinstance(step, Delete):
                command = f"rm -f -- {quoted}"
            else:
                # The contents travel on stdin: one argument is capped near 128 KiB, and a captured corpus is larger.
                stdin = base64.b64encode(step.text.encode()).decode()
                command = (
                    f"mkdir -p -- {shlex.quote(str(Path(step.path).parent))} && "
                    f"base64 -d > {quoted} && "
                    f"chmod {'755' if step.executable else '644'} -- {quoted}"
                )
            action = Command(kind="command", command=command)
        elif isinstance(step, Tool):
            action = Command(
                kind="command",
                command=shlex.join(step.argv),
                exit_code=step.exit_code,
                contains=step.contains,
                timeout=step.timeout,
            )
        else:
            action = step
        stdout = ""
        for attempt in range(action.attempts):
            item = stream.command_started(action.command)
            # Read-only host with only the checkout writable. No model executable is invoked.
            # Keep networking for kubectl and the responder's real tool transport.
            argv = [
                "bwrap",
                "--ro-bind",
                "/",
                "/",
                "--ro-bind" if access == "read-only" else "--bind",
                str(root),
                str(root),
                "--dev",
                "/dev",
                "--proc",
                "/proc",
                "--",
                "/bin/sh",
                "-c",
                action.command,
            ]
            if access == "workspace-write":
                go_binary = shutil.which("go")
                if go_binary:
                    position = argv.index("--")
                    argv[position:position] = ["--ro-bind", "/bin/false", str(Path(go_binary).resolve())]
            # Like a real workspace-write turn, `sdo` persists each complete tool reply and its context under
            # /tmp/sdo and the session scratch directory, and later commands read them back.
            for shared in _shared_directories():
                argv[argv.index("--") : argv.index("--")] = ["--bind", str(shared), str(shared)]
            tools = Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools"))
            if tools.is_dir():
                argv[argv.index("--") : argv.index("--")] = ["--bind", str(tools.resolve()), str(tools.resolve())]
            # Like real workspace-write turns, the complete memory validator runs
            # outside the shell sandbox. A typed tool action cannot append shell syntax.
            if isinstance(step, Tool) and step.argv[:3] == ["sdo-operator", "memory", "check"]:
                argv = step.argv
            try:
                result = subprocess.run(
                    argv,
                    cwd=root,
                    capture_output=True,
                    text=True,
                    timeout=action.timeout,
                    check=False,
                    input=stdin
                    if stdin is not None
                    else json.dumps(action.input_json)
                    if action.input_json is not None
                    else None,
                )
                code, output = result.returncode, result.stdout + result.stderr
                stdout = result.stdout
            except subprocess.TimeoutExpired:
                code, output = 124, "script command timed out"
                stdout = ""
            stream.command_completed(item, action.command, output, code)
            if code == action.exit_code and (action.contains is None or action.contains in output):
                break
            if attempt + 1 == action.attempts:
                raise ValueError(f"script assertion failed: {action.command}: exit {code}, output {output[:2000]}")
            time.sleep(action.interval)

        if action.capture_json:
            captured = json.loads(stdout)

            def flatten(prefix: str, value: Any) -> None:
                context.values[prefix] = value
                if isinstance(value, dict):
                    for key, item in value.items():
                        flatten(f"{prefix}.{key}", item)
                elif isinstance(value, list):
                    for offset, item in enumerate(value):
                        flatten(f"{prefix}.{offset}", item)

            flatten(f"steps.{index}.output", captured)
        context.values[f"steps.{index}.completed_at"] = context.clock()
    return context.expand(turn.result, now=context.clock())
