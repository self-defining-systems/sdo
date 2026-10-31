"""Execute actions and emit every command through the real Codex wire parser."""

import base64
import os
import shlex
import shutil
import subprocess
import time
from pathlib import Path

from benchmarks.sregym.agent_script.wire import EventStream

from .model import Command, Delete, Exit, ScriptTurn, Tool, Wait, Write


class ScriptExit(Exception):
    """A planned CLI failure; the selected turn has already been consumed."""

    def __init__(self, code: int) -> None:
        self.code = code
        super().__init__(f"scripted exit {code}")


def execute(turn: ScriptTurn, root: Path, stream: EventStream, *, access: str = "danger-full-access") -> None:
    root = root.resolve()
    for step in turn.steps:
        if isinstance(step, Exit):
            raise ScriptExit(step.code)
        if isinstance(step, Wait):
            time.sleep(step.seconds)
            continue
        if isinstance(step, (Write, Delete)):
            target = root / step.path
            if any(path.is_symlink() for path in (target, *target.parents) if path != root and root in path.parents):
                raise ValueError(f"file step traverses symlink: {step.path}")
            quoted = shlex.quote(step.path)
            if isinstance(step, Delete):
                command = f"rm -f -- {quoted}"
            else:
                encoded = base64.b64encode(step.text.encode()).decode()
                command = (
                    f"mkdir -p -- {shlex.quote(str(Path(step.path).parent))} && "
                    f"printf %s {shlex.quote(encoded)} | base64 -d > {quoted} && "
                    f"chmod {'755' if step.executable else '644'} -- {quoted}"
                )
            action = Command(kind="command", command=command)
        elif isinstance(step, Tool):
            action = Command(
                kind="command", command=shlex.join(step.argv), exit_code=step.exit_code, contains=step.contains
            )
        else:
            action = step
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
            tools = Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools"))
            if tools.is_dir():
                argv[argv.index("--") : argv.index("--")] = ["--bind", str(tools.resolve()), str(tools.resolve())]
            # Like real workspace-write turns, the complete memory validator runs
            # outside the shell sandbox. A typed tool action cannot append shell syntax.
            if isinstance(step, Tool) and step.argv[:3] == ["sdo-operator", "memory", "check"]:
                argv = step.argv
            try:
                result = subprocess.run(
                    argv, cwd=root, capture_output=True, text=True, timeout=action.timeout, check=False
                )
                code, output = result.returncode, result.stdout + result.stderr
            except subprocess.TimeoutExpired:
                code, output = 124, "script command timed out"
            stream.command_completed(item, action.command, output, code)
            if code == action.exit_code and (action.contains is None or action.contains in output):
                break
            if attempt + 1 == action.attempts:
                raise ValueError(f"script assertion failed: {action.command}: exit {code}, output {output[:2000]}")
            time.sleep(action.interval)
