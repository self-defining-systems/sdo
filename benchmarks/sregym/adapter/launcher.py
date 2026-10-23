"""Lifetime owner for a benchmark launcher and its explicitly configured host helpers.

The small guardian has its own process group. It kills that group when the
launcher disappears, including SIGKILL. The registry stores identities only,
never command lines, helper configuration, environments, or credentials.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

from benchmarks.sregym.adapter.processes import ProcessRef, read_registry, register, terminate_run


class LauncherInterrupted(BaseException):
    def __init__(self, signum: int):
        self.signum = signum


def _guardian(state: Path) -> None:
    # The parent registers us before releasing this startup barrier.
    payload = json.load(sys.stdin)
    parent = next(ref for ref in read_registry(state).processes if ref.name == "launcher")
    invoker = ProcessRef.model_validate(payload["invoker"]) if payload["invoker"] else None

    def interrupted(signum, frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        for name, argv in payload["helpers"].items():
            helper = subprocess.Popen(argv, stdin=subprocess.DEVNULL)
            register(state, ProcessRef.capture(helper.pid, name))
        worker = subprocess.Popen(payload["command"], stdin=subprocess.DEVNULL)
        register(state, ProcessRef.capture(worker.pid, "worker"))
        while parent.alive() and (invoker is None or invoker.alive()):
            result = worker.poll()
            if result is not None:
                state.with_name(state.name + ".exit.json").write_text(json.dumps({"returncode": result}))
                return
            time.sleep(0.1)
    finally:
        # No helper can survive guardian exit, including grandchildren and a
        # helper created just before its identity was registered.
        os.killpg(os.getpgrp(), signal.SIGKILL)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--helpers", type=Path, help="JSON map of helper name to foreground argv; never persisted")
    parser.add_argument("--detached-owner", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--guard", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.guard:
        _guardian(args.state)
        return 0
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("a command after -- is required")
    selected = args.state or os.getenv("SDO_PERSISTENT_CONTROLLER_STATE")
    state = (
        Path(selected).resolve()
        if selected
        else (Path.cwd() / ".sdo-runs" / uuid.uuid4().hex / "sdo_persistent_controller.json")
    )
    helpers_path = args.helpers or os.getenv("SDO_RUN_HELPERS_FILE")
    helpers = json.loads(Path(helpers_path).read_text()) if helpers_path else {}
    if not isinstance(helpers, dict) or any(
        name in ("launcher", "guardian", "worker")
        or not isinstance(name, str)
        or not isinstance(cmd, list)
        or not cmd
        or any(not isinstance(arg, str) for arg in cmd)
        for name, cmd in helpers.items()
    ):
        parser.error("helpers must map distinct names to nonempty foreground argv lists")
    if args.detached_owner and sys.stdin.readline() != "go\n":
        raise RuntimeError("detached launch record was not published")
    if not args.detached_owner and os.getppid() == 1:
        raise RuntimeError("launcher invoker exited before lifetime tracking was established")
    register(state, ProcessRef.capture(os.getpid(), "launcher"), new_run=True)
    os.environ["SDO_RUN_REGISTRY_STATE"] = str(state)
    print(f"Abort this run: {sys.executable} -m benchmarks.sregym.adapter.persistent abort --state {state}", flush=True)

    def interrupted(signum, frame):
        raise LauncherInterrupted(signum)

    previous = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGTERM, signal.SIGINT)}
    exit_path = state.with_name(state.name + ".exit.json")
    exit_path.unlink(missing_ok=True)
    try:
        guardian = subprocess.Popen(
            [sys.executable, "-m", "benchmarks.sregym.adapter.launcher", "--guard", "--state", str(state)],
            stdin=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        register(state, ProcessRef.capture(guardian.pid, "guardian"))
        assert guardian.stdin is not None
        # uv keeps a wrapper PID exposed by exec-based shell launchers. Watch
        # that wrapper too, without binding detached Python launches to a shell.
        parent_pid = os.getppid()
        wrapper = not args.detached_owner and Path(f"/proc/{parent_pid}/exe").readlink().name == "uv"
        invoker = ProcessRef.capture(parent_pid, "invoker") if wrapper else None
        json.dump(
            {"command": command, "helpers": helpers, "invoker": invoker.model_dump() if invoker else None},
            guardian.stdin,
        )
        guardian.stdin.close()
        guardian.wait()
        result = json.loads(exit_path.read_text())["returncode"] if exit_path.exists() else 1
        if result:
            from benchmarks.sregym.adapter.persistent import KubectlClusterOps, abort

            errors = abort(state, ops=KubectlClusterOps())
            for error in errors:
                print(error, file=sys.stderr)
        return result
    except LauncherInterrupted as exc:
        # Disable re-entry while the trap performs bounded cleanup.
        for sig in previous:
            signal.signal(sig, signal.SIG_IGN)
        from benchmarks.sregym.adapter.persistent import KubectlClusterOps, abort

        errors = abort(state, ops=KubectlClusterOps())
        for error in errors:
            print(error, file=sys.stderr)
        return 128 + exc.signum
    finally:
        terminate_run(state)
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    sys.exit(main())
