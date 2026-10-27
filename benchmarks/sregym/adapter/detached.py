"""Detached benchmark launches with a stable record shared by all users of a lane.

Record only explicit command arguments, never environments or helper contents.
Use the same --record path across worktrees and run directories for a lane.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from benchmarks.sregym.adapter.budgets import WaitKind, wait_for
from benchmarks.sregym.adapter.processes import ProcessRef, read_registry

if TYPE_CHECKING:
    from collections.abc import Iterator

    from benchmarks.sregym.adapter.persistent import AbortClusterOps


class LaunchRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: int = 1
    launcher: ProcessRef
    started_at: datetime
    state_path: Path
    command: list[str]
    stdout_path: Path
    stderr_path: Path


def _read(path: Path) -> LaunchRecord | None:
    return LaunchRecord.model_validate_json(path.read_text()) if path.exists() else None


@contextmanager
def _lane_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_name(path.name + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def _owns_registry(record: LaunchRecord) -> bool:
    return record.launcher in read_registry(record.state_path).processes


def status(path: Path) -> LaunchRecord | None:
    """Find the live run to adopt, including registered helpers during cleanup."""
    record = _read(path)
    if record is None:
        return None
    if record.launcher.alive() or (
        _owns_registry(record) and any(ref.alive() for ref in read_registry(record.state_path).processes)
    ):
        return record
    return None


def abort_record(path: Path, *, ops: AbortClusterOps) -> list[str]:
    """Quiesce the recorded run while excluding a competing lane launch."""
    from benchmarks.sregym.adapter.persistent import abort

    with _lane_lock(path):
        record = _read(path)
        if record is None or not _owns_registry(record):
            raise RuntimeError(f"no matching launcher identity in registry for {path}")
        return abort(record.state_path, ops=ops)


def launch(path: Path, run_dir: Path, command: list[str], *, helpers: Path | None = None) -> LaunchRecord:
    """Publish identity before releasing the detached lifetime owner's barrier."""
    path = path.absolute()
    run_dir = run_dir.absolute()
    with _lane_lock(path):
        if status(path) is not None:
            raise RuntimeError(f"live run already owns lane: {path}; use status to adopt or abort to stop it")
        run_dir.mkdir(parents=True, exist_ok=True)
        state = run_dir / "sdo_persistent_controller.json"
        if any(ref.alive() for ref in read_registry(state).processes):
            raise RuntimeError(f"run directory still has live processes: {run_dir}")
        owner_command = [
            sys.executable,
            "-m",
            "benchmarks.sregym.adapter.launcher",
            "--detached-owner",
            "--state",
            str(state),
        ]
        if helpers is not None:
            owner_command.extend(["--helpers", str(helpers)])
        owner_command.extend(["--", *command])
        stdout_path = run_dir / "launcher.stdout.log"
        stderr_path = run_dir / "launcher.stderr.log"
        with stdout_path.open("a") as stdout, stderr_path.open("a") as stderr:
            owner = subprocess.Popen(
                owner_command,
                stdin=subprocess.PIPE,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
        assert owner.stdin is not None
        ref = ProcessRef.capture(owner.pid, "launcher")
        try:
            record = LaunchRecord(
                launcher=ref,
                started_at=datetime.now(timezone.utc),
                state_path=state,
                command=command,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
            )
            temporary = path.with_name(path.name + ".tmp")
            temporary.write_text(record.model_dump_json(indent=2) + "\n")
            temporary.replace(path)
            owner.stdin.write(b"go\n")
            owner.stdin.close()
            deadline = time.monotonic() + wait_for(WaitKind.PROBE)
            while time.monotonic() < deadline:
                refs = read_registry(state).processes
                if ref in refs and any(item.name == "guardian" for item in refs):
                    return record
                if owner.poll() is not None:
                    break
                time.sleep(0.05)
            raise RuntimeError(f"detached owner did not become ready; inspect {stderr_path}")
        except BaseException:
            owner.stdin.close()
            if ref.alive():
                os.kill(ref.pid, signal.SIGTERM)
            try:
                owner.wait(timeout=wait_for(WaitKind.PROBE))
            except subprocess.TimeoutExpired:
                if ref.alive():
                    os.kill(ref.pid, signal.SIGKILL)
                owner.wait(timeout=wait_for(WaitKind.PROBE))
            raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    for action in ("launch", "status", "abort"):
        sub = subparsers.add_parser(action)
        sub.add_argument("--record", required=True, type=Path, help="stable shared lane record path")
        if action == "launch":
            sub.add_argument("--run-dir", required=True, type=Path)
            sub.add_argument("--helpers", type=Path)
            sub.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    try:
        if args.action == "launch":
            command = args.command[1:] if args.command[:1] == ["--"] else args.command
            if not command:
                parser.error("a command after -- is required")
            record = launch(args.record, args.run_dir, command, helpers=args.helpers)
        elif args.action == "status":
            record = status(args.record)
            if record is None:
                print(f"No live run: {args.record}", file=sys.stderr)
                return 1
        else:
            from benchmarks.sregym.adapter.persistent import KubectlClusterOps

            errors = abort_record(args.record, ops=KubectlClusterOps())
            for error in errors:
                print(error, file=sys.stderr)
            return int(bool(errors))
        print(record.model_dump_json(indent=2))
        return 0
    except (OSError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
