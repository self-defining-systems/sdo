#!/usr/bin/env python3
"""Run an independent CI check across action steps without losing its result."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def write_result(path: Path, payload: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload))
    temporary.replace(path)


def ticks(pid: int) -> int:
    return int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19])


def start(state: Path, command: list[str]) -> None:
    if not command:
        raise ValueError("A background check requires a command")
    state.mkdir(parents=True, mode=0o700)
    write_result(state / "command.json", {"command": command})
    with (state / "output.log").open("w") as output:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "worker", str(state)],
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    deadline = time.monotonic() + 10
    while not (state / "process.json").exists():
        if process.poll() is not None or time.monotonic() > deadline:
            raise RuntimeError("Background check did not start; see " + str(state / "output.log"))
        time.sleep(0.02)


def worker(state: Path) -> None:
    write_result(state / "process.json", {"pid": os.getpid(), "ticks": ticks(os.getpid()), "started": time.monotonic()})

    def cancelled(signum: int, _frame: object) -> None:
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, cancelled)
    process = None
    status = 1
    try:
        command = json.loads((state / "command.json").read_text())["command"]
        process = subprocess.Popen(command, start_new_session=True)
        status = process.wait(timeout=420)
    except SystemExit as error:
        status = int(error.code or 1)
    finally:
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            except ProcessLookupError:
                pass
        write_result(state / "result.json", {"exit_code": status})


def finish(state: Path) -> int:
    record = json.loads((state / "process.json").read_text())
    try:
        while not (state / "result.json").exists():
            try:
                alive = ticks(record["pid"]) == record["ticks"]
            except FileNotFoundError:
                alive = False
            if not alive:
                raise RuntimeError("Background check exited without a result")
            if time.monotonic() - record["started"] > 435:
                os.kill(record["pid"], signal.SIGTERM)
                raise TimeoutError("Background check exceeded its seven-minute execution bound")
            time.sleep(0.1)
        return int(json.loads((state / "result.json").read_text())["exit_code"])
    finally:
        print((state / "output.log").read_text(), end="", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["start", "worker", "finish"])
    parser.add_argument("state", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.mode == "start":
        start(args.state, args.command)
    elif args.mode == "worker":
        worker(args.state)
    else:
        raise SystemExit(finish(args.state))


if __name__ == "__main__":
    main()
