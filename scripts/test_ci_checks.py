#!/usr/bin/env python3
"""Overlap complete Go and Python contracts with bounded compiler concurrency."""

from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import FrameType


@dataclass(frozen=True)
class Lane:
    name: str
    command: tuple[str, ...]
    environment: dict[str, str] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("A check lane needs a nonempty name")
        if (
            not isinstance(self.command, tuple)
            or not self.command
            or not all(isinstance(arg, str) for arg in self.command)
        ):
            raise ValueError("A check lane needs a tuple of command arguments")
        if self.environment is not None and not isinstance(self.environment, dict):
            raise TypeError("Check lane environment must be a dictionary")
        if self.environment is not None and not all(
            isinstance(key, str) and isinstance(value, str) for key, value in self.environment.items()
        ):
            raise TypeError("Check lane environment names and values must be strings")


def run_lanes(lanes: list[Lane], directory: Path) -> int:
    """Wait for every lane, preserve failures, and cancel owned process groups."""
    if not lanes or len({lane.name for lane in lanes}) != len(lanes):
        raise ValueError("Check lanes must have unique names and cannot be empty")
    processes: list[tuple[Lane, subprocess.Popen, float]] = []

    def cancelled(signum: int, _frame: FrameType | None) -> None:
        raise SystemExit(128 + signum)

    previous = signal.signal(signal.SIGTERM, cancelled)
    try:
        for lane in lanes:
            print(f"Starting {lane.name} checks", flush=True)
            process = subprocess.Popen(lane.command, cwd=directory, env=lane.environment, start_new_session=True)
            processes.append((lane, process, time.monotonic()))
        statuses: dict[int, int] = {}
        while len(statuses) < len(processes):
            for index, (lane, process, started) in enumerate(processes):
                if index in statuses:
                    continue
                status = process.poll()
                if status is not None:
                    statuses[index] = status
                    print(f"{lane.name} checks: exit={status}, elapsed={time.monotonic() - started:.2f}s", flush=True)
            if len(statuses) < len(processes):
                time.sleep(0.05)
        return next((statuses[index] for index in range(len(processes)) if statuses[index]), 0)
    finally:
        for _, process, _ in processes:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        for _, process, _ in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        signal.signal(signal.SIGTERM, previous)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    # The Go script has two lanes. Python uses one worker and one compiler so
    # native CLI/fixture builds can overlap without exhausting the runner.
    python_environment = {**os.environ, "GOMAXPROCS": "1", "GOFLAGS": "-p=1", "SDO_PYTEST_WORKERS": "1"}
    raise SystemExit(
        run_lanes(
            [
                Lane("Go modules", ("bash", "scripts/test_go_ci.sh")),
                Lane(
                    "Python",
                    (
                        "bash",
                        "-e",
                        "-c",
                        "uv run python scripts/operator_test_binary.py\nbash scripts/test_python.sh",
                    ),
                    python_environment,
                ),
            ],
            root,
        )
    )


if __name__ == "__main__":
    main()
