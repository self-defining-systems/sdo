"""Streaming Go JSON diagnostics; Go and CI retain their existing deadlines."""

from __future__ import annotations
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path

OPERATOR = Path(__file__).resolve().parents[1] / "sdo/operator"
TEST_BUDGET_SECONDS = 180
SHARD_BUDGET_SECONDS = 210


def check_budgets(events: list[dict], *, elapsed: float, budget_seconds: float | None = None) -> None:
    shard_budget = SHARD_BUDGET_SECONDS if budget_seconds is None else budget_seconds
    completed: list[dict] = []
    starts: dict[tuple[str, str], float] = {}
    paused: dict[tuple[str, str], float] = {}
    pause_seconds: dict[tuple[str, str], float] = {}
    for event in events:
        if not event.get("Test") or "/" in event["Test"]:
            continue
        key = (event["Package"], event["Test"])
        at = datetime.fromisoformat(event["Time"].replace("Z", "+00:00")).timestamp() if event.get("Time") else None
        action = event.get("Action")
        if action == "run" and at is not None:
            starts[key] = at
        elif action == "pause" and at is not None:
            paused[key] = at
        elif action == "cont" and at is not None and key in paused:
            pause_seconds[key] = pause_seconds.get(key, 0) + at - paused.pop(key)
        elif action in ("pass", "fail", "skip"):
            measured = dict(event)
            if at is not None and key in starts:
                measured["Elapsed"] = max(event.get("Elapsed", 0), at - starts[key] - pause_seconds.get(key, 0))
            completed.append(measured)
    slow = sorted(completed, key=lambda e: -e.get("Elapsed", 0))[:5]
    running = {(e["Package"], e["Test"]) for e in events if e.get("Action") == "run" and "/" not in e["Test"]}
    running -= {(e["Package"], e["Test"]) for e in completed}
    detail = "; ".join(f"{e['Package']}/{e['Test']}={e.get('Elapsed', 0):.2f}s" for e in slow)
    if running:
        detail += "; still running: " + ", ".join(f"{p}/{n}" for p, n in sorted(running))
    failures = [
        f"{e['Package']}/{e['Test']} {e.get('Elapsed', 0):.2f}s exceeds {TEST_BUDGET_SECONDS}s test budget"
        for e in completed
        if e.get("Elapsed", 0) > TEST_BUDGET_SECONDS
    ]
    if elapsed > shard_budget:
        failures.append(f"{elapsed:.2f}s exceeds {shard_budget}s shard budget")
    print(f"Operator timing: {elapsed:.2f}s; slowest: {detail}", flush=True)
    if failures:
        print("Timing report-only: " + "; ".join(failures), flush=True)


def run_tests(
    command: list[str],
    *,
    started: float,
    cwd: Path = OPERATOR,
    budget_seconds: float | None = None,
) -> None:
    """Stream diagnostics; the unchanged Go and hosted job deadlines own failure."""
    shard_budget = SHARD_BUDGET_SECONDS if budget_seconds is None else budget_seconds
    events: list[dict] = []
    parse_error: json.JSONDecodeError | None = None
    with subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, text=True) as process:
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as exc:
                parse_error = exc
        result = process.wait()
    check_budgets(events, elapsed=time.monotonic() - started, budget_seconds=shard_budget)
    if parse_error is not None:
        raise ValueError(f"invalid Go test JSON: {parse_error}")
    if result:
        raise subprocess.CalledProcessError(result, command)
