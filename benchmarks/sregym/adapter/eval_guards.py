"""Cheap, deterministic benchmark admission contracts; no operational decisions."""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

HEALTH_SETUP_PATHS = (
    ".sdo/diagnostics/detectors/health/",
    ".sdo/diagnostics/traffic/",
    ".sdo/playbooks/health-objective/",
    ".sdo/rendered/",
)


class StabilizationError(RuntimeError):
    """The app never established independent and controller traffic proof."""


@dataclass(frozen=True)
class Deadline:
    """Child operations consume the same monotonic budget, never renew it."""

    expires_at: float
    monotonic: Callable[[], float] = time.monotonic

    def remaining(self) -> float:
        remaining = self.expires_at - self.monotonic()
        if remaining <= 0:
            raise StabilizationError("eval: admission deadline exhausted; no injection")
        return remaining


def validate_setup_commit(actor: str, phase: str, validation: str, paths: list[str]) -> None:
    if actor not in {"health_judge", "controller"} or phase not in {"initial", "rendered"} or validation != "passed":
        raise ValueError(f"unvalidated health setup commit: actor={actor}, phase={phase}, validation={validation}")
    if not paths or any(
        (not path.startswith(HEALTH_SETUP_PATHS) and path != ".sdo/playbooks/README.md") or ".." in Path(path).parts
        for path in paths
    ):
        raise ValueError(f"setup changed paths outside health-owned artifacts: {paths}")


def setup_lineage(repository: Path, seed: str, head: str = "HEAD") -> dict[str, Any]:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(repository), *args], check=True, capture_output=True, text=True, timeout=10
        ).stdout.strip()

    git("merge-base", "--is-ancestor", seed, head)
    commits = []
    for sha in git("rev-list", "--reverse", f"{seed}..{head}").splitlines():
        parents = git("show", "-s", "--format=%P", sha).split()
        if len(parents) != 1:
            raise ValueError("fresh setup must have a linear accepted lineage")
        trailers = dict(
            line.split(": ", 1)
            for line in git("show", "-s", "--format=%B", sha).splitlines()
            if line.startswith("SDO-") and ": " in line
        )
        paths = git("diff-tree", "--no-commit-id", "--name-only", "-r", sha).splitlines()
        validate_setup_commit(
            trailers.get("SDO-Actor", ""), trailers.get("SDO-Phase", ""), trailers.get("SDO-Validation", ""), paths
        )
        commits.append({"sha": sha, "actor": trailers["SDO-Actor"], "phase": trailers["SDO-Phase"], "paths": paths})
    return {"passed": True, "seed": seed, "accepted_commit": git("rev-parse", head), "commits": commits}


def controller_proof(state: dict[str, Any], required: set[str], not_before: str) -> dict[str, Any]:
    latest = {row.get("detector_id"): row for row in (state.get("history") or [])}
    blocked = {}
    boundary = datetime.fromisoformat(not_before.replace("Z", "+00:00"))
    for key in required | set(latest):
        row = latest.get(key, {})
        try:
            fresh = datetime.fromisoformat(row.get("evaluated_at", "").replace("Z", "+00:00")) >= boundary
        except (ValueError, TypeError):
            fresh = False
        if row.get("status") != "clear" or row.get("error") or not fresh:
            blocked[key] = row or {"reason": "no controller observation"}
    busy = any(state.get(key) for key in ("incident_open", "incident_request", "pending_closure"))
    return {
        "passed": bool(required) and not blocked and not busy,
        "blocked": blocked,
        "incident_work_pending": busy,
        "latest": latest,
    }


def qualified_scenarios(windows: dict[str, Any], required: set[str]) -> bool:
    observed = {
        row.get("scenario", {}).get("id"): row
        for window in (windows.get("windows") or {}).values()
        for row in (window.get("scenarios") or [])
    }
    return (
        bool(required)
        and required.issubset(observed)
        and all(observed[key].get("qualified") is True and bool(observed[key].get("samples")) for key in required)
    )


def assert_stable_workloads(before: dict[str, Any], after: dict[str, Any]) -> None:
    if before != after:
        changed = sorted(key for key in before.keys() | after.keys() if before.get(key) != after.get(key))
        raise ValueError(f"restore invariant: workloads rolled after Ready: {changed}")


def closure_pending(state: dict[str, Any]) -> bool:
    return bool(state.get("pending_closure") or state.get("incident_request") or state.get("incident_open"))


def wait_for_proof(
    sample: Callable[[], dict[str, Any]],
    restore: Callable[[], None],
    record: Callable[[dict[str, Any]], None],
    *,
    timeout_seconds: float = 300,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    deadline: Deadline | None = None,
) -> dict[str, Any]:
    if timeout_seconds <= 0 or timeout_seconds > 300:
        raise ValueError("stabilization budget must be in (0, 300] seconds")
    started = monotonic()
    budget = deadline or Deadline(started + timeout_seconds, monotonic)
    retry_at = started + budget.remaining() / 2
    for attempt in range(2):
        attempt_end = retry_at if attempt == 0 else budget.expires_at
        while monotonic() < attempt_end:
            proof = sample()
            if monotonic() >= budget.expires_at:
                proof.update(passed=False, reason="eval: admission deadline exhausted; no injection")
            proof.update(attempt=attempt + 1, seconds=monotonic() - started)
            record(proof)
            if proof.get("passed") is True:
                return proof
            sleep(min(2, max(0, attempt_end - monotonic())))
        if attempt == 0:
            budget.remaining()
            restore()
    raise StabilizationError("eval: traffic proof failed after one app-only restore and retry; no injection")
