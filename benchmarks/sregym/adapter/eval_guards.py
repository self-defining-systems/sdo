"""Cheap, deterministic benchmark admission contracts; no operational decisions."""

from __future__ import annotations

import json
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter.budgets import WaitKind, wait_for

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
            ["git", "-C", str(repository), *args],
            check=True,
            capture_output=True,
            text=True,
            timeout=wait_for(WaitKind.PROBE),
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
    busy = incident_work_open(state)
    return {
        "passed": bool(required) and not blocked and not busy,
        "blocked": blocked,
        "incident_work_pending": busy,
        "latest": latest,
    }


def shell_runner_proof(evidence: dict[str, Any], not_before: str, now: str) -> dict[str, Any]:
    """Bind complete clear shell results to live accepted Git inventory, never Go policy."""

    def rejected(reason: str) -> dict[str, Any]:
        return {"passed": False, "reason": reason, "evidence": evidence}

    try:
        boundary = datetime.fromisoformat(not_before.replace("Z", "+00:00"))
        current = datetime.fromisoformat(now.replace("Z", "+00:00"))
        report = evidence["report"]
        commit = evidence["accepted_commit"]
        scripts = evidence["scripts"]
        if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", commit) or report["accepted_commit"] != commit:
            return rejected("shell runner is not observing the live accepted commit")
        if report["version"] != 1 or report.get("error") or report.get("healthy") is not True:
            return rejected("shell runner has not established stable health")
        if (
            not scripts
            or len(set(scripts)) != len(scripts)
            or not any(path.startswith(".sdo/diagnostics/detectors/health/") for path in scripts)
        ):
            return rejected("accepted shell policy has no executable health detectors")
        rows = report["results"]
        if len(rows) != len(scripts) or {row["script"] for row in rows} != set(scripts):
            return rejected("shell runner results do not cover every accepted executable detector")
        activated = datetime.fromisoformat(report["activated_at"].replace("Z", "+00:00"))
        for stamp in [report["at"], *(row["at"] for row in rows)]:
            observed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            if observed < boundary or observed < activated or (observed - current).total_seconds() > 5:
                return rejected("shell runner observation is stale or has invalid activation time")
        if any(
            type(row["exit_code"]) is not int
            or row["exit_code"] != 0
            or row.get("error")
            or row.get("timed_out")
            or row.get("fired")
            for row in rows
        ):
            return rejected("accepted shell detectors have nonzero, timed out or errored results")
    except (KeyError, ValueError, TypeError, AttributeError):
        return rejected("shell runner evidence is incomplete or malformed")
    return {"passed": True, "evidence": evidence}


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


def terminal_incident_ids(state: dict[str, Any]) -> set[str]:
    """Incidents whose closure is over: acknowledged, permanently failed or retired."""
    terminal = {state.get("last_acknowledged_incident_id")}
    for failure in state.get("failed_closures") or []:
        terminal.add(failure.get("closure", {}).get("request", {}).get("incident_id"))
    failure = state.get("closure_failure") or {}
    if state.get("closure_state") == "CLOSURE_STATE_FAILED" and failure.get("permanent") is True:
        terminal.add(failure.get("incident_id"))
    return {incident_id for incident_id in terminal if isinstance(incident_id, str)}


def busy_until(state: dict[str, Any]) -> datetime | None:
    """The controller's one published fact about incident work: unset when idle, else its own latest end.

    The controller computes it from its open incident, closure in flight and reflection queue, and from its
    own response timeout, verification timeout and retry policy (``busyUntilLocked`` in the runtime). Every
    eval wait on SDO's incident work reads this and nothing else: no Job list, label or queue count.
    """
    raw = state.get("busy_until")
    if not raw:
        return None
    return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))


def incident_work_open(state: dict[str, Any]) -> bool:
    """Whether SDO still owns incident work: exactly whether the controller published ``busy_until``."""
    return busy_until(state) is not None


def busy_remaining_seconds(state: dict[str, Any], now: datetime | None = None) -> float | None:
    """Seconds until the controller's own bound on its work (negative once passed); None when idle."""
    until = busy_until(state)
    if until is None:
        return None
    return (until - (now or datetime.now(timezone.utc))).total_seconds()


@dataclass
class BusyDeadline:
    """A wait bound that follows ``busy_until`` and nothing else.

    ``margin_seconds`` is the only slack past the controller's own bound. The deadline never moves back, and
    it only moves forward when the controller itself raises ``busy_until`` (new work joined), so a controller
    that stays busy past its bound expires the wait one margin later instead of extending it forever.
    """

    margin_seconds: float
    deadline: float | None = None

    def __post_init__(self) -> None:
        if self.margin_seconds < 0:
            raise ValueError(f"margin_seconds must not be negative, got {self.margin_seconds}")

    def observe(self, state: dict[str, Any], monotonic_now: float, now: datetime | None = None) -> bool:
        """Record a state read; true while the controller is busy."""
        remaining = busy_remaining_seconds(state, now)
        if remaining is None:
            return False
        candidate = monotonic_now + max(remaining, 0.0) + self.margin_seconds
        if self.deadline is None or remaining > 0:
            self.deadline = candidate if self.deadline is None else max(self.deadline, candidate)
        return True

    def expired(self, monotonic_now: float) -> bool:
        return self.deadline is not None and monotonic_now >= self.deadline

    def seconds_left(self, monotonic_now: float) -> float:
        return 0.0 if self.deadline is None else max(0.0, self.deadline - monotonic_now)


def queued_reflection_ids(state: dict[str, Any]) -> list[str]:
    """Incidents whose acknowledged closure still has a reflection queued, oldest first."""
    ids = state.get("queued_reflection_incident_ids") or []
    return [incident_id for incident_id in ids if isinstance(incident_id, str)]


def wait_for_proof(
    sample: Callable[[], dict[str, Any]],
    record: Callable[[dict[str, Any]], None],
    *,
    timeout_seconds: float = int(wait_for(WaitKind.ROLLOUT)),
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    deadline: Deadline | None = None,
) -> dict[str, Any]:
    """Observe admission without mutating the application or renewing its budget.

    Missing controller observations, unqualified traffic and pending reflection
    are not evidence that the application needs restoration. Recovery belongs
    to the explicit reset before admission; restarting here can create the very
    incident whose completion admission must await.
    """
    if timeout_seconds <= 0 or timeout_seconds > 300:
        raise ValueError("stabilization budget must be in (0, 300] seconds")
    started = monotonic()
    budget = deadline or Deadline(started + timeout_seconds, monotonic)
    while monotonic() < budget.expires_at:
        proof = sample()
        if monotonic() >= budget.expires_at:
            proof.update(passed=False, reason="eval: admission deadline exhausted; no injection")
        proof.update(attempt=1, seconds=monotonic() - started)
        record(proof)
        if proof.get("passed") is True:
            return proof
        sleep(min(2, max(0, budget.expires_at - monotonic())))
    raise StabilizationError("eval: admission proof did not settle before deadline; no injection")


#: Label the controller puts on its warm responder standby Job template.
STANDBY_LABEL = "sdo.dev/standby"
#: Label of the immutable identity ConfigMap the controller publishes before a
#: warm inbox releases its task (worker_registry.ensureIncidentExecution).
INCIDENT_EXECUTION_LABEL = "sdo.dev/incident-execution"


def job_labels(job: dict[str, Any]) -> dict[str, str]:
    """Job and Pod-template labels of a Job listing row."""
    return {
        **job.get("spec", {}).get("template", {}).get("metadata", {}).get("labels", {}),
        **job["metadata"].get("labels", {}),
    }


def idle_standby_jobs(jobs: list[dict[str, Any]], executions: list[dict[str, Any]]) -> set[str]:
    """Name warm standby Jobs no incident has claimed; a claimed standby is responder work.

    *executions* are the ConfigMaps labeled ``INCIDENT_EXECUTION_LABEL``. A
    malformed identity raises ``ValueError`` so callers fail closed.
    """
    claimed: set[str] = set()
    for record in executions:
        identity = json.loads(record.get("data", {}).get("execution.json", "null"))
        if (
            record["metadata"].get("labels", {}).get(INCIDENT_EXECUTION_LABEL) != "1"
            or not isinstance(identity, dict)
            or identity.get("version") != 1
            or not all(
                isinstance(identity.get(key), str) and identity[key]
                for key in ("job_name", "task_id", "incident_id", "request_sha256")
            )
            or record["metadata"].get("annotations", {}).get("sdo.dev/incident-id") != identity["incident_id"]
        ):
            raise ValueError(f"eval: invalid incident execution identity {record['metadata'].get('name')}")
        claimed.add(identity["job_name"])
    return {
        job["metadata"]["name"]
        for job in jobs
        if job_labels(job).get(STANDBY_LABEL) == "true"
        and job["metadata"]["name"] not in claimed
        and not job_labels(job).get("sdo.dev/validator-isolation")
        and not job_labels(job).get("sdo.dev/validator-canary")
    }
