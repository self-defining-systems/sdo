"""Cheap, deterministic benchmark admission contracts; no operational decisions."""

from __future__ import annotations

import json
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
    busy = incident_work_open(state)
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


#: Closure states in which no incident is being closed, reflected or acknowledged.
IDLE_CLOSURE_STATES = frozenset({None, 0, "", "CLOSURE_STATE_UNSPECIFIED"})


def terminal_incident_ids(state: dict[str, Any]) -> set[str]:
    """Incidents whose closure is over: acknowledged, permanently failed or retired."""
    terminal = {state.get("last_acknowledged_incident_id")}
    for failure in state.get("failed_closures") or []:
        terminal.add(failure.get("closure", {}).get("request", {}).get("incident_id"))
    failure = state.get("closure_failure") or {}
    if state.get("closure_state") == "CLOSURE_STATE_FAILED" and failure.get("permanent") is True:
        terminal.add(failure.get("incident_id"))
    return {incident_id for incident_id in terminal if isinstance(incident_id, str)}


def incident_work_open(state: dict[str, Any]) -> bool:
    """The one "closure is over" rule: whether SDO still owns incident work.

    An open incident or request is work. A pending closure or a non-idle
    closure state is work unless its incident's closure is terminal
    (acknowledged, permanently failed or retired). A failed attempt's
    reflection queued or running after its acknowledgment is work, and so is
    its accepted detector change until the restarted controller records the
    rollout, so the next problem starts only on the controller that learned. A
    queue the controller could not read is unknown, which counts as work.
    """
    if (
        state.get("incident_request")
        or state.get("incident_open")
        or state.get("queued_reflection_incident_ids")
        or state.get("queued_reflections_unknown")
    ):
        return True
    closure_state = state.get("closure_state")
    pending = state.get("pending_closure")
    if not pending and closure_state in IDLE_CLOSURE_STATES:
        return False
    request = (pending or {}).get("request") or {}
    incident_id = (
        request.get("incident_id")
        or (state.get("closure_receipt") or {}).get("incident_id")
        or (state.get("closure_failure") or {}).get("incident_id")
    )
    return incident_id not in terminal_incident_ids(state)


#: A queued reflection runs for at most the controller's response timeout: its frozen deadline is
#: dispatch time + ``--response-timeout`` (registered_reflection.go ReflectionDeadline), 30 min by default (run.go).
DEFAULT_RESPONSE_TIMEOUT_SECONDS = 1800.0
#: Transient failures retry with the default closure backoff (2+4+8+16+32+60+60 s, closure_retry.go and
#: reflection_queue.go) before the reflection is abandoned.
CLOSURE_RETRY_BACKOFF_SECONDS = 182.0
#: Commit, cleanup and detector-rollout handoff after the last attempt.
REFLECTION_CLOSEOUT_SECONDS = 120.0


def reflection_attempt_seconds(response_timeout_seconds: float = DEFAULT_RESPONSE_TIMEOUT_SECONDS) -> float:
    """SDO's worst case to finish or abandon one queued reflection (the FIFO runs one at a time)."""
    if response_timeout_seconds <= 0:
        raise ValueError(f"response_timeout_seconds must be positive, got {response_timeout_seconds}")
    return response_timeout_seconds + CLOSURE_RETRY_BACKOFF_SECONDS + REFLECTION_CLOSEOUT_SECONDS


def queued_reflection_ids(state: dict[str, Any]) -> list[str]:
    """Incidents whose acknowledged closure still has a reflection queued, oldest first."""
    ids = state.get("queued_reflection_incident_ids") or []
    return [incident_id for incident_id in ids if isinstance(incident_id, str)]


def queued_reflection_count(state: dict[str, Any]) -> int:
    """Reflections SDO still owes; a queue the controller could not read counts as one."""
    return len(queued_reflection_ids(state)) + (1 if state.get("queued_reflections_unknown") else 0)


def wait_for_proof(
    sample: Callable[[], dict[str, Any]],
    record: Callable[[dict[str, Any]], None],
    *,
    timeout_seconds: float = 300,
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
