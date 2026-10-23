"""Benchmark-only gate that injects a deferred SREGym fault into a monitored app.

SDO operates an application continuously: it deploys, judges health, and
installs its controller before anything goes wrong. The gate reproduces that
order inside SREGym by asking the conductor to inject the fault only after the
controller has evaluated its detectors once with no active finding.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.protocol import request_with_retry
from benchmarks.sregym.protocol.injection import InvalidInjectionError, check_injection

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable

INJECT_FAULT_ENDPOINT = "/inject_fault"
AWAITING_FAULT_INJECTION = "awaiting_fault_injection"
BASELINE_TIMEOUT_SECONDS = 1800
# Reuse needs an informer resync and a fresh evaluation, not lifecycle setup.
RESUMED_BASELINE_TIMEOUT_SECONDS = 300
BASELINE_POLL_SECONDS = 1.0
PERSISTENT_LOG_TAIL = 2000
BASELINE_PROGRESS_SECONDS = 30.0
logger = logging.getLogger(__name__)


class FaultGateError(RuntimeError):
    """Raised when the fault cannot be injected into a monitored baseline."""


class InvalidFaultInjection(InvalidInjectionError, FaultGateError):
    """Deferred injection ended without opening a valid incident."""


class KubectlRunner(Protocol):
    def __call__(self, args: list[str], *, namespace: str, check: bool) -> subprocess.CompletedProcess[str]: ...


def controller_active_findings(controller_logs: str) -> list[str] | None:
    """Return the active rule IDs of the latest controller evaluation, or None before any evaluation."""

    latest: list[str] | None = None
    for line in controller_logs.splitlines():
        try:
            payload: Any = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict) or "controller_iteration" not in payload:
            continue
        findings = payload.get("findings") or []
        latest = [
            str(finding.get("rule_id"))
            for finding in (findings if isinstance(findings, list) else [])
            if isinstance(finding, dict) and finding.get("status") == "active"
        ]
    return latest


def controller_baseline_clear(controller_logs: str) -> bool:
    """Return whether the latest controller evaluation reported no active finding."""

    return controller_active_findings(controller_logs) == []


def request_fault_injection(api_base: str) -> None:
    """Ask the conductor to inject the deferred fault and open the incident."""

    response = request_with_retry("POST", f"{api_base.rstrip('/')}{INJECT_FAULT_ENDPOINT}", max_retries=2, timeout=300)
    reply = response.json()
    try:
        check_injection(reply)
    except InvalidInjectionError as exc:
        raise InvalidFaultInjection(exc.details, reply=reply) from exc
    stage = reply.get("stage")
    if stage in (None, AWAITING_FAULT_INJECTION, "done", "finished", "completed"):
        raise FaultGateError(f"conductor did not open an incident stage after injection (stage={stage!r})")


def inject_fault_after_controller_baseline(
    namespace: str,
    *,
    inject: Callable[[], None],
    kubectl_runner: KubectlRunner,
    not_before: datetime,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    timeout_seconds: float = BASELINE_TIMEOUT_SECONDS,
) -> dict[str, float]:
    """Wait for an all-clear evaluation from a controller Job created at or after
    ``not_before`` (never a previous round's Job), then inject exactly once."""

    started = monotonic()
    deadline = started + timeout_seconds
    last_error = "controller Job has not produced an evaluation"
    while monotonic() < deadline:
        if not _current_controller_job(namespace, kubectl_runner, not_before):
            last_error = "current controller Job has not been created"
            sleep(BASELINE_POLL_SECONDS)
            continue
        completed = kubectl_runner(["logs", "job/sdo-controller-run", "--tail=200"], namespace=namespace, check=False)
        active = controller_active_findings(completed.stdout) if completed.returncode == 0 else None
        if active:
            last_error = f"latest controller evaluation has active findings {sorted(set(active))}"
        if active == []:
            baseline_ready = monotonic()
            inject()
            return {
                "controller_baseline_wait": baseline_ready - started,
                "fault_injection_request": monotonic() - baseline_ready,
            }
        if completed.returncode != 0:
            last_error = (completed.stderr or completed.stdout or "").strip() or last_error
        sleep(BASELINE_POLL_SECONDS)
    raise FaultGateError(f"controller did not report an all-clear baseline within {timeout_seconds:.0f}s: {last_error}")


def _current_controller_job(namespace: str, kubectl_runner: KubectlRunner, not_before: datetime) -> bool:
    completed = kubectl_runner(["get", "job/sdo-controller-run", "-o", "json"], namespace=namespace, check=False)
    if completed.returncode != 0:
        return False
    try:
        created = json.loads(completed.stdout)["metadata"]["creationTimestamp"]
        created_at = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
    except (KeyError, TypeError, ValueError):
        return False
    # creationTimestamp has one-second resolution.
    return created_at >= not_before.replace(microsecond=0)


def controller_active_findings_after_resume(controller_logs: str, generation: str) -> list[str] | None:
    """Return the latest evaluation's active rule IDs after the controller resumed ``generation``.

    A persistent controller observes several benchmark problems. Only an
    evaluation after it announced this stage's resume (following the stage's
    application deploy) counts; a later pause invalidates the baseline.
    """

    latest: list[str] | None = None
    resumed = False
    for line in controller_logs.splitlines():
        try:
            payload: Any = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        if "controller_maintenance" in payload:
            resumed = (
                payload.get("controller_maintenance") == "active"
                and payload.get("maintenance_generation") == generation
            )
            latest = None
            continue
        if resumed and "controller_iteration" in payload:
            latest = controller_active_findings(line)
    return latest


def inject_fault_after_resumed_baseline(
    control_namespace: str,
    generation: str,
    *,
    inject: Callable[[], None],
    kubectl_runner: KubectlRunner,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    timeout_seconds: float = RESUMED_BASELINE_TIMEOUT_SECONDS,
    poll_seconds: float = BASELINE_POLL_SECONDS,
) -> dict[str, float]:
    """Inject once the persistent controller reports all-clear after resuming ``generation``."""

    started = monotonic()
    deadline = started + timeout_seconds
    next_progress = started
    last_error = f"controller has not resumed maintenance generation {generation!r}"
    while monotonic() < deadline:
        completed = kubectl_runner(
            ["logs", "job/sdo-controller-run", "--container=controller", f"--tail={PERSISTENT_LOG_TAIL}"],
            namespace=control_namespace,
            check=False,
        )
        active = (
            controller_active_findings_after_resume(completed.stdout, generation) if completed.returncode == 0 else None
        )
        if active:
            last_error = f"latest controller evaluation has active findings {sorted(set(active))}"
        if active == []:
            baseline_ready = monotonic()
            inject()
            return {
                "controller_baseline_wait": baseline_ready - started,
                "fault_injection_request": monotonic() - baseline_ready,
            }
        if completed.returncode != 0:
            last_error = (completed.stderr or completed.stdout or "").strip() or last_error
        elif active is None:
            # Informer authorization failures precede the resume record: the
            # controller emits it only after its namespace cache has synced.
            # Keep waiting for recovery, but expose the actual readiness error.
            forbidden = next(
                (line.strip() for line in reversed(completed.stdout.splitlines()) if " is forbidden:" in line), None
            )
            if forbidden:
                last_error = (
                    f"check namespace-scoped Role/RoleBinding grants for the controller ServiceAccount: {forbidden}"
                )
        now = monotonic()
        if now >= next_progress:
            logger.info(
                "waiting for controller baseline in %s, generation %s (%.0f/%.0fs): %s",
                control_namespace,
                generation,
                now - started,
                timeout_seconds,
                last_error,
            )
            next_progress = now + BASELINE_PROGRESS_SECONDS
        sleep(min(poll_seconds, max(0.0, deadline - now)))
    raise FaultGateError(
        f"controller in {control_namespace!r}, generation {generation!r}, did not report an all-clear baseline "
        f"within {timeout_seconds:.0f}s: {last_error}; inspect "
        f"kubectl -n {control_namespace} logs job/sdo-controller-run --container=controller"
    )
