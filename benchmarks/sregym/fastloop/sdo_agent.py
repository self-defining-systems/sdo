"""SDO's long-running controller as a fast-loop agent.

Each incident is one :func:`run_persistent_stage` against the application's
persistent controller: the first incident validates the lifecycle and installs
the controller; later ones reuse it. The controller reports an all-clear
baseline before the fault is injected, detects, dispatches a responder, and
independently verifies recovery. :meth:`SdoPersistentAgent.learn` then waits for
reflection and detector rollout and reads the strict receipt, so reflection is
measured separately from resolution.

There is no benchmark submission transport: the responder repairs and
verifies, and the loop grades the cluster with the problem's own oracle.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import (
    REJECTED_RECEIPT_FILENAME,
    STRICT_RECEIPT_FILENAME,
    Clock,
    ClusterOps,
    PersistentState,
    ReceiptRejectedError,
    StageInputs,
    drain_pending_incident,
    run_persistent_stage,
)
from benchmarks.sregym.fastloop.loop import AgentOutcome, InjectionWindow
from benchmarks.sregym.fastloop.records import AgentName, TokenCounts
from benchmarks.sregym.protocol import receipt_resolution

if TYPE_CHECKING:
    from collections.abc import Callable

    from benchmarks.sregym.adapter import DeployedLifecycle, DeployedLifecycleContext, RuntimeConfig


@dataclass(frozen=True)
class SdoAgentSettings:
    repository: Path
    namespace: str
    application: str
    runtime_config: RuntimeConfig
    state_path: Path
    results_dir: Path
    kubeconfig: str | None = None
    verification_timeout_seconds: float = 3900.0

    def __post_init__(self) -> None:
        if self.runtime_config.submission_api_base or self.runtime_config.submission_relay_target_base:
            raise ValueError("the fast loop has no benchmark submission transport")
        if self.verification_timeout_seconds <= 0:
            raise ValueError("verification_timeout_seconds must be positive")


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _last_successful_repair(repairs: object) -> datetime | None:
    completions = [
        completed
        for item in (repairs if isinstance(repairs, list) else [])
        if isinstance(item, dict) and item.get("success") is True
        for completed in [_timestamp(item.get("completed_at"))]
        if completed is not None
    ]
    return max(completions) if completions else None


def _receipt_closure_times(receipt: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    """Detection and verification times, recovered from the receipt's ``recorded_at`` and phase timings.

    The runtime receipt derives ``total`` and ``post_recovery_learning_and_receipt``
    from the broker closure's ``detected_at`` and ``verified_at``, so this is exact.
    """

    recorded = _timestamp(receipt.get("recorded_at"))
    phases = receipt.get("phase_timings_seconds")
    if recorded is None or not isinstance(phases, dict):
        return None, None
    total = phases.get("total")
    learning = phases.get("post_recovery_learning_and_receipt")
    detected = recorded - timedelta(seconds=float(total)) if isinstance(total, (int, float)) else None
    verified = recorded - timedelta(seconds=float(learning)) if isinstance(learning, (int, float)) else None
    return detected, verified


def _summaries(items: object, *, successful_only: bool = False) -> str:
    if not isinstance(items, list):
        return ""
    return "; ".join(
        str(item.get("summary", "")).strip()
        for item in items
        if isinstance(item, dict)
        and str(item.get("summary", "")).strip()
        and (not successful_only or item.get("success") is True)
    )


class SdoPersistentAgent:
    name: AgentName = "sdo"

    def __init__(
        self,
        settings: SdoAgentSettings,
        *,
        ops: ClusterOps,
        lifecycle_inputs: Callable[[], DeployedLifecycle],
        run_lifecycle: Callable[[DeployedLifecycleContext], bool],
        clock: Clock | None = None,
        keep_running: bool = False,
        stop_on_detector_review: bool = True,
    ) -> None:
        self._keep_running = keep_running
        self._stop_on_detector_review = stop_on_detector_review
        self._settings = settings
        self._ops = ops
        self._lifecycle_inputs = lifecycle_inputs
        self._run_lifecycle = run_lifecycle
        self._clock = clock or Clock()

    @property
    def model(self) -> str:
        return self._settings.runtime_config.model

    def stage_inputs(self, stage_label: str, receipt_dir: Path, fingerprint: str) -> StageInputs:
        settings = self._settings
        return StageInputs(
            stage_label=stage_label,
            application=settings.application,
            namespace=settings.namespace,
            lifecycle_fingerprint=fingerprint,
            runtime_config=replace(settings.runtime_config, artifacts_dir=receipt_dir),
            receipt_dir=receipt_dir,
            state_path=settings.state_path,
            kubeconfig=settings.kubeconfig,
            verification_timeout_seconds=settings.verification_timeout_seconds,
            stop_on_detector_review=self._stop_on_detector_review,
        )

    def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
        settings = self._settings
        lifecycle = self._lifecycle_inputs()
        receipt_dir = settings.results_dir / f"{index:03d}_{problem_id}"
        windows: list[InjectionWindow] = []

        def gated_inject() -> None:
            windows.append(inject())

        resolution = run_persistent_stage(
            self.stage_inputs(f"fastloop-{index:03d}", receipt_dir, lifecycle.fingerprint),
            ops=self._ops,
            run_lifecycle=lambda: self._run_lifecycle(lifecycle.context),
            inject=gated_inject,
            clock=self._clock,
            pause_after_verified=not self._keep_running,
        )
        if not windows:
            raise RuntimeError("the controller verified an incident without the fault being injected")
        verified = _timestamp(resolution.get("verified_at"))
        seconds = resolution.get("incident_resolution_seconds")
        detected = (
            verified - timedelta(seconds=float(seconds))
            if verified is not None and isinstance(seconds, (int, float))
            else None
        )
        repairs = resolution.get("repair_actions")
        gate = resolution.get("fault_gate_timings_seconds") or {}
        costs = resolution.get("pre_injection_costs_seconds") or {}
        controller = resolution.get("persistent_controller") or {}
        return AgentOutcome(
            injection=windows[0],
            detected_at=detected,
            responder_started_at=_timestamp(resolution.get("responder_started_at")),
            responder_finished_at=_timestamp(resolution.get("responder_finished_at")),
            mitigation_applied_at=_last_successful_repair(repairs),
            resolved_at=verified,
            diagnosis=_summaries(resolution.get("confirmed_root_causes")),
            mitigation=_summaries(repairs, successful_only=True),
            baseline_gate_seconds=gate.get("controller_baseline_wait"),
            setup_seconds=float(costs.get("inventory_and_lifecycle", 0.0))
            + float(costs.get("controller_install_or_reuse", 0.0)),
            controller_installed=controller.get("installed_this_stage"),
            lifecycle_reused=resolution.get("lifecycle_reused"),
            lifecycle_validation_source=(resolution.get("lifecycle_validation") or {}).get("source"),
            previous_reflection_drain_seconds=costs.get("previous_incident_reflection_drain"),
            incident_id=str(resolution["incident_id"]),
            artifacts_dir=str(receipt_dir),
        )

    def learn(self, outcome: AgentOutcome) -> AgentOutcome:
        """Wait for reflection and detector rollout, then read the incident's strict receipt."""

        settings = self._settings
        state = PersistentState.load(settings.state_path)
        record = state.controllers.get(settings.namespace)
        if record is None or record.pending is None or record.pending.incident_id != outcome.incident_id:
            raise RuntimeError(f"incident {outcome.incident_id!r} is not pending on the persistent controller")
        started = self._clock.monotonic()
        reflection_observed_at = self._clock.now()
        error: str | None = None
        try:
            drain_pending_incident(
                record,
                ops=self._ops,
                repository=settings.repository,
                drained_by=f"fastloop-learn-{outcome.incident_id}",
                clock=self._clock,
            )
        except ReceiptRejectedError as exc:
            error = f"strict receipt rejected: {exc}"
        # Receipt rejection happens after the controller has acknowledged and relaunched.
        # Other drain failures leave the pending gate intact and stop the stream.
        record.pending = None
        state.save(settings.state_path)
        receipt_dir = Path(str(outcome.artifacts_dir))
        receipt = self._receipt(receipt_dir)
        if not receipt:
            raise RuntimeError(f"incident {outcome.incident_id!r} receipt missing after reflection drain")
        # Only a receipt that passed strict validation states how the incident closed.
        resolution = (
            receipt.get("resolution") or receipt_resolution(receipt)
            if (receipt_dir / STRICT_RECEIPT_FILENAME).is_file()
            else None
        )
        memory = receipt.get("memory_reuse")
        memory = memory if isinstance(memory, dict) else {}
        attempts = receipt.get("reflection_attempts")
        skipped = receipt.get("reflection_skipped_reason")
        # When reflection finished between polls the driver never saw the closure; the receipt has it.
        detected, verified = _receipt_closure_times(receipt)
        ledger = receipt.get("native_ledger")
        ledger = ledger if isinstance(ledger, dict) else {}
        outcome_document = ledger.get("outcome_document")
        outcome_document = outcome_document if isinstance(outcome_document, dict) else {}
        rollout_receipts = ledger.get("rollout_receipts")
        rollout_receipts = rollout_receipts if isinstance(rollout_receipts, list) else []
        rebuild_verified = next(
            (
                _timestamp(item.get("verified_at"))
                for item in rollout_receipts
                if isinstance(item, dict) and item.get("success") is True
            ),
            None,
        )
        return outcome.with_learning(
            detected_at=outcome.detected_at or detected,
            mitigation_applied_at=outcome.mitigation_applied_at
            or _last_successful_repair(receipt.get("repair_actions")),
            resolved_at=outcome.resolved_at or verified,
            diagnosis=outcome.diagnosis or _summaries(receipt.get("confirmed_root_causes")),
            mitigation=outcome.mitigation or _summaries(receipt.get("repair_actions"), successful_only=True),
            reflection_seconds=self._clock.monotonic() - started,
            reflection_started_at=reflection_observed_at,
            reflection_finished_at=self._clock.now(),
            rebuild_done_at=rebuild_verified or self._clock.now(),
            responder_started_at=outcome.responder_started_at or _timestamp(receipt.get("responder_started_at")),
            responder_finished_at=outcome.responder_finished_at or _timestamp(receipt.get("responder_finished_at")),
            responder_tokens=TokenCounts.from_usage(receipt.get("usage") or outcome_document.get("usage")),
            reflection_tokens=TokenCounts.from_usage(receipt.get("reflection_usage")),
            warm_path=bool(memory["warm_path"]) if "warm_path" in memory else None,
            match_reasons=tuple(str(reason) for reason in memory.get("match_reasons", []) or []),
            reflection_attempts=attempts if isinstance(attempts, int) else None,
            reflection_skipped_reason=skipped if isinstance(skipped, str) else None,
            sdo_resolution=resolution if isinstance(resolution, str) else None,
            error=error,
        )

    @staticmethod
    def _receipt(receipt_dir: Path) -> dict[str, Any]:
        strict = receipt_dir / STRICT_RECEIPT_FILENAME
        if strict.is_file():
            document = json.loads(strict.read_text(encoding="utf-8"))
            return document if isinstance(document, dict) else {}
        rejected = receipt_dir / REJECTED_RECEIPT_FILENAME
        if rejected.is_file():
            document = json.loads(rejected.read_text(encoding="utf-8")).get("receipt")
            return document if isinstance(document, dict) else {}
        return {}

    def close(self) -> None:
        """The controller keeps running for the next loop; ``fastloop down`` stops it."""
