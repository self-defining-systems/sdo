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
import logging
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import (
    REJECTED_RECEIPT_FILENAME,
    STRICT_RECEIPT_FILENAME,
    Clock,
    ClosureFailedError,
    ClusterOps,
    FollowupClosureTimeout,
    PersistentState,
    ReflectionDeadlineError,
    StageInputs,
    WaitKind,
    drain_pending_incident,
    pause_for_recovery,
    record_failed_closure,
    run_persistent_stage,
    wait_for,
)
from benchmarks.sregym.fastloop.loop import AgentOutcome, InjectionWindow, RecoveryWaitTimeout, ResolutionGradePoint
from benchmarks.sregym.fastloop.records import AgentName, PostClosureWait, RecoveryTrigger, TokenCounts
from benchmarks.sregym.fastloop.replay import CheckpointStore, capture_checkpoints
from benchmarks.sregym.protocol import receipt_resolution
from benchmarks.sregym.protocol.injection import ensure_incident_follows_injection

if TYPE_CHECKING:
    from collections.abc import Callable

    from benchmarks.sregym.adapter import (
        ControllerRecord,
        DeployedLifecycle,
        DeployedLifecycleContext,
        FollowupClosureWait,
        RuntimeConfig,
    )


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SdoAgentSettings:
    repository: Path
    namespace: str
    application: str
    runtime_config: RuntimeConfig
    state_path: Path
    results_dir: Path
    kubeconfig: str | None = None
    verification_timeout_seconds: float = wait_for(WaitKind.INCIDENT_VERIFICATION)
    reflection_wait_seconds: float = wait_for(WaitKind.REFLECTION_DRAIN)
    detection_timeout_seconds: float | None = None
    checkpoint_enabled: bool = True

    def __post_init__(self) -> None:
        if self.runtime_config.submission_api_base or self.runtime_config.submission_relay_target_base:
            raise ValueError("the fast loop has no benchmark submission transport")
        if self.verification_timeout_seconds <= 0:
            raise ValueError("verification_timeout_seconds must be positive")
        if self.reflection_wait_seconds <= 0:
            raise ValueError("reflection_wait_seconds must be positive")
        if self.detection_timeout_seconds is not None and self.detection_timeout_seconds <= 0:
            raise ValueError("detection_timeout_seconds must be positive")


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def drift_tokens_from_state(state: dict[str, Any], started: datetime, finished: datetime) -> TokenCounts:
    # Legacy investigator accounting; current controllers never publish this field.
    investigation = state.get("drift_investigation") or {}
    uses = investigation.get("token_usage", []) if isinstance(investigation, dict) else []
    input_tokens = 0
    output_tokens = 0
    for use in uses:
        if not isinstance(use, dict):
            continue
        at = _timestamp(use.get("at"))
        if at is not None and started <= at <= finished:
            input_tokens += int(use.get("input_tokens") or 0)
            output_tokens += int(use.get("output_tokens") or 0)
    return TokenCounts(input_tokens=input_tokens, output_tokens=output_tokens)


def _closure_phase_seconds(ledger: dict[str, Any]) -> dict[str, float]:
    phases = ledger.get("closure_phase_timestamps")
    if not isinstance(phases, dict):
        return {}
    ordered = sorted(
        ((at, name) for name, value in phases.items() if isinstance(name, str) for at in [_timestamp(value)] if at),
        key=lambda item: item[0],
    )
    return {name: (at - ordered[index - 1][0]).total_seconds() for index, (at, name) in enumerate(ordered) if index > 0}


def _receipt_closure_fields(receipt: dict[str, Any]) -> tuple[dict[str, float], str | None]:
    ledger = receipt.get("native_ledger")
    ledger = ledger if isinstance(ledger, dict) else {}
    skipped = receipt.get("reflection_skipped_reason") or ledger.get("reflection_skipped_reason")
    return _closure_phase_seconds(ledger), skipped if isinstance(skipped, str) else None


def _receipt_no_repair_count(receipt: dict[str, Any]) -> int | None:
    ledger = receipt.get("native_ledger")
    value = ledger.get("reflection_no_repair_count") if isinstance(ledger, dict) else None
    return value if type(value) is int and value >= 0 else None


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
    """Prefer durable outcome timestamps; older receipts carry equivalent phase timings."""
    ledger = receipt.get("native_ledger") or {}
    document = ledger.get("outcome_document") or {}
    timestamps = document.get("timestamps") or {}
    detected = _timestamp(timestamps.get("detected_at"))
    verified = _timestamp(timestamps.get("verified_at"))
    recorded = _timestamp(receipt.get("recorded_at"))
    phases = receipt.get("phase_timings_seconds")
    if recorded is None or not isinstance(phases, dict):
        return detected, verified
    total = phases.get("total")
    learning = phases.get("post_recovery_learning_and_receipt")
    if detected is None and isinstance(total, (int, float)):
        detected = recorded - timedelta(seconds=float(total))
    if verified is None and isinstance(learning, (int, float)):
        verified = recorded - timedelta(seconds=float(learning))
    return detected, verified


def _with_receipt_times(outcome: AgentOutcome, receipt: dict[str, Any]) -> AgentOutcome:
    ledger = receipt.get("native_ledger") or {}
    document = ledger.get("outcome_document") or {}
    timestamps = document.get("timestamps") or {}
    detected, verified = _receipt_closure_times(receipt)
    return replace(
        outcome,
        detected_at=outcome.detected_at or detected,
        resolved_at=outcome.resolved_at or verified,
        mitigation_applied_at=outcome.mitigation_applied_at
        or _timestamp(timestamps.get("mitigated_at"))
        or _last_successful_repair(receipt.get("repair_actions") or document.get("repair_actions")),
        responder_started_at=outcome.responder_started_at
        or _timestamp(timestamps.get("dispatched_at"))
        or _timestamp(receipt.get("responder_started_at")),
        responder_finished_at=outcome.responder_finished_at or _timestamp(receipt.get("responder_finished_at")),
        reflection_no_repair_count=_receipt_no_repair_count(receipt),
    )


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


def _program_metrics(result: dict[str, Any], memory_reasons: tuple[str, ...] = ()) -> dict[str, Any]:
    runs = result.get("program_runs")
    calls = result.get("program_tool_calls")
    verdicts: dict[str, int] = {}
    commands: dict[str, int] = {}
    match_reasons = list(memory_reasons)
    binding_sources: list[str] = []
    if isinstance(runs, list):
        for run in runs:
            if isinstance(run, dict):
                verdict = run.get("verdict")
                name = verdict.get("result") if isinstance(verdict, dict) else None
                if isinstance(name, str):
                    verdicts[name] = verdicts.get(name, 0) + 1
                if name == "RESULT_MATCH":
                    source = run.get("binding_source")
                    if isinstance(source, str) and source:
                        binding_sources.append(source)
                    evidence = verdict.get("evidence") if isinstance(verdict, dict) else None
                    facts = (
                        [
                            item["fact"]
                            for item in evidence
                            if isinstance(item, dict) and isinstance(item.get("fact"), str) and item["fact"]
                        ]
                        if isinstance(evidence, list)
                        else []
                    )
                    match_reasons.extend(facts)
                    if not facts and isinstance(run.get("program_id"), str):
                        match_reasons.append(f"program:{run['program_id']}")
    if isinstance(calls, list):
        for call in calls:
            if isinstance(call, dict) and isinstance(call.get("command"), str):
                name = call["command"]
                commands[name] = commands.get(name, 0) + 1
    loops = result.get("response_loops_used")
    return {
        "playbook_coverage": result.get("playbook_coverage"),
        "program_diagnoses": len(runs) if isinstance(runs, list) else None,
        "resolution_path": result.get("resolution_path") if result.get("resolution_path") == "responder" else None,
        "match_reasons": tuple(dict.fromkeys(match_reasons)),
        "binding_sources": tuple(dict.fromkeys(binding_sources)),
        "program_verdicts": verdicts,
        "program_tool_calls": len(calls) if isinstance(calls, list) else None,
        "tool_commands": commands,
        "response_loops_used": loops if isinstance(loops, int) else None,
    }


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
    ) -> None:
        self._keep_running = keep_running
        self._on_verified: Callable[[ResolutionGradePoint], None] | None = None
        self._settings = settings
        self._ops = ops
        self._lifecycle_inputs = lifecycle_inputs
        self._run_lifecycle = run_lifecycle
        self._clock = clock or Clock()
        self.last_recovery_trigger: RecoveryTrigger | None = None

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
            detection_timeout_seconds=settings.detection_timeout_seconds,
            lifecycle_installs_controller=True,
            reflection_wait_seconds=settings.reflection_wait_seconds,
        )

    def resolve_at_closure(
        self,
        index: int,
        problem_id: str,
        inject: Callable[[], InjectionWindow],
        on_verified: Callable[[ResolutionGradePoint], None],
    ) -> AgentOutcome:
        """Observe closure within the agent's complete (possibly composite) resolution path."""
        previous = self._on_verified
        self._on_verified = on_verified
        try:
            return self.resolve(index, problem_id, inject)
        finally:
            self._on_verified = previous

    def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
        settings = self._settings
        lifecycle = self._lifecycle_inputs()
        receipt_dir = settings.results_dir / f"{index:03d}_{problem_id}"
        windows: list[InjectionWindow] = []

        def gated_inject() -> None:
            windows.append(inject())

        def measure_verified(incident_id: str, detected_at: datetime | None, verified_at: datetime | None) -> None:
            if not windows:
                raise RuntimeError("the controller verified an incident without the fault being injected")
            ensure_incident_follows_injection(incident_id, detected_at, windows[0].started_at)
            if self._on_verified is not None:
                self._on_verified(ResolutionGradePoint(incident_id, verified_at))

        resolution = run_persistent_stage(
            self.stage_inputs(f"fastloop-{index:03d}", receipt_dir, lifecycle.fingerprint),
            ops=self._ops,
            run_lifecycle=lambda: self._run_lifecycle(lifecycle.context),
            inject=gated_inject,
            clock=self._clock,
            pause_after_verified=not self._keep_running,
            on_verified=measure_verified,
        )
        self._checkpoint(index, str(resolution["incident_id"]), receipt_dir)
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
        drift_tokens = TokenCounts()
        return AgentOutcome(
            injection=windows[0],
            detected_at=detected,
            responder_started_at=_timestamp(resolution.get("responder_started_at")),
            responder_finished_at=_timestamp(resolution.get("responder_finished_at")),
            mitigation_applied_at=_last_successful_repair(repairs),
            resolved_at=verified,
            diagnosis=_summaries(resolution.get("confirmed_root_causes")),
            mitigation=_summaries(repairs, successful_only=True),
            responder_tokens=TokenCounts.from_usage(resolution.get("usage")),
            drift_tokens=drift_tokens,
            baseline_gate_seconds=gate.get("controller_baseline_wait"),
            driver_phase_timings_seconds=resolution.get("driver_phase_timings_seconds") or {},
            fault_gate_timings_seconds=gate,
            setup_seconds=float(costs.get("inventory_and_lifecycle", 0.0))
            + float(costs.get("controller_install_or_reuse", 0.0)),
            controller_installed=controller.get("installed_this_stage"),
            lifecycle_reused=resolution.get("lifecycle_reused"),
            lifecycle_validation_source=(resolution.get("lifecycle_validation") or {}).get("source"),
            previous_reflection_drain_seconds=costs.get("previous_incident_reflection_drain"),
            incident_id=str(resolution["incident_id"]),
            artifacts_dir=str(receipt_dir),
            playbook_coverage=resolution.get("playbook_coverage"),
            resolution_path=resolution.get("resolution_path"),
        )

    def learn(self, outcome: AgentOutcome) -> AgentOutcome:
        """Wait for reflection and detector rollout, then read the incident's strict receipt."""

        settings = self._settings
        state = PersistentState.load(settings.state_path)
        record = state.controllers.get(settings.namespace)
        if (
            not outcome.incident_id
            or record is None
            or record.pending is None
            or record.pending.incident_id != outcome.incident_id
        ):
            raise RuntimeError(f"incident {outcome.incident_id!r} is not pending on the persistent controller")
        started = self._clock.monotonic()
        reflection_observed_at = self._clock.now()
        error: str | None = None
        try:
            drained = drain_pending_incident(
                record,
                ops=self._ops,
                repository=settings.repository,
                drained_by=f"fastloop-learn-{outcome.incident_id}",
                clock=self._clock,
            )
            if drained.HasField("failure"):
                error = f"strict receipt rejected: {drained.failure.message}"
        finally:
            if settings.kubeconfig:
                receipt_dir = Path(str(outcome.artifacts_dir))
                exported = self._ops.export_runtime_artifacts(
                    replace(settings.runtime_config, artifacts_dir=receipt_dir), receipt_dir
                )
                if exported.get("error"):
                    raise RuntimeError(f"incomplete replay recording: {exported['error']}")
                self._checkpoint(int(receipt_dir.name.split("_", 1)[0]), outcome.incident_id, receipt_dir)
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
        skipped = receipt.get("reflection_skipped_reason")
        # When reflection finished between polls the driver never saw the closure; the receipt has it.
        outcome = _with_receipt_times(outcome, receipt)
        closure_phases, skipped = _receipt_closure_fields(receipt)
        ledger = receipt.get("native_ledger")
        ledger = ledger if isinstance(ledger, dict) else {}
        outcome_document = ledger.get("outcome_document")
        outcome_document = outcome_document if isinstance(outcome_document, dict) else {}
        program_metrics = _program_metrics(
            outcome_document, tuple(str(reason) for reason in memory.get("match_reasons", []) or [])
        )
        program_metrics["playbook_coverage"] = program_metrics["playbook_coverage"] or outcome.playbook_coverage
        program_metrics["resolution_path"] = program_metrics["resolution_path"] or outcome.resolution_path

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
            diagnosis=outcome.diagnosis or _summaries(receipt.get("confirmed_root_causes")),
            mitigation=outcome.mitigation or _summaries(receipt.get("repair_actions"), successful_only=True),
            reflection_seconds=self._clock.monotonic() - started,
            reflection_started_at=reflection_observed_at,
            reflection_finished_at=self._clock.now(),
            rebuild_done_at=rebuild_verified or self._clock.now(),
            responder_tokens=TokenCounts.from_usage(receipt.get("usage") or outcome_document.get("usage"))
            if receipt.get("usage") or outcome_document.get("usage")
            else outcome.responder_tokens,
            reflection_tokens=TokenCounts.from_usage(receipt.get("reflection_usage")),
            warm_path=bool(memory["warm_path"]) if "warm_path" in memory else None,
            reflection_skipped_reason=skipped,
            reflection_no_repair_count=_receipt_no_repair_count(receipt),
            closure_phase_seconds=closure_phases,
            sdo_resolution=resolution if isinstance(resolution, str) else None,
            **program_metrics,
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

    def prepare_recovery(self, outcome: AgentOutcome | None) -> PostClosureWait | None:
        """Leave incident intake paused throughout harness recovery and its baseline check.

        Recovery runs once the row is over, whichever way SDO's closure ended: a closure still
        pending here is waited for until the reflection deadline, never aborted. The path that let
        recovery run is ``last_recovery_trigger``.
        """

        settings = self._settings
        state = PersistentState.load(settings.state_path)
        record = state.controllers.get(settings.namespace)
        if record is None:
            raise RuntimeError("eval: recovery requires a recorded controller")
        trigger: RecoveryTrigger = "natural_closure" if outcome is not None and outcome.incident_id else "no_incident"
        if record.pending is not None:
            trigger = self._settle_pending_closure(record)
            state.save(settings.state_path)
        self.last_recovery_trigger = trigger
        try:
            wait = pause_for_recovery(
                self._ops,
                record.control_namespace,
                incident_id=outcome.incident_id if outcome is not None and trigger != "reflection_deadline" else None,
                clock=self._clock,
                closure_open=trigger == "reflection_deadline",
            )
        except FollowupClosureTimeout as exc:
            raise RecoveryWaitTimeout(_post_closure_wait(exc.wait), str(exc)) from exc
        return _post_closure_wait(wait) if wait is not None else None

    def _settle_pending_closure(self, record: ControllerRecord) -> RecoveryTrigger:
        """Wait out a closure learning left pending, until the incident's one reflection deadline.

        A terminal failure becomes its rejected receipt.
        """

        pending = record.pending
        assert pending is not None
        started = self._clock.monotonic()
        try:
            drain_pending_incident(
                record,
                ops=self._ops,
                repository=self._settings.repository,
                drained_by=f"fastloop-recovery-{pending.incident_id}",
                clock=self._clock,
            )
        except ClosureFailedError as exc:
            record_failed_closure(pending, str(exc), self._clock.monotonic() - started)
            record.pending = None
            return "closure_failed"
        except ReflectionDeadlineError as exc:
            # SDO was observed still working past the deadline: the closure stays pending for SDO to finish.
            # Any other error (an unreadable state, a receipt failure) propagates, so recovery never
            # skips the closed-incident fence on a read error.
            logger.warning("recovering incident %s past its reflection deadline: %s", pending.incident_id, exc)
            return "reflection_deadline"
        record.pending = None
        return "natural_closure"

    def close(self) -> None:
        """The controller keeps running for the next loop; ``fastloop down`` stops it."""

    def _checkpoint(self, index: int, incident_id: str, receipt_dir: Path) -> None:
        if not self._settings.checkpoint_enabled or not self._settings.kubeconfig:
            return
        from benchmarks.sregym.adapter import kubectl

        control = self._settings.runtime_config.controller_namespace
        response = kubectl(
            ["get", "configmaps", "-o", "json"],
            namespace=control,
            kubeconfig=Path(self._settings.kubeconfig),
        )
        document = json.loads(response.stdout)
        items = document.get("items")
        if not isinstance(items, list):
            raise RuntimeError("worker ConfigMap listing omitted items")
        capture_checkpoints(CheckpointStore(self._settings.results_dir), index, incident_id, receipt_dir, items)


def _post_closure_wait(wait: FollowupClosureWait) -> PostClosureWait:
    return PostClosureWait(
        graded_incident_id=wait.graded_incident_id,
        followup_incident_ids=list(wait.followup_incident_ids),
        waited_seconds=round(wait.waited_seconds, 3),
        bound_seconds=wait.bound_seconds,
        closed=wait.closed,
    )
