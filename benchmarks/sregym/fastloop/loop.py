"""The fast inner loop: many incidents against one warm deployment.

For every incident the agent decides when to inject (the SDO controller first
reports an all-clear baseline; a raw baseline agent injects at once) and then
resolves the incident. The loop grades the live cluster with the problem's own
deterministic oracle, recovers the fault the harness's way, lets the agent
learn (SDO reflection, measured separately), and appends one record.

There is no conductor or application rebuild or undeploy between incidents.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.fastloop.records import (
    AgentName,
    DiagnosisGrade,
    IncidentRecord,
    OracleVerdict,
    TokenCounts,
    append_record,
    write_records,
)
from benchmarks.sregym.faults.description import SuiteCapabilities
from benchmarks.sregym.protocol.injection import InvalidInjectionError

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InjectionWindow:
    started_at: datetime
    finished_at: datetime

    def __post_init__(self) -> None:
        if self.started_at.tzinfo is None or self.finished_at.tzinfo is None:
            raise ValueError("injection timestamps must be timezone-aware")
        if self.finished_at < self.started_at:
            raise ValueError("injection cannot finish before it starts")


@dataclass(frozen=True)
class AgentOutcome:
    """What an agent reports for one incident; learning fields arrive after recovery."""

    injection: InjectionWindow
    detected_at: datetime | None = None
    mitigation_applied_at: datetime | None = None
    resolved_at: datetime | None = None
    responder_started_at: datetime | None = None
    responder_finished_at: datetime | None = None
    reflection_started_at: datetime | None = None
    reflection_finished_at: datetime | None = None
    rebuild_done_at: datetime | None = None
    diagnosis: str = ""
    mitigation: str = ""
    responder_tokens: TokenCounts = field(default_factory=TokenCounts)
    drift_tokens: TokenCounts = field(default_factory=TokenCounts)
    reflection_tokens: TokenCounts = field(default_factory=TokenCounts)
    warm_path: bool | None = None
    match_reasons: tuple[str, ...] = ()
    binding_sources: tuple[str, ...] = ()
    reflection_attempts: int | None = None
    reflection_skipped_reason: str | None = None
    reflection_evidence_gap_count: int | None = None
    resolution_path: str | None = None
    fast_path_programs: tuple[str, ...] = ()
    fast_path_relation_covered_findings: tuple[dict[str, Any], ...] = ()
    fast_path_skipped_reason: str | None = None
    closure_phase_seconds: dict[str, float] = field(default_factory=dict)
    driver_phase_timings_seconds: dict[str, float] = field(default_factory=dict)
    fault_gate_timings_seconds: dict[str, float] = field(default_factory=dict)
    #: How the strict receipt says the incident closed (``sdo_mitigated``, ``external_recovery`` or
    #: ``cleared_without_sdo_action``); ``None`` when no receipt validated. Only ``sdo_mitigated`` is SDO's own fix.
    sdo_resolution: str | None = None
    reflection_seconds: float | None = None
    baseline_gate_seconds: float | None = None
    #: One-time setup paid inside this incident (lifecycle and controller install), excluded from resolution.
    setup_seconds: float | None = None
    controller_installed: bool | None = None
    lifecycle_reused: bool | None = None
    #: How lifecycle validation was satisfied when the opt-in validation cache is on.
    lifecycle_validation_source: str | None = None
    #: Waiting for the previous incident's reflection before this one could start.
    previous_reflection_drain_seconds: float | None = None
    incident_id: str | None = None
    artifacts_dir: str | None = None
    program_diagnoses: int | None = None
    program_verdicts: dict[str, int] = field(default_factory=dict)
    program_tool_calls: int | None = None
    tool_commands: dict[str, int] = field(default_factory=dict)
    response_loops_used: int | None = None
    terminal_error: bool = False
    #: A non-fatal problem the agent reports alongside its evidence (for example a rejected receipt).
    error: str | None = None

    def with_learning(self, **updates: object) -> AgentOutcome:
        return replace(self, **updates)  # type: ignore[arg-type]


class FaultDriver(Protocol):
    def inject(self, problem_id: str) -> InjectionWindow: ...

    def oracle(self) -> OracleVerdict: ...

    def recover(self) -> float: ...


class IncidentAgent(Protocol):
    @property
    def name(self) -> AgentName: ...

    @property
    def model(self) -> str: ...

    def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome: ...

    def learn(self, outcome: AgentOutcome) -> AgentOutcome: ...

    def close(self) -> None: ...


class DiagnosisGrader(Protocol):
    def grade(self, problem_ids: tuple[str, ...], diagnosis: str) -> DiagnosisGrade: ...


@dataclass(frozen=True)
class LoopConfig:
    run_id: str
    problems: tuple[str, ...]
    incidents: int
    results_path: Path
    delivery_record_path: Path | None = None
    image_delivery: str = "unknown"
    image_delivery_reason: str = ""
    stream: tuple[tuple[str, ...], ...] = ()
    continue_on_error: bool = False
    slot_offset: int = 0
    run_prelude_phase_seconds: dict[str, float] = field(default_factory=dict)
    capability_evidence: SuiteCapabilities = field(default_factory=SuiteCapabilities)
    permissive_responder: bool = False
    responder_namespaces: tuple[str, ...] = ()
    cleanup_passes: int = 1

    def __post_init__(self) -> None:
        if type(self.cleanup_passes) is not int or not 1 <= self.cleanup_passes <= 2:
            raise ValueError("cleanup_passes must be 1 or 2")
        if not isinstance(self.slot_offset, int) or self.slot_offset < 0:
            raise ValueError("slot_offset must be a non-negative integer")
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if not self.problems or not all(isinstance(problem, str) and problem for problem in self.problems):
            raise ValueError("at least one problem id is required")
        if self.incidents < 1:
            raise ValueError("incidents must be positive")
        if self.stream and self.incidents != len(self.stream):
            raise ValueError("incidents must equal stream length")


def _describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def write_loop_manifest(config: LoopConfig) -> None:
    """Persist benchmark capability policy before a worker or agent starts."""
    config.results_path.parent.mkdir(parents=True, exist_ok=True)
    config.results_path.with_name("run_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "sdo.fastloop-run/v1",
                "run_id": config.run_id,
                "fault_capabilities": config.capability_evidence.model_dump(mode="json"),
                "permissive_responder": config.permissive_responder,
                "responder_namespaces": list(config.responder_namespaces),
                "cleanup_passes": config.cleanup_passes,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def run_incidents(
    agent: IncidentAgent,
    driver: FaultDriver,
    config: LoopConfig,
    *,
    grader: DiagnosisGrader | None = None,
    diagnosis_grading: str = "off",
    grading_timeout_seconds: float = 1800.0,
    on_failed_incident: Callable[[], None] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> list[IncidentRecord]:
    """Run ``config.incidents`` incidents, cycling through ``config.problems``."""

    records: list[IncidentRecord] = []
    if diagnosis_grading not in ("off", "deferred", "inline"):
        raise ValueError("diagnosis_grading must be off, deferred, or inline")
    if diagnosis_grading != "off" and grader is None:
        raise ValueError("diagnosis grader is required when grading is enabled")
    write_loop_manifest(config)
    config.results_path.with_name("run_metadata.json").write_text(
        json.dumps(
            {
                "run_id": config.run_id,
                "token_accounting_version": "responder-drift/v2",
                "drift_tokens": "legacy-zero",
                "drift_reasoning_charged_to": "responder_tokens",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    pool = ThreadPoolExecutor(max_workers=2) if diagnosis_grading == "deferred" else None
    pending: dict[Future[DiagnosisGrade], IncidentRecord] = {}
    stop_reason = "completed"
    try:
        for local_index in range(config.incidents):
            index = config.slot_offset + local_index
            ids = (
                config.stream[local_index] if config.stream else (config.problems[local_index % len(config.problems)],)
            )
            record, recovered = _run_one(
                agent,
                driver,
                config,
                index,
                ids,
                monotonic=monotonic,
                now=now,
                grader=grader if diagnosis_grading == "inline" else None,
            )
            baseline_ready = recovered or (record.recovery_oracle is not None and record.recovery_oracle.success)
            if (
                config.continue_on_error
                and record.error
                and record.recovery_error is None
                and record.infra_error is None
                and local_index + 1 < config.incidents
                and on_failed_incident is not None
            ):
                try:
                    on_failed_incident()
                    baseline_ready = True
                except Exception as exc:
                    logger.exception("reset failed after incident %d", index)
                    record.error = f"{record.error}; reset failed: {_describe(exc)}"
                    baseline_ready = False
            record.capability_mode = config.capability_evidence.mode
            record.fault_capabilities = [f for f in config.capability_evidence.faults if f.problem_id in ids]
            append_record(config.results_path, record)
            records.append(record)
            if pool is not None and grader is not None:
                if record.diagnosis.strip():
                    pending[pool.submit(grader.grade, ids, record.diagnosis)] = record
                else:
                    record.diagnosis_grade = DiagnosisGrade(skipped=True, skip_reason="no diagnosis text")
            logger.info(
                "incident %d %s: oracle=%s wall=%.1fs error=%s",
                index,
                record.problem_id,
                record.oracle.success if record.oracle else None,
                record.incident_wall_seconds or 0.0,
                record.error,
            )
            if (
                record.recovery_error
                or (not config.continue_on_error and not recovered)
                or (config.continue_on_error and not baseline_ready)
            ):
                stop_reason = f"incident {index} did not clear the recovery and learning gate"
                logger.error("stopping: incident %d did not clear the recovery and learning gate", index)
                break
    except BaseException as exc:
        stop_reason = _describe(exc)
        logger.exception("run %s aborted: %s", config.run_id, stop_reason)
        raise
    finally:
        try:
            agent.close()
            if pool is not None:
                try:
                    done, outstanding = wait(pending, timeout=grading_timeout_seconds)
                    for future in done:
                        record = pending[future]
                        try:
                            record.diagnosis_grade = future.result()
                        except Exception as exc:
                            record.diagnosis_grade = DiagnosisGrade(error=_describe(exc))
                    for future in outstanding:
                        pending[future].diagnosis_grade = DiagnosisGrade(error="diagnosis grading timed out")
                        future.cancel()
                    if records:
                        write_records(config.results_path, records)
                finally:
                    pool.shutdown(wait=False, cancel_futures=True)
        except BaseException as exc:
            stop_reason = f"cleanup failed: {_describe(exc)}"
            logger.exception("run %s %s", config.run_id, stop_reason)
            raise
        finally:
            logger.info(
                "run_end run_id=%s completed=%d requested=%d reason=%s",
                config.run_id,
                len(records),
                config.incidents,
                stop_reason,
            )
    return records


def _run_one(
    agent: IncidentAgent,
    driver: FaultDriver,
    config: LoopConfig,
    index: int,
    problem_ids: tuple[str, ...],
    *,
    monotonic: Callable[[], float],
    now: Callable[[], datetime],
    grader: DiagnosisGrader | None = None,
) -> tuple[IncidentRecord, bool]:
    if not problem_ids:
        raise ValueError("an incident requires at least one problem ID")
    first_problem = problem_ids[0]
    started = monotonic()
    problem_id = "+".join(problem_ids)
    windows: list[InjectionWindow] = []

    def inject() -> InjectionWindow:
        window = (
            driver.inject_composite(problem_ids).window  # type: ignore[attr-defined]
            if len(problem_ids) > 1
            else driver.inject(first_problem)
        )
        windows.append(window)
        return window

    errors: list[str] = []
    outcome: AgentOutcome | None = None
    try:
        outcome = agent.resolve(index, problem_id, inject)
    except InvalidInjectionError as exc:
        cleanup_error = exc.cleanup_error
        recovered = False
        recovery_seconds = None
        try:
            recovery_seconds = driver.recover()
            recovered = cleanup_error is None
        except Exception as recovery_exc:
            cleanup_error = f"invalid injection cleanup failed: {_describe(recovery_exc)}"
        stamp = now()
        start = exc.reply.get("started_at")
        end = exc.reply.get("finished_at")
        return IncidentRecord(
            run_id=config.run_id,
            index=index,
            agent=agent.name,
            problem_id=problem_id,
            model=agent.model,
            injection_started_at=datetime.fromtimestamp(start, tz=timezone.utc)
            if isinstance(start, (int, float))
            else stamp,
            injection_finished_at=datetime.fromtimestamp(end, tz=timezone.utc)
            if isinstance(end, (int, float))
            else stamp,
            fault_components=getattr(driver, "last_components", []),
            infra_error=exc.infra_error,
            recovery_error=cleanup_error,
            injection_evidence=exc.details,
            error=cleanup_error,
            recovery_finished_at=now() if recovered else None,
            fault_recovery_seconds=recovery_seconds,
            incident_wall_seconds=monotonic() - started,
        ), recovered
    except KeyboardInterrupt:
        if windows:
            try:
                driver.recover()
                if not driver.oracle().success:
                    logger.error("fault oracle still fails after interrupt recovery for incident %d", index)
            except Exception:
                logger.exception("fault recovery failed after interrupt for incident %d", index)
        raise
    except Exception as exc:
        logger.exception("incident %d failed", index)
        errors.append(_describe(exc))

    learning_complete = outcome is None or not outcome.terminal_error
    if outcome is not None:
        try:
            outcome = agent.learn(outcome)
        except Exception as exc:
            logger.exception("learning failed for incident %d", index)
            errors.append(f"learning failed: {_describe(exc)}")
            learning_complete = False

    oracle: OracleVerdict | None = None
    infra_error: str | None = None
    recovery_oracle: OracleVerdict | None = None
    oracle_finished_at: datetime | None = None
    recovery_finished_at: datetime | None = None
    recovery_seconds: float | None = None
    cleanup_passes_verified = 0
    recovery_error: str | None = None
    recovered = bool(windows)
    diagnosis_grade: DiagnosisGrade | None = None
    if windows and grader is not None and outcome is not None and outcome.diagnosis.strip():
        try:
            diagnosis_grade = grader.grade(problem_ids, outcome.diagnosis if outcome else "")
        except Exception as exc:
            diagnosis_grade = DiagnosisGrade(error=_describe(exc))
    elif windows and grader is not None:
        diagnosis_grade = DiagnosisGrade(skipped=True, skip_reason="no diagnosis text")
    if windows:

        def grade() -> OracleVerdict:
            # The official aggregate oracle owns collateral/traffic policy. A submitted
            # problem may itself contribute multiple components; positions are not IDs.
            return driver.oracle()

        try:
            oracle = grade()
            raw_infra = oracle.details.get("infra_error")
            infra_error = raw_infra if isinstance(raw_infra, str) and raw_infra.strip() else None
            if not oracle.success and infra_error is None:
                errors.append("oracle reported failure")
        except Exception as exc:
            logger.exception("oracle failed for incident %d", index)
            infra_error = f"oracle failed: {_describe(exc)}"
            errors.append(infra_error)
        finally:
            oracle_finished_at = now()
        try:
            recovery_seconds = 0.0
            for _ in range(config.cleanup_passes):
                recovery_seconds += driver.recover()
                # Seed cleanup can remove injected workloads; grade the recovered baseline.
                recovery_oracle = getattr(driver, "recovery_oracle", lambda: None)()
                if recovery_oracle is None:
                    recovery_oracle = grade()
                if not recovery_oracle.success:
                    raise RuntimeError("recovery residue: the fault oracle still fails after harness recovery")
                full_cleanup = getattr(driver, "last_full_cleanup_verified", False)
                if config.cleanup_passes > 1 and not full_cleanup:
                    raise RuntimeError("repeated cleanup requires verified full seed recovery on every pass")
                if full_cleanup:
                    cleanup_passes_verified += 1
            recovery_finished_at = now()
        except Exception as exc:
            logger.exception("fault recovery failed for incident %d", index)
            recovery_error = f"fault recovery failed: {_describe(exc)}"
            errors.append(recovery_error)
            recovered = False

    if outcome is not None and outcome.error:
        errors.append(outcome.error)
    fallback = windows[0] if windows else InjectionWindow(started_at=now(), finished_at=now())
    window = outcome.injection if outcome is not None else fallback
    cleanup = getattr(driver, "last_cleanup", None) if windows else None
    delivery, reason = config.image_delivery, config.image_delivery_reason
    if config.delivery_record_path is not None and config.delivery_record_path.is_file():
        from benchmarks.sregym.fastloop.image_delivery import delivery_state

        state = delivery_state(config.delivery_record_path)
        if state:
            delivery, reason = state["path"], state["reason"]
    record = IncidentRecord(
        image_delivery=delivery,
        image_delivery_reason=reason,
        run_id=config.run_id,
        index=index,
        agent=agent.name,
        problem_id=problem_id,
        model=agent.model,
        injection_started_at=window.started_at,
        injection_finished_at=window.finished_at,
        fault_components=getattr(driver, "last_components", []),
        infra_error=infra_error,
        oracle=oracle,
        recovery_oracle=recovery_oracle,
        recovery_error=recovery_error,
        diagnosis_grade=diagnosis_grade,
        oracle_finished_at=oracle_finished_at,
        recovery_finished_at=recovery_finished_at,
        fault_recovery_seconds=recovery_seconds,
        cleanup_deleted_counts=cleanup.deleted_counts if cleanup is not None else {},
        cleanup_restored=cleanup.restored if cleanup is not None else [],
        cleanup_skipped_reason=cleanup.skipped_reason if cleanup is not None else None,
        full_cleanup_verified=getattr(driver, "last_full_cleanup_verified", False) and recovery_error is None,
        cleanup_passes_verified=cleanup_passes_verified,
        incident_wall_seconds=monotonic() - started,
        run_prelude_phase_seconds=config.run_prelude_phase_seconds if index == config.slot_offset else {},
        error="; ".join(errors) or None,
        **(_outcome_fields(outcome) if outcome is not None else {}),
    )
    return record, recovered and learning_complete


def _outcome_fields(outcome: AgentOutcome) -> dict[str, Any]:
    return {
        "detected_at": outcome.detected_at,
        "mitigation_applied_at": outcome.mitigation_applied_at,
        "resolved_at": outcome.resolved_at,
        "responder_started_at": outcome.responder_started_at,
        "responder_finished_at": outcome.responder_finished_at,
        "reflection_started_at": outcome.reflection_started_at,
        "reflection_finished_at": outcome.reflection_finished_at,
        "rebuild_done_at": outcome.rebuild_done_at,
        "diagnosis": outcome.diagnosis,
        "mitigation": outcome.mitigation,
        "responder_tokens": outcome.responder_tokens,
        "drift_tokens": outcome.drift_tokens,
        "reflection_tokens": outcome.reflection_tokens,
        "warm_path": outcome.warm_path,
        "match_reasons": list(outcome.match_reasons),
        "binding_sources": list(outcome.binding_sources),
        "reflection_attempts": outcome.reflection_attempts,
        "reflection_skipped_reason": outcome.reflection_skipped_reason,
        "reflection_evidence_gap_count": outcome.reflection_evidence_gap_count,
        "resolution_path": outcome.resolution_path,
        "fast_path_programs": list(outcome.fast_path_programs),
        "fast_path_relation_covered_findings": list(outcome.fast_path_relation_covered_findings),
        "fast_path_skipped_reason": outcome.fast_path_skipped_reason,
        "closure_phase_seconds": dict(outcome.closure_phase_seconds),
        "driver_phase_timings_seconds": dict(outcome.driver_phase_timings_seconds),
        "fault_gate_timings_seconds": dict(outcome.fault_gate_timings_seconds),
        "sdo_resolution": outcome.sdo_resolution,
        "reflection_seconds": outcome.reflection_seconds,
        "baseline_gate_seconds": outcome.baseline_gate_seconds,
        "setup_seconds": outcome.setup_seconds,
        "controller_installed": outcome.controller_installed,
        "lifecycle_reused": outcome.lifecycle_reused,
        "lifecycle_validation_source": outcome.lifecycle_validation_source,
        "previous_reflection_drain_seconds": outcome.previous_reflection_drain_seconds,
        "incident_id": outcome.incident_id,
        "artifacts_dir": outcome.artifacts_dir,
        "program_diagnoses": outcome.program_diagnoses,
        "program_verdicts": outcome.program_verdicts,
        "program_tool_calls": outcome.program_tool_calls,
        "tool_commands": outcome.tool_commands,
        "response_loops_used": outcome.response_loops_used,
    }
