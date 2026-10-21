"""The fast inner loop: many incidents against one warm deployment.

For every incident the agent decides when to inject (the SDO controller first
reports an all-clear baseline; a raw baseline agent injects at once) and then
resolves the incident. The loop grades the live cluster with the problem's own
deterministic oracle, recovers the fault the harness's way, lets the agent
learn (SDO reflection, measured separately), and appends one record.

There is no conductor or application rebuild or undeploy between incidents.
"""

from __future__ import annotations

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
    reflection_tokens: TokenCounts = field(default_factory=TokenCounts)
    warm_path: bool | None = None
    match_reasons: tuple[str, ...] = ()
    reflection_attempts: int | None = None
    reflection_skipped_reason: str | None = None
    closure_phase_seconds: dict[str, float] = field(default_factory=dict)
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
    stream: tuple[tuple[str, ...], ...] = ()

    def __post_init__(self) -> None:
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


def run_incidents(
    agent: IncidentAgent,
    driver: FaultDriver,
    config: LoopConfig,
    *,
    grader: DiagnosisGrader | None = None,
    diagnosis_grading: str = "off",
    grading_timeout_seconds: float = 1800.0,
    monotonic: Callable[[], float] = time.monotonic,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> list[IncidentRecord]:
    """Run ``config.incidents`` incidents, cycling through ``config.problems``."""

    records: list[IncidentRecord] = []
    if diagnosis_grading not in ("off", "deferred", "inline"):
        raise ValueError("diagnosis_grading must be off, deferred, or inline")
    if diagnosis_grading != "off" and grader is None:
        raise ValueError("diagnosis grader is required when grading is enabled")
    pool = ThreadPoolExecutor(max_workers=2) if diagnosis_grading == "deferred" else None
    pending: dict[Future[DiagnosisGrade], IncidentRecord] = {}
    try:
        for index in range(config.incidents):
            ids = config.stream[index] if config.stream else (config.problems[index % len(config.problems)],)
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
            if not recovered:
                logger.error("stopping: incident %d did not clear the recovery and learning gate", index)
                break
    finally:
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
    started = monotonic()
    problem_id = "+".join(problem_ids)
    windows: list[InjectionWindow] = []

    def inject() -> InjectionWindow:
        window = (
            driver.inject_composite(problem_ids).window  # type: ignore[attr-defined]
            if len(problem_ids) > 1
            else driver.inject(problem_ids[0])
        )
        windows.append(window)
        return window

    errors: list[str] = []
    outcome: AgentOutcome | None = None
    try:
        outcome = agent.resolve(index, problem_id, inject)
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

    oracle: OracleVerdict | None = None
    recovery_oracle: OracleVerdict | None = None
    oracle_finished_at: datetime | None = None
    recovery_finished_at: datetime | None = None
    recovery_seconds: float | None = None
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
            if len(problem_ids) > 1:
                verdicts = [driver.oracle(fault=position) for position in range(len(problem_ids))]  # type: ignore[call-arg]
                return OracleVerdict(
                    kind="composition",
                    success=all(verdict.success for verdict in verdicts),
                    details={
                        problem: verdict.model_dump() for problem, verdict in zip(problem_ids, verdicts, strict=True)
                    },
                )
            return driver.oracle()

        try:
            oracle = grade()
            if not oracle.success:
                errors.append("oracle reported failure")
        except Exception as exc:
            logger.exception("oracle failed for incident %d", index)
            errors.append(f"oracle failed: {_describe(exc)}")
        finally:
            oracle_finished_at = now()
        try:
            recovery_seconds = driver.recover()
            recovery_finished_at = now()
            recovery_oracle = grade()
            if not recovery_oracle.success:
                raise RuntimeError("recovery residue: the fault oracle still fails after harness recovery")
        except Exception as exc:
            logger.exception("fault recovery failed for incident %d", index)
            errors.append(f"fault recovery failed: {_describe(exc)}")
            recovered = False

    learning_complete = True
    if outcome is not None and outcome.terminal_error:
        learning_complete = False
    elif outcome is not None and recovered:
        try:
            outcome = agent.learn(outcome)
        except Exception as exc:
            logger.exception("learning failed for incident %d", index)
            errors.append(f"learning failed: {_describe(exc)}")
            learning_complete = False

    if outcome is not None and outcome.error:
        errors.append(outcome.error)
    fallback = windows[0] if windows else InjectionWindow(started_at=now(), finished_at=now())
    window = outcome.injection if outcome is not None else fallback
    cleanup = getattr(driver, "last_cleanup", None) if windows else None
    record = IncidentRecord(
        run_id=config.run_id,
        index=index,
        agent=agent.name,
        problem_id=problem_id,
        model=agent.model,
        injection_started_at=window.started_at,
        injection_finished_at=window.finished_at,
        oracle=oracle,
        recovery_oracle=recovery_oracle,
        diagnosis_grade=diagnosis_grade,
        oracle_finished_at=oracle_finished_at,
        recovery_finished_at=recovery_finished_at,
        fault_recovery_seconds=recovery_seconds,
        cleanup_deleted_counts=cleanup.deleted_counts if cleanup is not None else {},
        cleanup_restored=cleanup.restored if cleanup is not None else [],
        cleanup_skipped_reason=cleanup.skipped_reason if cleanup is not None else None,
        incident_wall_seconds=monotonic() - started,
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
        "reflection_tokens": outcome.reflection_tokens,
        "warm_path": outcome.warm_path,
        "match_reasons": list(outcome.match_reasons),
        "reflection_attempts": outcome.reflection_attempts,
        "reflection_skipped_reason": outcome.reflection_skipped_reason,
        "closure_phase_seconds": dict(outcome.closure_phase_seconds),
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
