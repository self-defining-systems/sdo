"""The fast inner loop: many incidents against one warm deployment.

For every incident the agent decides when to inject (the SDO controller first
reports an all-clear baseline; a raw baseline agent injects at once) and then
resolves the incident. The loop grades the live cluster with the problem's own
deterministic oracle, recovers the fault the harness's way, lets the agent
learn (SDO reflection, measured separately), and appends one record.

There is no conductor, no LLM judge, and no application rebuild or undeploy
between incidents.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.fastloop.records import (
    AgentName,
    IncidentRecord,
    OracleVerdict,
    TokenCounts,
    append_record,
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
    diagnosis: str = ""
    mitigation: str = ""
    responder_tokens: TokenCounts = field(default_factory=TokenCounts)
    reflection_tokens: TokenCounts = field(default_factory=TokenCounts)
    warm_path: bool | None = None
    match_reasons: tuple[str, ...] = ()
    reflection_skipped_reason: str | None = None
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


@dataclass(frozen=True)
class LoopConfig:
    run_id: str
    problems: tuple[str, ...]
    incidents: int
    results_path: Path

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if not self.problems or not all(isinstance(problem, str) and problem for problem in self.problems):
            raise ValueError("at least one problem id is required")
        if self.incidents < 1:
            raise ValueError("incidents must be positive")


def _describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def run_incidents(
    agent: IncidentAgent,
    driver: FaultDriver,
    config: LoopConfig,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> list[IncidentRecord]:
    """Run ``config.incidents`` incidents, cycling through ``config.problems``."""

    records: list[IncidentRecord] = []
    try:
        for index in range(config.incidents):
            problem_id = config.problems[index % len(config.problems)]
            record, recovered = _run_one(agent, driver, config, index, problem_id, monotonic=monotonic, now=now)
            append_record(config.results_path, record)
            records.append(record)
            logger.info(
                "incident %d %s: oracle=%s wall=%.1fs error=%s",
                index,
                problem_id,
                record.oracle.success if record.oracle else None,
                record.incident_wall_seconds or 0.0,
                record.error,
            )
            if not recovered:
                logger.error("stopping: the fault of incident %d could not be recovered", index)
                break
    finally:
        agent.close()
    return records


def _run_one(
    agent: IncidentAgent,
    driver: FaultDriver,
    config: LoopConfig,
    index: int,
    problem_id: str,
    *,
    monotonic: Callable[[], float],
    now: Callable[[], datetime],
) -> tuple[IncidentRecord, bool]:
    started = monotonic()
    windows: list[InjectionWindow] = []

    def inject() -> InjectionWindow:
        window = driver.inject(problem_id)
        windows.append(window)
        return window

    errors: list[str] = []
    outcome: AgentOutcome | None = None
    try:
        outcome = agent.resolve(index, problem_id, inject)
    except Exception as exc:
        logger.exception("incident %d failed", index)
        errors.append(_describe(exc))

    oracle: OracleVerdict | None = None
    recovery_seconds: float | None = None
    recovered = True
    if windows:
        try:
            oracle = driver.oracle()
        except Exception as exc:
            logger.exception("oracle failed for incident %d", index)
            errors.append(f"oracle failed: {_describe(exc)}")
        try:
            recovery_seconds = driver.recover()
        except Exception as exc:
            logger.exception("fault recovery failed for incident %d", index)
            errors.append(f"fault recovery failed: {_describe(exc)}")
            recovered = False

    if outcome is not None and recovered:
        try:
            outcome = agent.learn(outcome)
        except Exception as exc:
            logger.exception("learning failed for incident %d", index)
            errors.append(f"learning failed: {_describe(exc)}")

    if outcome is not None and outcome.error:
        errors.append(outcome.error)
    fallback = windows[0] if windows else InjectionWindow(started_at=now(), finished_at=now())
    window = outcome.injection if outcome is not None else fallback
    record = IncidentRecord(
        run_id=config.run_id,
        index=index,
        agent=agent.name,
        problem_id=problem_id,
        model=agent.model,
        injection_started_at=window.started_at,
        injection_finished_at=window.finished_at,
        oracle=oracle,
        fault_recovery_seconds=recovery_seconds,
        incident_wall_seconds=monotonic() - started,
        error="; ".join(errors) or None,
        **(_outcome_fields(outcome) if outcome is not None else {}),
    )
    return record, recovered


def _outcome_fields(outcome: AgentOutcome) -> dict[str, Any]:
    return {
        "detected_at": outcome.detected_at,
        "mitigation_applied_at": outcome.mitigation_applied_at,
        "resolved_at": outcome.resolved_at,
        "diagnosis": outcome.diagnosis,
        "mitigation": outcome.mitigation,
        "responder_tokens": outcome.responder_tokens,
        "reflection_tokens": outcome.reflection_tokens,
        "warm_path": outcome.warm_path,
        "match_reasons": list(outcome.match_reasons),
        "reflection_skipped_reason": outcome.reflection_skipped_reason,
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
    }
