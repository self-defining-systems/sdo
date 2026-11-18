"""One cell: claim a lane with a memory snapshot, inject, run the arm, grade once, collect, release.

The runner owns the clock, the independent prober, the phase timings, the evidence gate and the files. Everything
that touches a cluster, an agent or the accountant is behind a small interface, so the same state machine runs the
SDO arm, the vanilla arm and a scripted fake. A cell never rebuilds a lane or re-onboards: ``claim`` resets the warm
app and installs the snapshot, and ``deploy`` is a no-op on a warm lane.
"""

from __future__ import annotations

import dataclasses
import json
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.eval.health import Prober, ProbeResult, ProbeSample, assess_health
from benchmarks.sregym.eval.results import ResultRow, TokenRow, write_manifest
from benchmarks.sregym.eval.timings import PhaseTimings

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from benchmarks.sregym.eval.plan import Arm, Cell, EvalConfig


@dataclass(frozen=True)
class Lane:
    id: str
    app: str
    #: Backend-private facts (kubeconfig, namespace, ...), recorded in the manifest.
    info: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Injection:
    at: float


@dataclass(frozen=True)
class Verdict:
    """SREGym's one-shot mitigation oracle, read once at the end of the cell."""

    success: bool
    detail: str = ""
    harm: str | None = None


@dataclass(frozen=True)
class ArmResult:
    """What an arm reports. Times are epoch seconds; ``paged_at`` is ``None`` when no page happened."""

    paged_at: float | None
    page_source: str  # own | fallback | instruction | none
    agent_ended_at: float
    status: str = "completed"  # completed | escalated | timeout | error
    recovered_at: float | None = None
    closed_at: float | None = None
    resolution_path: str | None = None
    applied_playbooks: tuple[str, ...] = ()
    binding_sources: tuple[str, ...] = ()
    #: Head of the lane's accepted memory after the cell (SDO arm); the first-try delta is taken from it.
    memory_commit_after: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class Accounting:
    online: TokenRow
    offline: TokenRow
    #: Per-role rows, written verbatim to ``accounting.json``.
    detail: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GateVerdict:
    gradable: bool
    reasons: tuple[str, ...] = ()


class PageHook(Protocol):
    """Fallback page when SDO has not opened its own incident. Not built this round: see ``NoPageHook``."""

    def page(self, lane: Lane, cell: Cell, summary: str) -> float | None: ...


class NoPageHook:
    def page(self, lane: Lane, cell: Cell, summary: str) -> float | None:
        return None


class LaneBackend(Protocol):
    def claim(self, cell: Cell, lane_id: str, snapshot_commit: str | None) -> Lane:
        """Claim ``lane_id`` (chosen by the scheduler), reset the app to healthy, install the memory snapshot."""
        ...

    def deploy(self, lane: Lane) -> None:
        """Make sure the app is deployed; a warm lane does nothing."""
        ...

    def probe(self, lane: Lane) -> Callable[[], ProbeResult]:
        """The app's user-health probe, independent of SDO's own."""
        ...

    def inject(self, lane: Lane, cell: Cell) -> Injection: ...

    def grade(self, lane: Lane, cell: Cell) -> Verdict: ...

    def collect(self, lane: Lane, cell: Cell, directory: Path) -> None:
        """Copy everything the cell produced (transcripts, outcome, logs) into ``directory``."""
        ...

    def release(self, lane: Lane, *, reusable: bool) -> None:
        """Give the lane back; ``reusable=False`` holds it for inspection."""
        ...


@dataclass(frozen=True)
class ArmContext:
    config: EvalConfig
    page_hook: PageHook
    now: Callable[[], float]
    sleep: Callable[[float], None]


class ArmExecutor(Protocol):
    def execute(self, lane: Lane, cell: Cell, injection: Injection, directory: Path, context: ArmContext) -> ArmResult:
        """Page the agent, wait for it to finish (bounded by the cell cap), report times and tokens' inputs."""
        ...


class Accountant(Protocol):
    def account(self, cell: Cell, directory: Path, result: ArmResult | None) -> Accounting: ...


class EvidenceGate(Protocol):
    def check(self, cell: Cell, directory: Path) -> GateVerdict:
        """Refuse to grade a cell whose transcripts are missing."""
        ...


class ProberLike(Protocol):
    def start(self) -> None: ...

    def stop(self) -> list[ProbeSample]: ...

    def samples(self) -> list[ProbeSample]: ...


def default_prober(probe: Callable[[], ProbeResult], path: Path, interval: float) -> ProberLike:
    return Prober(probe, path, interval_seconds=interval)


@dataclass
class CellContext:
    config: EvalConfig
    backend: LaneBackend
    executors: Mapping[Arm, ArmExecutor]
    accountant: Accountant
    gate: EvidenceGate
    #: Commit of a snapshot key for a cell (``None`` for vanilla).
    resolve_snapshot: Callable[[Cell], str | None]
    page_hook: PageHook = field(default_factory=NoPageHook)
    prober_factory: Callable[[Callable[[], ProbeResult], Path, float], ProberLike] = default_prober
    now: Callable[[], float] = time.time
    sleep: Callable[[float], None] = time.sleep


def _write(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 3)


def run_cell(cell: Cell, ctx: CellContext, lane_id: str) -> ResultRow:
    """Run one cell to a result row. Never raises for a cell failure: the row says what happened."""
    config = ctx.config
    directory = cell.directory(config)
    directory.mkdir(parents=True, exist_ok=True)
    timings = PhaseTimings(now=ctx.now)
    lane: Lane | None = None
    prober: ProberLike | None = None
    injection: Injection | None = None
    result: ArmResult | None = None
    verdict: Verdict | None = None
    samples: list[ProbeSample] = []
    snapshot_commit: str | None = None
    error: str | None = None
    reusable = True
    try:
        snapshot_commit = ctx.resolve_snapshot(cell)
        with timings.phase("claim_reset"):
            lane = ctx.backend.claim(cell, lane_id, snapshot_commit)
        with timings.phase("deploy"):
            ctx.backend.deploy(lane)
        prober = ctx.prober_factory(ctx.backend.probe(lane), directory / "probes.jsonl", config.probe_interval_seconds)
        prober.start()
        with timings.phase("inject"):
            injection = ctx.backend.inject(lane, cell)
        arm = ArmContext(config=config, page_hook=ctx.page_hook, now=ctx.now, sleep=ctx.sleep)
        result = ctx.executors[cell.arm].execute(lane, cell, injection, directory, arm)
        if result.paged_at is not None:
            timings.record("page", injection.at, max(injection.at, result.paged_at))
            timings.record("agent", max(injection.at, result.paged_at), max(result.paged_at, result.agent_ended_at))
        else:
            timings.record("agent", injection.at, max(injection.at, result.agent_ended_at))
        with timings.phase("settle"):
            _settle(prober, ctx)
        samples = prober.stop()
        prober = None
        with timings.phase("grade"):
            verdict = ctx.backend.grade(lane, cell)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        reusable = False
    finally:
        if prober is not None:
            samples = prober.stop()
    gate = GateVerdict(gradable=False, reasons=("cell did not reach collection",))
    accounting = Accounting(TokenRow(), TokenRow())
    try:
        with timings.phase("collect"):
            if lane is not None:
                ctx.backend.collect(lane, cell, directory)
            accounting = ctx.accountant.account(cell, directory, result)
            gate = ctx.gate.check(cell, directory)
    except Exception as exc:
        error = error or f"collect: {type(exc).__name__}: {exc}"
        reusable = False
    if lane is not None:
        try:
            ctx.backend.release(lane, reusable=reusable)
        except Exception as exc:
            error = error or f"release: {type(exc).__name__}: {exc}"
    return _finish(
        cell,
        ctx,
        directory,
        timings,
        lane,
        injection,
        result,
        verdict,
        samples,
        accounting,
        gate,
        error,
        snapshot_commit,
    )


def _settle(prober: ProberLike, ctx: CellContext) -> None:
    config = ctx.config
    max_gap = 2 * config.probe_interval_seconds + 2.0
    deadline = ctx.now() + config.settle_cap_seconds
    while True:
        if (
            assess_health(prober.samples(), hold_seconds=config.hold_seconds, max_gap_seconds=max_gap).healthy_at
            is not None
        ):
            return
        if ctx.now() >= deadline:
            return
        ctx.sleep(config.probe_interval_seconds)


def _finish(
    cell: Cell,
    ctx: CellContext,
    directory: Path,
    timings: PhaseTimings,
    lane: Lane | None,
    injection: Injection | None,
    result: ArmResult | None,
    verdict: Verdict | None,
    samples: list[ProbeSample],
    accounting: Accounting,
    gate: GateVerdict,
    error: str | None,
    snapshot_commit: str | None,
) -> ResultRow:
    config = ctx.config
    max_gap = 2 * config.probe_interval_seconds + 2.0
    assessment = assess_health(samples, hold_seconds=config.hold_seconds, max_gap_seconds=max_gap)
    t_page = result.paged_at if result else None
    t_inject = injection.at if injection else None
    healthy = assessment.healthy_at
    time_to_mitigate = max(0.0, healthy - t_page) if healthy is not None and t_page is not None else None
    inject_to_healthy = max(0.0, healthy - t_inject) if healthy is not None and t_inject is not None else None
    recovered = result.recovered_at if result else None
    reason = error or (result.error if result else None)
    if error or (result and result.status == "error"):
        status = "error"
    elif not gate.gradable:
        status = "not_graded"
        reason = "; ".join(gate.reasons) or "evidence gate refused"
    else:
        status = "graded"
    timing_document = timings.to_dict()
    _write(directory / "timings.json", timing_document)
    _write(directory / "health.json", dataclasses.asdict(assessment))
    _write(
        directory / "accounting.json",
        {"online": accounting.online.model_dump(), "offline": accounting.offline.model_dump(), **accounting.detail},
    )
    _write(directory / "arm-result.json", dataclasses.asdict(result) if result else None)
    _write(directory / "verdict.json", dataclasses.asdict(verdict) if verdict else None)
    row = ResultRow(
        stage=config.stage,
        app=cell.app,
        pass_=cell.pass_,
        problem=cell.problem,
        arm=cell.arm,
        snapshot=cell.snapshot,
        snapshot_commit=snapshot_commit,
        lane=lane.id if lane else None,
        status=status,
        reason=reason,
        success=verdict.success if verdict and status == "graded" else None,
        t_inject=t_inject,
        t_page=t_page,
        page_source=result.page_source if result else "none",
        no_page=t_page is None,
        fallback_page=bool(result and result.page_source == "fallback"),
        t_healthy=healthy,
        t_recovered=recovered,
        t_closed=result.closed_at if result else None,
        inject_to_healthy_s=_round(inject_to_healthy),
        time_to_mitigate_s=_round(time_to_mitigate),
        confirmation_s=_round(recovered - healthy) if recovered is not None and healthy is not None else None,
        healthy_hold_seconds=config.hold_seconds,
        flaps=assessment.flaps,
        unhealthy_seconds=_round(assessment.unhealthy_seconds),
        resolution_path=result.resolution_path if result else None,
        applied_playbooks=",".join(result.applied_playbooks) if result else None,
        binding_sources=",".join(result.binding_sources) if result else None,
        agent_seconds=timing_document["phases"]["agent"],
        overhead_seconds=timing_document["overhead_seconds"],
        online=accounting.online,
        offline=accounting.offline,
        harm=verdict.harm if verdict else None,
        memory_commit_after=result.memory_commit_after if result else None,
    )
    _write(directory / "row.json", row.flat())
    identity = {
        "cell": cell.id,
        "stage": config.stage,
        "app": cell.app,
        "pass": cell.pass_,
        "problem": cell.problem,
        "arm": cell.arm,
        "snapshot": cell.snapshot,
        "snapshot_commit": snapshot_commit,
        "lane": dict(lane.info) | {"id": lane.id} if lane else None,
        "status": status,
        "config": {
            "hold_seconds": config.hold_seconds,
            "probe_interval_seconds": config.probe_interval_seconds,
            "after_playbook": config.after_playbook,
            "fallback_page_seconds": config.fallback_page_seconds,
            "cell_cap_seconds": config.cell_cap_seconds,
        },
    }
    row.manifest_sha256 = write_manifest(directory, identity)
    return row
