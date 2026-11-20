"""Scripted stand-ins for the lane backend, arm executors, accountant, gate and prober (no cluster, no sleeps)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from benchmarks.sregym.eval.cell import (
    Accounting,
    ArmResult,
    CellContext,
    GateVerdict,
    Injection,
    Lane,
    Verdict,
)
from benchmarks.sregym.eval.closing import ClosingResult
from benchmarks.sregym.eval.health import ProbeResult, ProbeSample
from benchmarks.sregym.eval.plan import AppConfig, EvalConfig
from benchmarks.sregym.eval.results import TokenRow

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from benchmarks.sregym.eval.cell import ArmContext
    from benchmarks.sregym.eval.plan import Cell


class Clock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class FakeProber:
    """Healthy from ``healthy_from`` (epoch) onward, sampled every 3 s up to the clock."""

    def __init__(self, clock: Clock, healthy_from: float, start: float) -> None:
        self.clock, self.healthy_from, self.start_at = clock, healthy_from, start

    def start(self) -> None:
        pass

    def samples(self) -> list[ProbeSample]:
        out, at = [], self.start_at
        while at <= self.clock.now:
            out.append(ProbeSample(at=at, healthy=at >= self.healthy_from))
            at += 3
        return out

    def stop(self) -> list[ProbeSample]:
        return self.samples()


@dataclass
class FakeBackend:
    clock: Clock
    calls: list[str] = field(default_factory=list)
    released: list[tuple[str, bool]] = field(default_factory=list)
    fail_at: str | None = None
    success: bool = True
    snapshots: list[str | None] = field(default_factory=list)

    def _step(self, name: str) -> None:
        self.calls.append(name)
        if self.fail_at == name:
            raise RuntimeError(f"{name} failed")

    def claim(self, cell: Cell, lane_id: str, snapshot_commit: str | None) -> Lane:
        self.snapshots.append(snapshot_commit)
        self._step("claim")
        self.clock.sleep(2)
        return Lane(id=lane_id, app=cell.app, info={"namespace": cell.app})

    def deploy(self, lane: Lane) -> None:
        self._step("deploy")

    def probe(self, lane: Lane) -> Callable[[], ProbeResult]:
        return lambda: ProbeResult(True)

    def inject(self, lane: Lane, cell: Cell) -> Injection:
        self._step("inject")
        self.clock.sleep(1)
        return Injection(at=self.clock.now)

    def grade(self, lane: Lane, cell: Cell) -> Verdict:
        self._step("grade")
        return Verdict(success=self.success, harm="none")

    def collect(self, lane: Lane, cell: Cell, directory: Path) -> None:
        self._step("collect")
        (directory / "transcript.jsonl").write_text("{}\n")

    def release(self, lane: Lane, *, reusable: bool) -> None:
        self.released.append((lane.id, reusable))


@dataclass
class FakeExecutor:
    clock: Clock
    page_after: float | None = 5.0
    work: float = 30.0
    status: str = "completed"
    memory_after: str | None = "head-1"

    def execute(self, lane: Lane, cell: Cell, injection: Injection, directory: Path, context: ArmContext) -> ArmResult:
        paged = None if self.page_after is None else injection.at + self.page_after
        self.clock.sleep(self.work)
        end = self.clock.now
        return ArmResult(
            paged_at=paged,
            page_source="none" if paged is None else "own",
            agent_ended_at=end,
            status=self.status,
            recovered_at=end + 70,
            closed_at=end + 80,
            resolution_path="responder",
            applied_playbooks=("p1",),
            binding_sources=("finding",),
            memory_commit_after=self.memory_after,
        )


class FakeAccountant:
    def account(self, cell: Cell, directory: Path, result: ArmResult | None) -> Accounting:
        return Accounting(TokenRow(output=10, usd=1.0), TokenRow(output=4), {"roles": {"responder": 10}})


@dataclass
class FakeGate:
    gradable: bool = True

    def check(self, cell: Cell, directory: Path) -> GateVerdict:
        return GateVerdict(self.gradable, () if self.gradable else ("transcript missing: responder",))


def make_config(root: Path, **overrides: object) -> EvalConfig:
    values: dict[str, object] = {
        "stage": "t",
        "runs_root": root,
        "apps": (AppConfig(name="hotel", problems=("a", "b"), sdo_lanes=("h1", "h2"), vanilla_lanes=("v1",)),),
        "hold_seconds": 9,
        "settle_cap_seconds": 60,
    }
    return EvalConfig(**{**values, **overrides})  # type: ignore[arg-type]


def make_context(
    config: EvalConfig,
    clock: Clock,
    *,
    backend: FakeBackend | None = None,
    executor: FakeExecutor | None = None,
    gate: FakeGate | None = None,
    healthy_after_inject: float = 20.0,
) -> tuple[CellContext, FakeBackend]:
    backend = backend or FakeBackend(clock)
    executor = executor or FakeExecutor(clock)
    start = clock.now

    def factory(probe: Callable[[], ProbeResult], path: Path, interval: float) -> FakeProber:
        # Injection happens 3 s after the prober starts (2 s claim + 1 s inject in the fake backend).
        return FakeProber(clock, healthy_from=start + 3 + healthy_after_inject, start=clock.now)

    context = CellContext(
        config=config,
        backend=backend,
        executors={"sdo": executor, "vanilla": executor},
        accountant=FakeAccountant(),
        gate=gate or FakeGate(),
        resolve_snapshot=lambda cell: None if cell.snapshot is None else f"commit-{cell.snapshot}",
        prober_factory=factory,
        now=clock.time,
        sleep=clock.sleep,
    )
    return context, backend


@dataclass
class DrainingExecutor(FakeExecutor):
    """An executor whose row ends at the committed outcome and whose closing turn drains on the held lane."""

    drain_seconds: float = 600.0
    closing_status: str = "drained"
    closing_commit: str | None = "closing-head"
    error_keeps_commit: bool = False
    backend: FakeBackend | None = None
    during_drain: list[object] = field(default_factory=list)
    drain_error: str | None = None
    offline_fallback: TokenRow | None = TokenRow(
        uncached_input=1, cache_read=2, cache_write=3, output=4, requests=5, usd=0.5
    )

    def drain(self, lane: Lane, cell: Cell, directory: Path, context: ArmContext) -> ClosingResult:
        # What a runner may observe while the lane is still held: the online row is already on disk.
        self.during_drain.append(
            {
                "row_json": (directory / "row.json").is_file(),
                "manifest": (directory / "manifest.json").is_file(),
                "released": list(self.backend.released) if self.backend else None,
            }
        )
        if self.drain_error:
            raise RuntimeError(self.drain_error)
        self.clock.sleep(self.drain_seconds)
        return ClosingResult(
            status=self.closing_status,  # type: ignore[arg-type]
            incident_id="incident-1",
            closing_s=self.drain_seconds - 40,
            recovery_s=25.0,
            memory_commit_after=self.closing_commit
            if self.closing_status == "drained" or self.error_keeps_commit
            else None,
            decision="updated" if self.closing_status == "drained" else "not_drained",
            learning={"playbooks": {"added": ["p-new"]}, "commit": self.closing_commit},
            offline_fallback=self.offline_fallback,
        )
