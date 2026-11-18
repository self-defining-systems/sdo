"""A no-cluster dry run: scripted backend, executors, accountant and gate through the real scheduler and files."""

from __future__ import annotations

import threading
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
from benchmarks.sregym.eval.health import ProbeResult, ProbeSample
from benchmarks.sregym.eval.results import TokenRow

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from benchmarks.sregym.eval.cell import ArmContext
    from benchmarks.sregym.eval.plan import Cell


class _Clock:
    """A clock the dry run advances itself, so a dry run takes no wall time."""

    def __init__(self) -> None:
        self._now = 1_000_000.0
        self._lock = threading.Lock()

    def time(self) -> float:
        with self._lock:
            return self._now

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self._now += seconds


class _Prober:
    def __init__(self, clock: _Clock) -> None:
        self._clock = clock
        self._start = clock.time()

    def start(self) -> None:
        self._start = self._clock.time()

    def samples(self) -> list[ProbeSample]:
        at, out = self._start, []
        while at <= self._clock.time():
            out.append(ProbeSample(at=at, healthy=at >= self._start + 20))
            at += 3
        return out

    def stop(self) -> list[ProbeSample]:
        return self.samples()


@dataclass
class _Backend:
    clock: _Clock

    def claim(self, cell: Cell, lane_id: str, snapshot_commit: str | None) -> Lane:
        self.clock.sleep(1)
        return Lane(id=lane_id, app=cell.app, info={"dry_run": "true"})

    def deploy(self, lane: Lane) -> None:
        pass

    def probe(self, lane: Lane) -> Callable[[], ProbeResult]:
        return lambda: ProbeResult(True)

    def inject(self, lane: Lane, cell: Cell) -> Injection:
        self.clock.sleep(1)
        return Injection(at=self.clock.time())

    def grade(self, lane: Lane, cell: Cell) -> Verdict:
        return Verdict(success=True)

    def collect(self, lane: Lane, cell: Cell, directory: Path) -> None:
        (directory / "transcript.jsonl").write_text("{}\n", encoding="utf-8")

    def release(self, lane: Lane, *, reusable: bool) -> None:
        pass


@dataclass
class _Executor:
    clock: _Clock

    def execute(self, lane: Lane, cell: Cell, injection: Injection, directory: Path, context: ArmContext) -> ArmResult:
        self.clock.sleep(30)
        end = self.clock.time()
        sdo = cell.arm == "sdo"
        return ArmResult(
            paged_at=injection.at + 5 if sdo else injection.at,
            page_source="own" if sdo else "instruction",
            agent_ended_at=end,
            recovered_at=end + 60 if sdo else None,
            resolution_path="responder" if sdo else "vanilla",
            memory_commit_after=f"dry-{cell.problem}" if sdo else None,
        )


class _Accountant:
    def account(self, cell: Cell, directory: Path, result: ArmResult | None) -> Accounting:
        return Accounting(TokenRow(output=1), TokenRow(output=0))


class _Gate:
    def check(self, cell: Cell, directory: Path) -> GateVerdict:
        return GateVerdict(True)


@dataclass
class DryRunStore:
    """Snapshot store that mints fake commit names; records what the scheduler asked of it."""

    events: list[str] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def resolve(self, app: str, key: str) -> str:
        return f"{app}-{key}"

    def record_first_try(self, app: str, problem: str, memory_commit_after: str | None) -> None:
        with self._lock:
            self.events.append(f"first {app}/{problem}")

    def merge(self, app: str, problems: Sequence[str]) -> object:
        with self._lock:
            self.events.append(f"merge {app} ({len(problems)} problems)")
        return None


def dry_run_context(config: object) -> CellContext:
    from benchmarks.sregym.eval.plan import EvalConfig

    assert isinstance(config, EvalConfig)
    clock = _Clock()
    executor = _Executor(clock)
    return CellContext(
        config=config,
        backend=_Backend(clock),
        executors={"sdo": executor, "vanilla": executor},
        accountant=_Accountant(),
        gate=_Gate(),
        resolve_snapshot=lambda cell: None if cell.snapshot is None else f"{cell.app}-{cell.snapshot}",
        prober_factory=lambda probe, path, interval: _Prober(clock),
        now=clock.time,
        sleep=clock.sleep,
    )
