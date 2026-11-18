"""Runs a plan: lane pools per app and arm, dependency order, and a load gate instead of a fixed concurrency.

Each (app, arm) has a pool of lanes. A cell takes any free lane of its pool (the memory snapshot is installed at
claim), so the number of cells in flight is bounded by the lanes and, on top of that, by host load. The merge of an
app's pass-1 deltas is a lane-free step that pass-2 cells wait for; a pass-2 own-delta cell waits only for its own
problem's pass-1 cell. A finished cell directory with a verified manifest is skipped on restart.

A cell holds its lane through its closing turn: ``run_cell`` returns only after the closing record is written, so a
lane is never claimed again, the pass-1 delta is never taken, and the merge never starts before a cell's
``closing.json`` is terminal.
"""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import TYPE_CHECKING, Protocol

from benchmarks.sregym.eval.plan import Cell, Plan, merge_id
from benchmarks.sregym.eval.results import ResultRow, ResultsTable, verify_manifest

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from benchmarks.sregym.eval.cell import CellContext

logger = logging.getLogger(__name__)


class SnapshotStore(Protocol):
    """Memory snapshots by key; commits live in the store, lanes fetch them at claim."""

    def resolve(self, app: str, key: str) -> str:
        """Commit of ``base``, ``merged`` or ``own:<problem>``."""
        ...

    def record_first_try(self, app: str, problem: str, memory_commit_after: str | None) -> None:
        """Remember a pass-1 cell's accepted memory so its delta can be taken."""
        ...

    def merge(self, app: str, problems: Sequence[str]) -> object:
        """Fold every recorded pass-1 delta of the app into the ``merged`` snapshot; returns a report."""
        ...


class LoadGate:
    """Admit a cell start only while the load average is under ``max_load``, at most one start per spacing."""

    def __init__(
        self,
        max_load: float,
        spacing_seconds: float,
        *,
        loadavg: Callable[[], float] = lambda: os.getloadavg()[0],
        now: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._max_load = max_load
        self._spacing = spacing_seconds
        self._loadavg = loadavg
        self._now = now
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next_start = 0.0
        self.waited = 0.0

    def admit(self) -> float:
        """Block until a start is allowed; returns the seconds waited."""
        started = self._now()
        with self._lock:  # one admitter at a time, so starts are spaced and load is re-read for each
            while True:
                now = self._now()
                if now < self._next_start:
                    self._sleep(self._next_start - now)
                    continue
                if self._loadavg() >= self._max_load:
                    self._sleep(min(5.0, max(self._spacing, 1.0)))
                    continue
                self._next_start = self._now() + self._spacing
                break
        waited = self._now() - started
        self.waited += waited
        return waited


class Pools:
    """Free lanes per (app, arm). A lane a cell errored on is held for inspection and never handed out again."""

    def __init__(self, plan: Plan) -> None:
        self._free: dict[tuple[str, str], queue.Queue[str]] = {}
        self._live: dict[tuple[str, str], int] = {}
        self._lock = threading.Lock()
        self.held: list[tuple[str, str]] = []
        for app in plan.config.apps:
            for arm, names in (("sdo", app.sdo_lanes), ("vanilla", app.vanilla_lanes)):
                free: queue.Queue[str] = queue.Queue()
                for name in names:
                    free.put(name)
                self._free[(app.name, arm)] = free
                self._live[(app.name, arm)] = len(names)

    def size(self, key: tuple[str, str]) -> int:
        return self._live[key]

    def acquire(self, key: tuple[str, str]) -> str | None:
        """A free lane, waiting for one to come back; ``None`` once every lane of the pool is held."""
        while True:
            try:
                return self._free[key].get(timeout=0.5)
            except queue.Empty:
                with self._lock:
                    if self._live[key] == 0:
                        return None

    def release(self, key: tuple[str, str], lane: str, *, reusable: bool) -> None:
        if reusable:
            self._free[key].put(lane)
            return
        with self._lock:
            self._live[key] -= 1
            self.held.append((key[0], lane))


class Scheduler:
    def __init__(
        self,
        plan: Plan,
        context: CellContext,
        store: SnapshotStore,
        table: ResultsTable,
        *,
        gate: LoadGate | None = None,
    ) -> None:
        self.plan = plan
        self.store = store
        self.table = table
        self.context = context
        self.gate = gate or LoadGate(plan.config.max_load, plan.config.admit_spacing_seconds)
        self.pools = Pools(plan)
        self.done: set[str] = set()

    def _run_cell(self, cell: Cell) -> ResultRow:
        from benchmarks.sregym.eval.cell import run_cell

        lane = self.pools.acquire(cell.pool)
        row: ResultRow
        if lane is None:
            row = ResultRow(
                stage=self.plan.config.stage,
                app=cell.app,
                pass_=cell.pass_,
                problem=cell.problem,
                arm=cell.arm,
                snapshot=cell.snapshot,
                status="error",
                reason="no lane left: every lane of the pool is held",
            )
        else:
            reusable = False
            try:
                self.gate.admit()
                row = run_cell(cell, self.context, lane)
                # A lane whose closing turn was not drained (or whose drain failed) is never handed out again.
                reusable = row.status != "error" and row.closing_status not in ("not_drained", "error")
            finally:
                self.pools.release(cell.pool, lane, reusable=reusable)
        self.table.append(row)
        if cell.pass_ == "pass1":
            self.store.record_first_try(cell.app, cell.problem, row.memory_commit_after)
        return row

    def run(self) -> list[ResultRow]:
        plan = self.plan
        pools = {key: ThreadPoolExecutor(max_workers=max(1, self.pools.size(key))) for key in self.pools._free}
        merge_pool = ThreadPoolExecutor(max_workers=max(1, len(plan.config.apps)))
        pending: dict[str, Callable[[], object]] = {}
        executor_of: dict[str, ThreadPoolExecutor] = {}
        needs: dict[str, tuple[str, ...]] = {}
        for cell in plan.cells:
            needs[cell.id] = cell.needs
            if _complete(cell, plan):
                logger.info("skip finished cell %s", cell.id)
                self.done.add(cell.id)
                continue
            pending[cell.id] = lambda cell=cell: self._run_cell(cell)
            executor_of[cell.id] = pools[cell.pool]
        for app in plan.merge_steps():
            step = merge_id(app)
            pending[step] = lambda app=app: self.store.merge(app, plan.pass1_problems(app))
            executor_of[step] = merge_pool
            needs[step] = tuple(f"{app}/pass1/{p}" for p in plan.pass1_problems(app))
        rows: list[ResultRow] = []
        running: dict[Future[object], str] = {}
        try:
            while pending or running:
                for step in [s for s in pending if all(n in self.done for n in needs[s])]:
                    running[executor_of[step].submit(pending.pop(step))] = step
                if not running:
                    raise RuntimeError(f"eval: steps never became ready: {sorted(pending)}")
                finished, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in finished:
                    step = running.pop(future)
                    try:
                        outcome = future.result()
                    except Exception:
                        logger.exception("step %s failed", step)
                        self.done.add(step)  # dependents still run: a missing delta leaves their snapshot at base
                        continue
                    self.done.add(step)
                    if isinstance(outcome, ResultRow):
                        rows.append(outcome)
        finally:
            for executor in (*pools.values(), merge_pool):
                executor.shutdown(wait=True, cancel_futures=True)
            self.table.write_csv()
        return rows


def _complete(cell: Cell, plan: Plan) -> bool:
    return not verify_manifest(cell.directory(plan.config))
