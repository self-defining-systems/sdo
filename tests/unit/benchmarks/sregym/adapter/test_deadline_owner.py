"""One owner for eval deadlines: a spent budget is a typed failure, never a near-zero subprocess timeout."""

from __future__ import annotations

import ast
import stat
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from benchmarks.sregym.adapter import Clock
from benchmarks.sregym.adapter.budgets import Deadline, DeadlineExhaustedError, WaitKind, wait_for
from benchmarks.sregym.adapter.persistent import (
    ControllerStalledError,
    PersistentControllerError,
    _wait_for_verified_incident,
)

ROOT = Path(__file__).resolve().parents[5]
SCANNED = ("benchmarks/sregym/fastloop", "benchmarks/sregym/adapter", "benchmarks/sregym/protocol")
OWNER = "benchmarks/sregym/adapter/budgets.py"
TIMEOUT_NAMES = {"timeout", "timeout_seconds"}


class _ManualClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


def test_step_timeout_never_falls_below_its_own_floor() -> None:
    clock = _ManualClock()
    budget = Deadline.after(10, "stage", clock.monotonic)
    clock.now += 9.9999
    assert budget.step_timeout("observe", floor=WaitKind.POLL, cap=WaitKind.COMMAND) == wait_for(WaitKind.POLL)
    clock.now += 500
    assert budget.step_timeout("observe", floor=WaitKind.POLL, cap=WaitKind.COMMAND) == wait_for(WaitKind.POLL)


def test_step_timeout_is_the_budget_left_within_its_cap() -> None:
    clock = _ManualClock()
    budget = Deadline.after(100, "stage", clock.monotonic)
    assert budget.step_timeout("observe", floor=WaitKind.POLL, cap=WaitKind.COMMAND) == wait_for(WaitKind.COMMAND)
    clock.now += 85
    assert budget.step_timeout("observe", floor=WaitKind.POLL, cap=WaitKind.COMMAND) == pytest.approx(15)


def test_bounded_timeout_refuses_to_start_a_step_the_budget_cannot_hold() -> None:
    clock = _ManualClock()
    budget = Deadline.after(15, "setup fingerprint", clock.monotonic)
    clock.now += 14.5
    with pytest.raises(DeadlineExhaustedError, match=r"setup fingerprint budget exhausted: git needs at least 1s"):
        budget.bounded_timeout("git", minimum=WaitKind.POLL_FAST)
    clock.now -= 10
    assert budget.bounded_timeout("git", minimum=WaitKind.POLL_FAST) == pytest.approx(10.5)


def test_remaining_is_typed_and_names_the_budget() -> None:
    clock = _ManualClock()
    budget = Deadline.after(5, "helper retirement", clock.monotonic)
    clock.now += 5
    with pytest.raises(DeadlineExhaustedError, match="helper retirement budget exhausted"):
        budget.remaining()


def test_a_cap_below_the_floor_is_rejected() -> None:
    with pytest.raises(ValueError, match="below its floor"):
        Deadline.after(5, "stage").step_timeout("observe", floor=WaitKind.COMMAND, cap=WaitKind.POLL)


@dataclass
class _SlowStateOps:
    """A cluster whose state read consumes the whole stage budget, as a loaded kubectl does."""

    clock: _ManualClock
    reads: list[str] = field(default_factory=list)

    def runtime_state(self, control: str) -> dict[str, Any]:
        self.reads.append(control)
        self.clock.now += 31.0
        return {"last_acknowledged_incident_id": ""}


def test_closure_observation_after_a_slow_state_read_runs_the_real_operator_with_its_own_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stage whose budget a state read spent still observes the controller; it does not time out at 0.001 s.

    The operator here is a real subprocess that takes 0.3 s: the timeout that reaches it is the only thing
    that can fail the read.
    """
    operator = tmp_path / "sdo-operator"
    operator.write_text(
        textwrap.dedent("""\
        #!/bin/sh
        cat >/dev/null
        sleep 0.3
        echo '{}'
        """)
    )
    operator.chmod(operator.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("SDO_OPERATOR_BIN", str(operator))
    manual = _ManualClock()
    ops = _SlowStateOps(manual)
    clock = Clock(monotonic=manual.monotonic, sleep=lambda _s: None)

    with pytest.raises(PersistentControllerError) as raised:
        _wait_for_verified_incident(ops, "astro-sdo", set(), 30.0, clock)  # type: ignore[arg-type]

    message = str(raised.value)
    assert "timed out" not in message
    assert "could not run" not in message
    assert "verified no new incident within 30s" in message
    assert ops.reads == ["astro-sdo"]


@dataclass
class _FrozenControllerOps:
    """A controller that finished its responder (failed) and then stopped evaluating, as seen live."""

    clock: _ManualClock
    iteration: int
    advance: bool = False
    reads: int = 0

    def runtime_state(self, control: str) -> dict[str, Any]:
        self.reads += 1
        if self.advance:
            self.iteration += 1
        return {
            "incident_open": True,
            "responder_done": True,
            "dispatch_state": "DISPATCH_STATE_COMPLETED",
            "evaluation_iteration": self.iteration,
            "incident_request": {"incident_id": "incident-frozen"},
            "incident_result": {"incident_id": "incident-frozen", "status": "failed"},
            "busy_until": "2099-01-01T00:00:00Z",
        }


def _sleeping_clock(manual: _ManualClock) -> Clock:
    def sleep(seconds: float) -> None:
        manual.now += seconds

    return Clock(monotonic=manual.monotonic, sleep=sleep)


def test_a_controller_that_stopped_evaluating_ends_the_wait_with_a_typed_stall() -> None:
    """The row ends within the stall bound, not after the 2400 s verification budget."""
    manual = _ManualClock()
    started = manual.now
    ops = _FrozenControllerOps(manual, iteration=794)

    with pytest.raises(ControllerStalledError) as raised:
        _wait_for_verified_incident(ops, "hotel-sdo", set(), 2400.0, _sleeping_clock(manual))  # type: ignore[arg-type]

    message = str(raised.value)
    assert "controller stalled" in message
    assert "incident 'incident-frozen' is open, responder status 'failed'" in message
    assert manual.now - started <= wait_for(WaitKind.SLOW_COMMAND) + wait_for(WaitKind.MINUTE)


def test_a_controller_that_keeps_evaluating_keeps_the_full_verification_budget() -> None:
    manual = _ManualClock()
    ops = _FrozenControllerOps(manual, iteration=794, advance=True)

    with pytest.raises(PersistentControllerError) as raised:
        _wait_for_verified_incident(ops, "hotel-sdo", set(), 600.0, _sleeping_clock(manual))  # type: ignore[arg-type]

    assert not isinstance(raised.value, ControllerStalledError)
    assert "verified no new incident within 600s" in str(raised.value)


def _clock_subtractions(node: ast.AST) -> bool:
    for sub in ast.walk(node):
        if isinstance(sub, ast.BinOp) and isinstance(sub.op, ast.Sub):
            for side in ast.walk(sub):
                if isinstance(side, ast.Call) and ast.unparse(side.func).endswith(("monotonic", "perf_counter")):
                    return True
    return False


def hand_rolled_timeouts(source: str) -> list[tuple[int, str]]:
    """Timeouts computed as ``deadline - clock()`` at the call site, and ``max(<1 s literal>, ...)`` floors."""
    found: list[tuple[int, str]] = []
    tree = ast.parse(source)
    for function in [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
        derived: set[str] = set()
        for node in ast.walk(function):
            if isinstance(node, ast.Assign) and _clock_subtractions(node.value):
                derived.update(t.id for t in node.targets if isinstance(t, ast.Name))
            elif isinstance(node, ast.NamedExpr) and _clock_subtractions(node.value):
                derived.add(node.target.id)
        for node in ast.walk(function):
            if not isinstance(node, ast.Call):
                continue
            for keyword in node.keywords:
                if keyword.arg not in TIMEOUT_NAMES:
                    continue
                names = {n.id for n in ast.walk(keyword.value) if isinstance(n, ast.Name)}
                if _clock_subtractions(keyword.value) or names & derived:
                    found.append((keyword.value.lineno, f"{keyword.arg}={ast.unparse(keyword.value)}"))
            if ast.unparse(node.func) == "max" and any(
                isinstance(a, ast.Constant) and isinstance(a.value, (int, float)) and 0 < a.value < 1 for a in node.args
            ):
                found.append((node.lineno, ast.unparse(node)))
    return sorted(set(found))


def test_the_scanner_sees_each_hand_rolled_timeout_shape() -> None:
    source = textwrap.dedent("""\
        def direct(run, deadline, clock):
            run(timeout=deadline - clock.monotonic())

        def derived(run, deadline, clock):
            remaining = max(0.001, deadline - clock.monotonic())
            run(timeout=min(30.0, remaining))

        def walrus(run, deadline, monotonic):
            while (left := deadline - monotonic()) > 0:
                run(timeout=left)

        def owned(run, budget):
            run(timeout=budget.step_timeout("x", floor=1))
        """)
    lines = [line for line, _ in hand_rolled_timeouts(source)]
    assert lines == [2, 5, 6, 10]


def test_no_eval_timeout_is_computed_from_a_clock_outside_the_deadline_owner() -> None:
    """Every eval subprocess, request or queue timeout derived from a budget comes from ``Deadline``."""
    offenders = []
    for directory in SCANNED:
        for path in sorted((ROOT / directory).rglob("*.py")):
            relative = path.relative_to(ROOT).as_posix()
            if relative == OWNER:
                continue
            offenders += [f"{relative}:{line}: {text}" for line, text in hand_rolled_timeouts(path.read_text())]
    assert not offenders, "route these through budgets.Deadline:\n" + "\n".join(offenders)
