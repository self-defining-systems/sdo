"""Fault injection, grading, and recovery through the SREGym fast-loop worker, one fault or a composition."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.fastloop.loop import InjectionWindow
from benchmarks.sregym.fastloop.records import OracleVerdict

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

HEALTH_POLL_SECONDS = 2.0
#: The oracle waits up to 60 s for rollouts itself; injection and recovery wait on rollouts too.
WORKER_OP_TIMEOUT_SECONDS = 900.0


class FastloopFaultError(RuntimeError):
    """Raised when a fault cannot be recovered to a healthy application."""


class WorkerRequester(Protocol):
    def request(self, op: str, *, timeout: float = ..., **args: Any) -> dict[str, Any]: ...


def _utc(epoch: object) -> datetime:
    if not isinstance(epoch, (int, float)):
        raise FastloopFaultError(f"worker returned a non-numeric timestamp {epoch!r}")
    return datetime.fromtimestamp(float(epoch), tz=timezone.utc)


def _window(reply: dict[str, Any]) -> InjectionWindow:
    return InjectionWindow(started_at=_utc(reply.get("started_at")), finished_at=_utc(reply.get("finished_at")))


@dataclass(frozen=True)
class InjectedFault:
    problem_id: str
    #: The fault's position in its composition; ``recover_fault`` takes it.
    index: int
    window: InjectionWindow


@dataclass(frozen=True)
class CompositeInjection:
    faults: tuple[InjectedFault, ...]

    def __post_init__(self) -> None:
        if not self.faults:
            raise ValueError("a composite injection has at least one fault")

    @property
    def window(self) -> InjectionWindow:
        """From the first fault's start to the last fault's end."""

        return InjectionWindow(
            started_at=self.faults[0].window.started_at, finished_at=self.faults[-1].window.finished_at
        )


class SregymFaultDriver:
    def __init__(
        self,
        worker: WorkerRequester,
        *,
        namespace: str,
        health_timeout_seconds: float = 300.0,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if health_timeout_seconds <= 0:
            raise ValueError("health_timeout_seconds must be positive")
        self._worker = worker
        self._namespace = namespace
        self._health_timeout = health_timeout_seconds
        self._monotonic = monotonic
        self._sleep = sleep

    def inject(self, problem_id: str) -> InjectionWindow:
        reply = self._worker.request("inject", problem_id=problem_id, timeout=WORKER_OP_TIMEOUT_SECONDS)
        return _window(reply)

    def inject_composite(self, problem_ids: Sequence[str]) -> CompositeInjection:
        """Inject several problems so their faults are live at once, in the given order.

        ``recover`` then reverts all of them, and ``recover_fault`` one of them.
        """

        problems = list(problem_ids)
        if not problems or not all(isinstance(problem, str) and problem for problem in problems):
            raise ValueError("a composite fault needs at least one non-empty problem id")
        if len(set(problems)) != len(problems):
            raise ValueError(f"a composite fault lists a problem more than once: {problems}")
        faults: list[InjectedFault] = []
        for position, problem_id in enumerate(problems):
            try:
                reply = self._worker.request(
                    "inject", problem_id=problem_id, compose=position > 0, timeout=WORKER_OP_TIMEOUT_SECONDS
                )
                index = reply.get("fault")
                if index != position:
                    raise FastloopFaultError(f"worker placed {problem_id} at fault {index!r}, expected {position}")
                faults.append(InjectedFault(problem_id=problem_id, index=position, window=_window(reply)))
            except Exception as exc:
                if faults:
                    try:
                        self.recover()
                    except Exception as recovery_error:
                        raise FastloopFaultError(
                            f"composite injection failed at {problem_id}: {exc}; recovery failed: {recovery_error}"
                        ) from recovery_error
                raise FastloopFaultError(f"composite injection failed at {problem_id}: {exc}") from exc
        return CompositeInjection(faults=tuple(faults))

    def oracle(self, *, fault: int | None = None) -> OracleVerdict:
        args: dict[str, Any] = {} if fault is None else {"fault": fault}
        return OracleVerdict.model_validate(self._worker.request("oracle", timeout=WORKER_OP_TIMEOUT_SECONDS, **args))

    def recover_fault(self, index: int) -> float:
        """Revert one fault of a composition; the others stay, so the application may stay unhealthy."""

        if index < 0:
            raise ValueError("fault index must not be negative")
        reply = self._worker.request("recover", fault=index, timeout=WORKER_OP_TIMEOUT_SECONDS)
        seconds = reply.get("seconds")
        if not isinstance(seconds, (int, float)):
            raise FastloopFaultError(f"worker returned a non-numeric recovery time {seconds!r}")
        return float(seconds)

    def recover(self) -> float:
        """Recover every remaining fault the harness's way, then wait for the application to be healthy."""

        started = self._monotonic()
        self._worker.request("recover", timeout=WORKER_OP_TIMEOUT_SECONDS)
        deadline = started + self._health_timeout
        health: dict[str, Any] = {}
        while True:
            health = self._worker.request("health", namespace=self._namespace, timeout=60)
            if health.get("healthy"):
                return self._monotonic() - started
            if self._monotonic() >= deadline:
                raise FastloopFaultError(
                    f"{self._namespace} is not healthy {self._health_timeout:.0f}s after recovery; "
                    f"unready: {health.get('unready')}"
                )
            self._sleep(HEALTH_POLL_SECONDS)
