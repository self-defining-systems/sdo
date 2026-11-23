"""A row's second worker boots beside the warm transaction and is entered like any worker."""

from __future__ import annotations

import threading
from typing import Any

import pytest

from benchmarks.sregym.fastloop.worker_client import BackgroundCall, PrestartedWorker


class FakeWorker:
    def __init__(self) -> None:
        self.started_on: str | None = None
        self.closed = 0

    def start(self) -> None:
        self.started_on = threading.current_thread().name

    def close(self) -> None:
        self.closed += 1


def test_the_worker_is_built_and_started_off_the_callers_thread_before_it_is_entered() -> None:
    built: list[FakeWorker] = []

    def build() -> Any:
        built.append(FakeWorker())
        return built[0]

    prestarted = PrestartedWorker(build)
    with prestarted as worker:
        assert worker is built[0]
        assert worker.started_on is not None
        assert worker.started_on != threading.current_thread().name
        assert worker.closed == 0
    assert built[0].closed >= 1


def test_a_worker_the_row_never_enters_is_closed_by_discard() -> None:
    worker = FakeWorker()
    prestarted = PrestartedWorker(lambda: worker)  # type: ignore[arg-type,return-value]
    prestarted.discard()
    assert worker.closed == 1


def test_a_failed_build_surfaces_when_the_worker_is_entered_and_discard_stays_quiet() -> None:
    def build() -> Any:
        raise RuntimeError("lane images differ")

    prestarted = PrestartedWorker(build)
    prestarted.discard()
    with pytest.raises(RuntimeError, match="lane images differ"), prestarted:
        pass


def test_a_background_call_returns_its_value_or_re_raises() -> None:
    assert BackgroundCall(lambda: 7).result() == 7

    def fail() -> None:
        raise ValueError("no")

    with pytest.raises(ValueError, match="no"):
        BackgroundCall(fail).result()
