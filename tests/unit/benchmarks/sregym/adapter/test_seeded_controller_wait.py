"""The seeded-controller wait ends at its deadline with a named error and says when the controller is ready."""

from __future__ import annotations

from typing import Any

import pytest

from benchmarks.sregym.adapter import driver
from benchmarks.sregym.adapter.native_operator import NativeOperatorError


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _patch(monkeypatch: pytest.MonkeyPatch, ready_at: float | None) -> _Clock:
    clock = _Clock()
    monkeypatch.setattr(driver.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(driver.time, "sleep", clock.sleep)

    def inspect(_install: Any) -> None:
        if ready_at is None or clock.now < ready_at:
            raise NativeOperatorError("existing controller has no ready owned Pod")

    monkeypatch.setattr(driver, "inspect_existing", inspect)
    return clock


def test_a_seeded_controller_that_never_becomes_ready_ends_with_a_named_error(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = _patch(monkeypatch, ready_at=None)
    with pytest.raises(driver.SeededControllerNotReadyError, match=r"not ready within 60s.*no ready owned Pod"):
        driver._wait_for_installed_controller(object(), timeout_seconds=60)  # type: ignore[arg-type]
    assert clock.now <= 70


def test_a_seeded_controller_that_becomes_ready_says_so(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _patch(monkeypatch, ready_at=12)
    caplog.set_level("INFO", logger=driver.__name__)
    driver._wait_for_installed_controller(object(), timeout_seconds=60)  # type: ignore[arg-type]
    assert "seeded controller ready after" in caplog.records[-1].getMessage()
