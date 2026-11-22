"""Fixtures for the fuzzops tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from benchmarks.fuzzops import capture

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def _no_capture_retry_wait(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """A capture that fails is retried after a pause; no test should sit through it."""
    monkeypatch.setattr(capture, "_pause", lambda seconds: None)
    return
