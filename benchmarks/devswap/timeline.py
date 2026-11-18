"""Seconds spent in each named step of a lane operation, printed as they finish (a failed step is timed too)."""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, ParamSpec, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

P = ParamSpec("P")
R = TypeVar("R")


class Timeline:
    """Ordered ``(step, seconds)`` pairs; ``step`` is a context manager, ``wrap`` times a callable."""

    def __init__(self, out: Callable[[str], object] = print, prefix: str = "") -> None:
        self.steps: list[tuple[str, float]] = []
        self._out = out
        self._prefix = prefix

    @contextmanager
    def step(self, name: str) -> Iterator[None]:
        started = time.monotonic()
        try:
            yield
        finally:
            seconds = round(time.monotonic() - started, 1)
            self.steps.append((self._prefix + name, seconds))
            self._out(f"step {self._prefix}{name}: {seconds}s")

    def wrap(self, name: str, function: Callable[P, R]) -> Callable[P, R]:
        def timed(*args: P.args, **kwargs: P.kwargs) -> R:
            with self.step(name):
                return function(*args, **kwargs)

        return timed

    def seconds(self) -> dict[str, float]:
        merged: dict[str, float] = {}
        for name, seconds in self.steps:
            merged[name] = round(merged.get(name, 0.0) + seconds, 1)
        return merged
