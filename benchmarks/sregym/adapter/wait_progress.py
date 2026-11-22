"""What a wait on the run path is waiting for: announced once, logged every minute.

A wait that is silent for an hour cannot be told from a hung process. Every polling wait of the adapter and the
fastloop holds one :class:`WaitProgress`: it names the wait in the run's log when it starts, repeats it every
``PROGRESS_LOG_SECONDS`` with the time spent and the bound, and records it in the file named by
``CURRENT_WAIT_ENV`` (when the launching adapter set one), so the process that kills a run at its row deadline can
name the wait it was stuck in.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter.budgets import WaitKind, wait_for

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

#: The file the launching adapter reads to name the wait a killed run was in.
CURRENT_WAIT_ENV = "SDO_CURRENT_WAIT_FILE"
PROGRESS_LOG_SECONDS = wait_for(WaitKind.MINUTE)


class WaitProgress:
    """One wait: ``tick()`` once per poll; it logs on start and then once per interval."""

    def __init__(
        self,
        name: str,
        monotonic: Callable[[], float],
        *,
        bound_seconds: float | None = None,
        every: float = PROGRESS_LOG_SECONDS,
    ) -> None:
        self.name = name
        self._monotonic = monotonic
        self._bound = bound_seconds
        self._every = every
        self._started = monotonic()
        self._logged = self._started
        self._announced = False

    def tick(self, detail: str = "") -> None:
        now = self._monotonic()
        if not self._announced:
            self._announced = True
            self._record()
            logger.info("waiting for %s%s%s", self.name, self._bound_text(), f": {detail}" if detail else "")
            return
        if now - self._logged >= self._every:
            self._logged = now
            logger.info(
                "still waiting for %s: %.0fs elapsed%s%s",
                self.name,
                now - self._started,
                self._bound_text(),
                f": {detail}" if detail else "",
            )

    def _bound_text(self) -> str:
        return f" (bound {self._bound:.0f}s)" if self._bound is not None else ""

    def _record(self) -> None:
        target = os.environ.get(CURRENT_WAIT_ENV)
        if not target:
            return
        path = Path(target)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False, encoding="utf-8") as handle:
                json.dump(
                    {
                        "wait": self.name,
                        "since": datetime.now(UTC).isoformat(timespec="seconds"),
                        "bound_seconds": self._bound,
                    },
                    handle,
                )
            Path(handle.name).replace(path)
        except OSError as exc:  # naming the wait is a diagnostic; it never fails the run
            logger.warning("could not record the current wait in %s: %s", path, exc)
