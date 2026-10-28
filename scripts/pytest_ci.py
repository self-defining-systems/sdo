"""Structured collection/timings for budgeted CI, without changing verdicts."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest

_COSTS: dict[str, float] = {}
_CONTROLLER = True


def pytest_configure(config: pytest.Config) -> None:
    global _CONTROLLER
    # A nested controller may inherit an outer worker's environment. The
    # configuration, not that inherited string, owns the actual xdist role.
    _CONTROLLER = not hasattr(config, "workerinput")
    _COSTS.clear()


def pytest_collection_finish(session: pytest.Session) -> None:
    target = os.environ.get("SDO_PYTEST_INVENTORY")
    if target:
        Path(target).write_text(json.dumps(sorted(item.nodeid for item in session.items)) + "\n")


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if _CONTROLLER:
        _COSTS[report.nodeid] = _COSTS.get(report.nodeid, 0) + report.duration


def pytest_sessionfinish() -> None:
    target = os.environ.get("SDO_PYTEST_COST_OUTPUT")
    if target and _CONTROLLER:
        Path(target).write_text(json.dumps({n: max(0.001, round(c, 3)) for n, c in sorted(_COSTS.items())}) + "\n")
