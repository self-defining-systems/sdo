"""Capture what the adapter launches: argv, the ``SDO_*`` variables and the lane's config overlay.

Unit tests never run the Go resolution (``sdo-operator config print``); ``stub_resolution`` returns the synthetic
resolved config the adapter derives its subprocess bounds from.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.fuzzops.config import BENCH_PROFILE

if TYPE_CHECKING:
    import pytest

RESOLVED: dict[str, Any] = {
    "agent": {"provider": "codex", "effort": "medium", "lifecycle_timeout": "15m0s"},
    "response": {"timeout": "30m0s"},
    "deployment": {"max_attempts": 3},
}


def stub_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.adapter import native_operator

    monkeypatch.setattr(native_operator, "resolved_settings", lambda _config: copy.deepcopy(RESOLVED))


def config_files(command: list[str]) -> list[Path]:
    return [Path(command[index + 1]) for index, item in enumerate(command) if item == "--config"]


def overlay(command: list[str]) -> dict[str, Any]:
    """The lane overlay a launch passes after the benchmark profile; read while the launch runs."""
    files = config_files(command)
    assert files[0] == BENCH_PROFILE
    if len(files) == 1:
        return {}
    return json.loads(files[-1].read_text(encoding="utf-8"))
