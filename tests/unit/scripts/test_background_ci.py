"""Real background checks preserve results across independent action steps."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

from scripts.background_ci import finish, start

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("status", [0, 7])
def test_background_check_preserves_real_child_result(tmp_path: Path, status: int) -> None:
    state = tmp_path / "check"
    marker = tmp_path / "ran"
    command = f"from pathlib import Path; Path({str(marker)!r}).write_text('ran'); raise SystemExit({status})"
    start(state, [sys.executable, "-c", command])
    assert finish(state) == status
    assert marker.read_text() == "ran"
