"""A series is one pipeline: only the rollout and the verdict of each release are serial.

The quiet window after a verdict and the evidence capture were paid once per position (94 s and 47 s of a 600 s
position cycle on EKS). The quiet window now matters at the end of the series only: an incident that opens after a
non-final position is caught by the next push's own verdict (and ``blocked_by_open_incident``), and the Prometheus and
Jaeger capture is taken once, by the last position, over the whole series.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.commit_series.scenario import Bounds
from benchmarks.fuzzops import capture, cellevidence

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_only_the_last_position_waits_out_the_quiet_window() -> None:
    bounds = Bounds(verdict=900, steady=30)
    assert bounds.at(1, 4).steady == bounds.steady_between
    assert bounds.at(3, 4).steady == bounds.steady_between
    assert bounds.at(4, 4).steady == 30
    assert bounds.at(1, 1).steady == 30
    assert bounds.at(1, 4).verdict == 900


def test_the_series_marker_tells_collect_whether_it_is_the_last_position(tmp_path: Path) -> None:
    capture.write_series_position(tmp_path, position=2, total=4, started="2026-01-01T00:00:00Z")
    plan = capture.capture_plan(tmp_path, start="2026-01-01T00:20:00Z")
    assert plan == capture.CapturePlan(defer=True, start="2026-01-01T00:20:00Z")
    capture.write_series_position(tmp_path, position=4, total=4, started="2026-01-01T00:00:00Z")
    plan = capture.capture_plan(tmp_path, start="2026-01-01T00:40:00Z")
    assert plan == capture.CapturePlan(defer=False, start="2026-01-01T00:00:00Z")


def test_a_lane_without_a_series_captures_every_run_over_its_own_window(tmp_path: Path) -> None:
    assert capture.capture_plan(tmp_path, start="s") == capture.CapturePlan(defer=False, start="s")


def test_a_deferred_capture_reads_nothing_and_says_where_it_is(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "observability").mkdir()
    monkeypatch.setattr(capture, "capture_logs", lambda *a, **k: {"status": "ok", "files": [], "bytes": 0})
    receipt = capture.capture_all(
        lambda *a: ["false"], "ctl", "app", tmp_path, "2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z", defer=True
    )
    for name in ("prometheus", "jaeger"):
        assert receipt[name]["status"] == "deferred", receipt
    assert json.loads((tmp_path / cellevidence.OBSERVABILITY).read_text())["prometheus"]["status"] == "deferred"
