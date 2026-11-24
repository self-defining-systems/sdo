"""A series is one pipeline: only the rollout and the verdict of each release are serial.

The quiet window after a verdict was paid once per position (94 s of a 600 s position cycle on EKS). It now matters at
the end of the series only: an incident that opens after a non-final position is caught by the next push's own verdict
(and ``blocked_by_open_incident``). The Prometheus and Jaeger capture is every position's own, over its own window, so
it is on disk before the next position starts.
"""

from __future__ import annotations

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


def test_a_position_captures_from_where_the_position_before_it_ended(tmp_path: Path) -> None:
    started = "2026-01-01T00:00:00Z"
    capture.write_series_position(tmp_path, position=1, total=4, started=started)
    assert capture.capture_plan(tmp_path).since == ""
    capture.mark_captured(tmp_path, "2026-01-01T00:10:00Z")
    capture.write_series_position(tmp_path, position=2, total=4, started=started)
    w = capture.window("2026-01-01T00:20:00Z", "2026-01-01T00:30:00Z", capture.capture_plan(tmp_path).since)
    assert w.start.isoformat() == "2026-01-01T00:10:00+00:00"
    assert w.run.isoformat() == "2026-01-01T00:20:00+00:00"


def test_a_later_series_on_the_lane_does_not_reach_back_into_the_earlier_one(tmp_path: Path) -> None:
    capture.write_series_position(tmp_path, position=1, total=1, started="2026-01-01T00:00:00Z")
    capture.mark_captured(tmp_path, "2026-01-01T00:10:00Z")
    capture.write_series_position(tmp_path, position=1, total=1, started="2026-01-02T00:00:00Z")
    assert capture.capture_plan(tmp_path).since == ""


def test_a_lane_without_a_series_captures_every_run_over_its_own_window(tmp_path: Path) -> None:
    capture.mark_captured(tmp_path, "2026-01-01T00:10:00Z")
    assert capture.capture_plan(tmp_path) == capture.CapturePlan()


def test_every_position_reads_its_stores(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A capture left to the last position was lost with it: the store was reset before anything was on disk."""
    ok = {"status": "ok", "files": [], "bytes": 0}
    for name in ("capture_prometheus", "capture_jaeger", "capture_logs"):
        monkeypatch.setattr(capture, name, lambda *a, **k: ok)
    monkeypatch.setattr(capture, "service_exists", lambda *a, **k: True)
    capture.write_series_position(tmp_path, position=1, total=4, started="2026-01-01T00:00:00Z")
    receipt = capture.capture_all(
        lambda *a: ["false"], "ctl", "app", tmp_path, "2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z"
    )
    assert [receipt[k]["status"] for k in cellevidence.CAPTURES] == ["ok"] * 3
