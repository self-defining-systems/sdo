"""A transient failure of one observability read must not fail the cell's evidence.

The collect step's Prometheus capture is hundreds of reads through the API server proxy; one stream error (the
Prometheus pod restarting, an apiserver blip) failed the whole capture, the evidence gate held the lane for the
rest of the run, and a person had to waive it. Captures are idempotent, so each is retried a few times first.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from benchmarks.fuzzops import capture

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_a_capture_that_fails_transiently_is_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"prometheus": 0}

    def flaky(proxy: Any, dest: Path, start: Any, end: Any, budget: int) -> dict[str, Any]:
        calls["prometheus"] += 1
        if calls["prometheus"] < 3:
            raise RuntimeError("Error from server (ServiceUnavailable): error trying to reach service")
        return {"status": "ok", "files": [], "bytes": 0}

    monkeypatch.setattr(capture, "capture_prometheus", flaky)
    monkeypatch.setattr(capture, "service_exists", lambda *a, **k: True)
    monkeypatch.setattr(capture, "capture_jaeger", lambda *a, **k: {"status": "ok", "files": [], "bytes": 0})
    monkeypatch.setattr(capture, "capture_logs", lambda *a, **k: {"status": "ok", "files": [], "bytes": 0})
    monkeypatch.setattr(capture, "_pause", lambda seconds: None, raising=False)
    (tmp_path / "observability").mkdir()
    receipt = capture.capture_all(
        lambda *a: ["true"], "ctl", "app", tmp_path, "2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z"
    )
    assert receipt["prometheus"]["status"] == "ok", receipt["prometheus"]
    assert calls["prometheus"] == 3


def test_a_capture_that_keeps_failing_still_reports_the_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(proxy: Any, dest: Path, start: Any, end: Any, budget: int) -> dict[str, Any]:
        raise RuntimeError("connection refused")

    monkeypatch.setattr(capture, "capture_prometheus", broken)
    monkeypatch.setattr(capture, "service_exists", lambda *a, **k: True)
    monkeypatch.setattr(capture, "capture_jaeger", lambda *a, **k: {"status": "ok", "files": [], "bytes": 0})
    monkeypatch.setattr(capture, "capture_logs", lambda *a, **k: {"status": "ok", "files": [], "bytes": 0})
    monkeypatch.setattr(capture, "_pause", lambda seconds: None, raising=False)
    (tmp_path / "observability").mkdir()
    receipt = capture.capture_all(
        lambda *a: ["true"], "ctl", "app", tmp_path, "2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z"
    )
    assert receipt["prometheus"]["status"] == "error"
