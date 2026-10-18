"""Benchmark firing stream export preserves event order from native runtime artifacts."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import benchmarks.sregym.adapter.runtime as runtime

if TYPE_CHECKING:
    from pathlib import Path


def test_firing_stream_places_rotated_events_before_live_events(tmp_path: Path) -> None:
    exported = tmp_path / "sdo_runtime" / "telemetry"
    exported.mkdir(parents=True)
    (exported / "detector-firings.jsonl.1").write_text('{"event_id":"old"}\n', encoding="utf-8")
    (exported / "detector-firings.jsonl").write_text('{"event_id":"new"}\n', encoding="utf-8")

    runtime._materialize_firing_stream(exported, tmp_path)

    stream = tmp_path / runtime.FIRING_STREAM_FILENAME
    ids = [json.loads(line)["event_id"] for line in stream.read_text(encoding="utf-8").splitlines()]
    assert ids == ["old", "new"]
