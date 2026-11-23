"""The Prometheus capture reads a long window in bounded slices.

EKS GitLab series: the last position captures Prometheus once over the whole series (#1810, ~30 minutes); one
query_range per metric over that window (four at a time) OOM-killed the 1 GiB Prometheus mid-capture, the capture
failed and the evidence gate held the lane. Each query now spans at most ``capture.SLICE``, and a series' slices are
joined in time order with no repeated sample.
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlparse

from benchmarks.fuzzops import capture

if TYPE_CHECKING:
    from pathlib import Path


class FakePrometheus:
    """One metric, one series, a sample every STEP_S; records the span of every range query."""

    def __init__(self) -> None:
        self.spans: list[float] = []

    def get(self, path: str) -> bytes:
        if path.startswith("/api/v1/label/__name__/values"):
            return json.dumps({"data": ["http_requests_total"]}).encode()
        q = parse_qs(urlparse(path).query)
        start, end, step = float(q["start"][0]), float(q["end"][0]), float(q["step"][0])
        self.spans.append(end - start)
        values, t = [], start
        while t <= end:
            values.append([t, "1"])
            t += step
        return json.dumps({"data": {"result": [{"metric": {"job": "web"}, "values": values}]}}).encode()


def test_a_long_window_is_read_in_bounded_slices_and_joined(tmp_path: Path) -> None:
    start = datetime(2026, 11, 23, 9, 50, tzinfo=UTC)
    end = start + timedelta(minutes=31)
    prom = FakePrometheus()
    receipt = capture.capture_prometheus(prom, tmp_path, start, end, budget=10**9)  # type: ignore[arg-type]
    assert receipt["status"] == "ok"
    assert len(prom.spans) >= 4
    assert max(prom.spans) <= capture.SLICE.total_seconds()
    line = json.loads(gzip.decompress((tmp_path / receipt["files"][0]).read_bytes()).decode().splitlines()[0])
    (series,) = line["result"]
    times = [v[0] for v in series["values"]]
    assert times == sorted(set(times)), "slices must join in order without repeated samples"
    assert times[0] == start.timestamp()
    assert times[-1] >= end.timestamp() - capture.STEP_S
    assert len(times) == int((end - start).total_seconds() // capture.STEP_S) + 1
