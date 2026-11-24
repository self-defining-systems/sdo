"""The Prometheus capture asks a bounded answer of a 1 GiB store and costs what its window holds.

EKS GitLab series: the last position captured the whole series (two hours), one query_range per metric per slice; one
lane's collect hook ran into the platform's limit and was killed, another's Prometheus was OOM-killed. A run captures
its own window; metrics are read together; no query asks for more than ``QUERY_POINTS`` points whatever the cardinality.
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from benchmarks.fuzzops import capture
from tests.unit.benchmarks.fuzzops.fake_stores import FakePrometheus

if TYPE_CHECKING:
    from pathlib import Path

START = datetime(2026, 11, 23, 9, 50, tzinfo=UTC)


def a_window(minutes: float) -> capture.Window:
    return capture.Window(START, START + capture.LEAD, START + timedelta(minutes=minutes))


def test_a_long_window_is_read_in_bounded_slices_and_joined(tmp_path: Path) -> None:
    w = a_window(31)
    prom = FakePrometheus({"http_requests_total": 1})
    receipt = capture.capture_prometheus(prom, tmp_path, w)  # type: ignore[arg-type]
    assert receipt["status"] == "ok"
    assert len(prom.queries) >= 4
    assert max(e - s for _, s, e in prom.queries) <= capture.SLICE.total_seconds()
    chunks = list(capture.read_lines(tmp_path / receipt["files"][0]))
    assert {chunk["metric"] for chunk in chunks} == {"http_requests_total"}
    assert receipt["series"] == 1
    times = [v[0] for chunk in chunks for series in chunk["result"] for v in series["values"]]
    assert times == sorted(set(times)), "slices must join in order without repeated samples"
    assert times[0] == w.start.timestamp()
    assert times[-1] >= w.end.timestamp() - capture.STEP_S
    assert len(times) == int((w.end - w.start).total_seconds() // capture.STEP_S) + 1


def test_many_small_metrics_cost_few_queries(tmp_path: Path) -> None:
    prom = FakePrometheus({f"m{i}": 3 for i in range(1000)})
    receipt = capture.capture_prometheus(prom, tmp_path, a_window(10))  # type: ignore[arg-type]
    assert receipt["queries"] == len(prom.queries) <= 60
    lines = gzip.decompress((tmp_path / receipt["files"][0]).read_bytes()).splitlines()
    assert {json.loads(line)["metric"] for line in lines} == set(prom.series)


def test_no_query_asks_for_more_points_than_its_bound() -> None:
    series = {"small": 10, "big": 60_000, "huge": 190_000}
    w = a_window(30)
    queries = capture.plan_queries(series, w.start, w.end)
    asked = {n for q in queries for n in q.metrics}
    assert asked == set(series)
    for q in queries:
        for s, e in q.spans:
            steps = int((e - s) // capture.STEP_S) + 1
            assert sum(series[n] for n in q.metrics) * steps <= capture.QUERY_POINTS
        assert q.spans[0][0] == w.start.timestamp()
        assert q.spans[-1][1] >= w.end.timestamp() - capture.STEP_S


def test_a_position_costs_the_same_wherever_it_is_in_the_series(tmp_path: Path) -> None:
    """The window is the position's own, not the series'."""
    costs = []
    for position in (1, 12):
        began = START + timedelta(minutes=10 * position)
        capture.write_series_position(tmp_path, position=position, total=12, started="2026-11-23T09:50:00Z")
        since = capture.capture_plan(tmp_path).since
        w = capture.window(began.isoformat(), (began + timedelta(minutes=9)).isoformat(), since)
        prom = FakePrometheus({"m": 100})
        capture.capture_prometheus(prom, tmp_path, w)  # type: ignore[arg-type]
        costs.append(sum(prom.points))
        capture.mark_captured(tmp_path, w.end.isoformat())
    assert costs[1] <= costs[0] * (capture.SERIES_GAP / timedelta(minutes=9) + 1)
    assert capture.window(began.isoformat(), "", "2026-11-23T00:00:00Z").start == began - capture.SERIES_GAP


@pytest.mark.parametrize(
    ("begins", "ends", "why"),
    [
        (START + capture.LEAD + timedelta(minutes=4), None, "what it held is lost"),
        (None, START + timedelta(minutes=15), "samples end"),
        (START + timedelta(days=1), None, "no sample"),
    ],
)
def test_a_store_that_does_not_cover_the_run_is_not_ok(
    tmp_path: Path, begins: datetime | None, ends: datetime | None, why: str
) -> None:
    prom = FakePrometheus({"m": 1}, begins.timestamp() if begins else 0, ends.timestamp() if ends else float("inf"))
    with pytest.raises(capture.CaptureError, match=why):
        capture.capture_prometheus(prom, tmp_path, a_window(20))  # type: ignore[arg-type]
    assert not list((tmp_path / "observability").iterdir()), "nothing unverified is left on disk"


def test_a_file_over_its_bound_is_not_kept(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(capture, "PROMETHEUS_MAX_BYTES", 10)
    with pytest.raises(capture.CaptureError, match="10 byte bound"):
        capture.capture_prometheus(FakePrometheus({"m": 1}), tmp_path, a_window(20))  # type: ignore[arg-type]
    assert not list((tmp_path / "observability").iterdir())


def test_capture_partitions_cardinality_queries_and_writes_each_slice(tmp_path: Path) -> None:
    class GroupedPrometheus(FakePrometheus):
        def get(self, path: str) -> bytes:
            if path.startswith("/api/v1/query?"):
                from urllib.parse import parse_qs, urlparse

                query = parse_qs(urlparse(path).query)["query"][0]
                assert '{__name__=~".+"}' not in query, "cardinality planning must not materialize the full store"
            return super().get(path)

    prom = GroupedPrometheus({"http_requests_total": 1})
    receipt = capture.capture_prometheus(prom, tmp_path, a_window(31))  # type: ignore[arg-type]
    lines = gzip.decompress((tmp_path / receipt["files"][0]).read_bytes()).splitlines()
    assert len(lines) == len(prom.queries) > 1, "flush each slice before reading the next"
