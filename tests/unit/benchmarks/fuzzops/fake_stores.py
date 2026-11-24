"""A Prometheus and a Jaeger as the capture sees them: the JSON their HTTP APIs answer, through ``Proxy.get``."""

from __future__ import annotations

import json
import re
from urllib.parse import parse_qs, urlparse

from benchmarks.fuzzops import capture


class FakePrometheus:
    """``series`` metric -> series count, each with a sample every ``STEP_S`` from ``begins`` to ``ends`` (epoch
    seconds); records what every range query asked for."""

    def __init__(self, series: dict[str, int], begins: float = 0, ends: float = float("inf")) -> None:
        self.series, self.begins, self.ends = series, begins, ends
        self.queries: list[tuple[list[str], float, float]] = []
        self.points: list[int] = []

    def get(self, path: str) -> bytes:
        url = urlparse(path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        if url.path.startswith("/api/v1/label/__name__/values"):
            return json.dumps({"data": sorted(self.series)}).encode()
        if url.path == "/api/v1/query":
            result = [{"metric": {"__name__": n}, "value": [0, str(c)]} for n, c in self.series.items()]
            return json.dumps({"data": {"result": result}}).encode()
        wanted = re.search(r'"(.*)"', q["query"])
        assert wanted
        names = wanted.group(1).split("|")
        start, end, step = float(q["start"]), float(q["end"]), float(q["step"])
        assert step == capture.STEP_S
        times = [start + i * step for i in range(int((end - start) // step) + 1)]
        values = [[t, "1"] for t in times if self.begins <= t <= self.ends]
        result = [
            {"metric": {"__name__": n, "i": str(i)}, "values": values}
            for n in names
            for i in range(self.series[n])
            if values
        ]
        self.queries.append((names, start, end))
        self.points.append(sum(self.series[n] for n in names) * len(times))
        return json.dumps({"data": {"result": result}}).encode()


class FakeJaeger:
    def __init__(self, traces: dict[str, int]) -> None:
        self.traces = traces

    def get(self, path: str) -> bytes:
        url = urlparse(path)
        if url.path == "/api/services":
            return json.dumps({"data": sorted(self.traces)}).encode()
        n = self.traces[parse_qs(url.query)["service"][0]]
        return json.dumps({"data": [{"traceID": str(i)} for i in range(n)]}).encode()
