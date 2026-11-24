"""Collect-time capture of what a cell's evidence needs beyond the evidence sink: build identity, host load, the
observability stores for the window (Prometheus series, Jaeger traces) and the application pods' logs.

Every run captures its own window, also each position of a series, so a capture costs what its window holds whatever
the series' length, and position k is on disk before position k+1 starts. Every file is written to a temp file, bounded
by the capture's own ``*_MAX_BYTES``, read back (``verified``) and only then renamed into place. Every capture has its
own share of the time collect gives them (``Deadline``). ``observability.json`` is the receipt the validator reads, per
capture ``status``:

- ``ok``: the ``files`` are on disk, open whole, hold something and (Prometheus) cover the window's start and end.
- ``absent``: the store is not installed.
- ``empty``: the store ran through the whole window and answers, and holds nothing for it. Nothing is written. The
  validator accepts it of Jaeger (an application that sends no traces has none, and no later read can make any) and of
  nothing else: Prometheus scrapes its own targets and the control namespace always logs.
- ``error``: anything else, with ``detail``.
"""

from __future__ import annotations

import gzip
import json
import os
import re
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from benchmarks.fuzzops import cellevidence as ce

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

#: The most each capture may put on disk per run, compressed. A capture that would exceed it is an error, never cut.
PROMETHEUS_MAX_BYTES = 96 * 1024 * 1024
JAEGER_MAX_BYTES = 32 * 1024 * 1024
LOGS_MAX_BYTES = 64 * 1024 * 1024
PROMETHEUS = ("sdo-prometheus", "9090")
JAEGER = ("sdo-jaeger", "16686")
STEP_S = 15
#: The longest span one Prometheus range query reads, the most series one query asks for (a metric with more is a query
#: of its own) and the most points one query returns: they bound what the lane's 1 GiB Prometheus holds for one answer
#: whatever the window and the cardinality. One query runs at a time; the metrics of a small lane fit a few queries, so
#: the capture costs tens of API calls, not one per metric.
SLICE = timedelta(minutes=10)
QUERY_SERIES = 5_000
QUERY_METRICS = 40
QUERY_POINTS = 200_000
#: A metric whose series count is not known is counted as this many.
UNCOUNTED_SERIES = 500
LEAD = timedelta(minutes=5)
SKIP_METRICS = re.compile(r"^(go_|process_|promhttp_|net_conntrack_|prometheus_(tsdb|engine|sd|rule|notifications)_)")
#: How far from the window's edges the first and last sample may be: several scrapes of SDO's Prometheus.
COVER = timedelta(seconds=60)
#: The most a series position's window reaches back for what happened since the position before it was captured.
SERIES_GAP = timedelta(minutes=20)
TIMEOUT_S = 60
#: The time every capture together may take when the caller does not say, and each one's share of what is left when it
#: starts (the last takes the rest).
CAPTURE_S = 300.0
SHARE = {"prometheus": 0.55, "jaeger": 0.4, "applogs": 1.0}
LOG_WORKERS = 4
ATTEMPTS = 3
RETRY_AFTER_S = 15


def _pause(seconds: float) -> None:
    time.sleep(seconds)


class CaptureError(RuntimeError):
    """A capture that another attempt cannot make: it has no time left, or the store no longer holds the window."""


class Deadline:
    """When something must have ended; every wait inside it is cut to what is left."""

    def __init__(self, seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock, self.seconds = clock, seconds
        self._end = clock() + seconds

    def left(self) -> float:
        return self._end - self._clock()

    def within(self, seconds: float) -> float:
        """``seconds``, or what is left when that is less; raises when nothing is."""
        left = self.left()
        if left <= 0:
            raise CaptureError(f"not done within its {self.seconds:.0f} s")
        return min(seconds, left)

    def share(self, part: float) -> Deadline:
        return Deadline(max(self.left(), 0.0) * part, self._clock)


#: Where the series adapter says which position of which series (its start) the run is, and where collect notes up to
#: when that series' stores are captured.
SERIES_POSITION_FILE = "series-position.json"
SERIES_CAPTURED_FILE = "series-captured.json"


@dataclass(frozen=True)
class CapturePlan:
    """What a collect step captures of the observability stores: a window that begins no later than ``since`` (""
    for the run's own window)."""

    since: str = ""


def write_series_position(lane_dir: Path, *, position: int, total: int, started: str) -> None:
    """The series adapter tells the collect step where in the series the run is."""
    (lane_dir / SERIES_POSITION_FILE).write_text(
        json.dumps({"position": position, "total": total, "started": started}) + "\n"
    )


def capture_plan(lane_dir: Path) -> CapturePlan:
    """A position of a series captures from where the series' last capture ended, so that nothing that happened between
    two positions is in neither window."""
    marker = ce.read_json_object(lane_dir / SERIES_POSITION_FILE)
    captured = ce.read_json_object(lane_dir / SERIES_CAPTURED_FILE)
    if not marker or captured.get("started") != marker.get("started"):
        return CapturePlan()
    return CapturePlan(since=str(captured.get("end") or ""))


def mark_captured(lane_dir: Path, end: str) -> None:
    """The stores of the lane's series are captured, verified, up to ``end``."""
    marker = ce.read_json_object(lane_dir / SERIES_POSITION_FILE)
    if marker:
        ce.atomic_write_json(lane_dir / SERIES_CAPTURED_FILE, {"started": marker.get("started"), "end": end})


def host_load() -> dict[str, float]:
    one, five, fifteen = os.getloadavg()
    return {"load1": one, "load5": five, "load15": fifteen, "cores": float(os.cpu_count() or 0)}


def write_host_load(dest: Path, now: Callable[[], str]) -> str:
    ce.atomic_write_json(dest / ce.HOST_LOAD, {"at": now(), **host_load()})
    return "ok"


def write_build(dest: Path, fields: dict[str, Any]) -> str:
    ce.atomic_write_json(dest / ce.BUILD, fields)
    return "ok"


def _parse(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


@dataclass(frozen=True)
class Window:
    """``start`` to ``end`` is captured; ``run`` is when the run itself began, which a capture must cover."""

    start: datetime
    run: datetime
    end: datetime


def window(start: str, end: str, since: str = "") -> Window:
    e = _parse(end or "") or datetime.now(UTC)
    run = _parse(start or "") or e - timedelta(minutes=15)
    s = run - LEAD
    if (back := _parse(since)) is not None and back < s:
        s = max(back, run - SERIES_GAP)
    return Window(s, run, e)


class Proxy:
    """Reads a Service of the control namespace through the API server proxy (no port-forward, no pod exec)."""

    def __init__(
        self, kube_cmd: Callable[..., list[str]], namespace: str, service: tuple[str, str], deadline: Deadline
    ) -> None:
        self.kube_cmd, self.namespace, self.service, self.deadline = kube_cmd, namespace, service, deadline

    def get(self, path: str) -> bytes:
        raw = f"/api/v1/namespaces/{self.namespace}/services/{self.service[0]}:{self.service[1]}/proxy{path}"
        proc = subprocess.run(
            self.kube_cmd("get", "--raw", raw),
            capture_output=True,
            timeout=self.deadline.within(TIMEOUT_S),
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.decode(errors="replace").strip()[-200:])
        return proc.stdout


def service_exists(kube_cmd: Callable[..., list[str]], namespace: str, name: str) -> bool:
    """Whether the store is installed; raises when the API server does not say."""
    cmd = kube_cmd("-n", namespace, "get", "service", name, "--ignore-not-found", "-o", "name")
    proc = subprocess.run(cmd, capture_output=True, timeout=30, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"get service {name}: {proc.stderr.decode(errors='replace').strip()[-200:]}")
    return bool(proc.stdout.strip())


def running_since(kube_cmd: Callable[..., list[str]], namespace: str, name: str, deadline: Deadline) -> datetime | None:
    """When the store's container started (None when no pod of it runs): a store in memory holds nothing older."""
    cmd = kube_cmd("-n", namespace, "get", "pods", "-l", f"app.kubernetes.io/name={name}", "-o", "json")
    proc = subprocess.run(cmd, capture_output=True, timeout=deadline.within(TIMEOUT_S), check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"pods of {name}: {proc.stderr.decode(errors='replace').strip()[-200:]}")
    started = [
        _parse(str(((status.get("state") or {}).get("running") or {}).get("startedAt") or ""))
        for pod in json.loads(proc.stdout).get("items", [])
        for status in (pod.get("status") or {}).get("containerStatuses") or []
    ]
    return min((s for s in started if s is not None), default=None)


# ---- files ---------------------------------------------------------------------------------------------------------


def write_lines(
    path: Path, lines: Iterable[str], limit: int, verify: Callable[[Path], dict[str, Any]]
) -> dict[str, Any]:
    """Write ``lines`` as one gzip file that appears at ``path`` only once it is whole, at most ``limit`` bytes, and
    ``verify`` has read it back (it raises, or says what it found, which this returns with the size)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "wb") as raw:
            with gzip.GzipFile(filename="", fileobj=raw, mode="wb", compresslevel=6, mtime=0) as gz:
                for line in lines:
                    gz.write(line.encode() + b"\n")
                    if raw.tell() > limit:
                        raise CaptureError(f"{path.name} exceeds its {limit} byte bound")
            raw.flush()
            os.fsync(raw.fileno())
        size = tmp.stat().st_size
        if size > limit:
            raise CaptureError(f"{path.name}: {size} bytes exceed its {limit} byte bound")
        found = verify(tmp)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return {**found, "bytes": size}


def read_lines(path: Path) -> Iterator[dict[str, Any]]:
    """The JSON objects of a gzip JSON-lines file, read to its end (a cut or corrupt file raises)."""
    with gzip.open(path, "rb") as f:
        for line in f:
            yield json.loads(line)


# ---- Prometheus ----------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Query:
    """Metrics read together, with the spans their answers are read in."""

    metrics: tuple[str, ...]
    spans: tuple[tuple[float, float], ...]


def slices(start: datetime, end: datetime, points: int) -> tuple[tuple[float, float], ...]:
    """``[start, end]`` as consecutive spans of at most ``points`` steps (and ``SLICE``) on the ``STEP_S`` grid from
    ``start``, each starting one step after the previous one ends, so no sample is read twice."""
    out: list[tuple[float, float]] = []
    s, e = start.timestamp(), end.timestamp()
    width = min(int(SLICE.total_seconds() // STEP_S), max(points - 1, 0)) * STEP_S
    while s <= e:
        out.append((s, min(s + width, e)))
        s += width + STEP_S
    return tuple(out)


def plan_queries(series: dict[str, int], start: datetime, end: datetime) -> list[Query]:
    """The metrics (with their series counts) as queries that each stay within ``QUERY_SERIES`` (one metric alone may
    exceed it) and ``QUERY_POINTS``."""
    groups: list[list[str]] = []
    size = 0
    for name, n in sorted(series.items(), key=lambda kv: (kv[1], kv[0])):
        if not groups or size + n > QUERY_SERIES or len(groups[-1]) >= QUERY_METRICS:
            groups.append([])
            size = 0
        groups[-1].append(name)
        size += n
    return [
        Query(tuple(g), slices(start, end, max(QUERY_POINTS // max(sum(series[n] for n in g), 1), 1))) for g in groups
    ]


def _selector(metrics: tuple[str, ...]) -> str:
    if len(metrics) == 1:
        return '{__name__="' + metrics[0] + '"}'
    return '{__name__=~"' + "|".join(metrics) + '"}'


def _api(proxy: Proxy, path: str, **params: Any) -> Any:
    query = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in params.items())
    return json.loads(proxy.get(f"/api/v1/{path}?{query}"))["data"]


def series_counts(proxy: Proxy, w: Window) -> dict[str, int]:
    """The metrics the store holds for the window (minus runtime noise), each with its series count at the window's
    edges."""
    names = _api(proxy, "label/__name__/values", start=w.start.timestamp(), end=w.end.timestamp())
    counts: dict[str, int] = {}
    for at in (w.start, w.end):
        answer = _api(proxy, "query", query='count by (__name__) ({__name__=~".+"})', time=at.timestamp())
        for item in answer["result"]:
            name = item["metric"].get("__name__", "")
            counts[name] = max(counts.get(name, 0), int(float(item["value"][1])))
    return {n: counts.get(n, UNCOUNTED_SERIES) for n in names if not SKIP_METRICS.match(n)}


def joined(results: list[list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    """Range-query results of consecutive slices by metric name: each series' values in time order."""
    series: dict[str, dict[str, Any]] = {}
    for result in results:
        for item in result:
            key = json.dumps(item.get("metric", {}), sort_keys=True)
            series.setdefault(key, {"metric": item.get("metric", {}), "values": []})["values"].extend(
                item.get("values", [])
            )
    out: dict[str, list[dict[str, Any]]] = {}
    for item in series.values():
        item["values"].sort(key=lambda v: float(v[0]))
        out.setdefault(str(item["metric"].get("__name__", "")), []).append(item)
    return out


def verified_series(path: Path, w: Window) -> dict[str, Any]:
    """Read the written file back: whole, not empty, and with samples from the run's start to the window's end."""
    first, last, series = float("inf"), float("-inf"), 0
    for line in read_lines(path):
        for item in line["result"]:
            if values := item["values"]:
                series += 1
                first, last = min(first, float(values[0][0])), max(last, float(values[-1][0]))
    if not series:
        raise CaptureError("the file holds no sample")
    slack = COVER.total_seconds()
    if first > w.run.timestamp() + slack:
        raise CaptureError(
            f"the store's samples begin {first - w.run.timestamp():.0f} s after the run did: what it held is lost"
        )
    if last < w.end.timestamp() - slack:
        raise CaptureError(f"the store's samples end {w.end.timestamp() - last:.0f} s before the window does")
    return {"series": series}


def capture_prometheus(proxy: Proxy, dest: Path, w: Window) -> dict[str, Any]:
    """Every metric's series over the window (minus runtime noise), one gzip JSON-lines file: {metric, result}."""
    queries = plan_queries(series_counts(proxy, w), w.start, w.end)
    calls = 0

    def lines() -> Iterator[str]:
        nonlocal calls
        for q in queries:
            parts = []
            for s, e in q.spans:
                answer = _api(proxy, "query_range", query=_selector(q.metrics), start=s, end=e, step=STEP_S)
                parts.append(answer["result"])
                calls += 1
            for name, result in sorted(joined(parts).items()):
                yield json.dumps({"metric": name, "result": result}, separators=(",", ":"))

    if not queries:
        return {"status": "empty", "detail": "the store holds no metric for the window"}
    rel = "observability/prometheus.jsonl.gz"
    found = write_lines(dest / rel, lines(), PROMETHEUS_MAX_BYTES, lambda p: verified_series(p, w))
    metrics = sum(len(q.metrics) for q in queries)
    return {"status": "ok", "files": [rel], "metrics": metrics, "queries": calls, **found}


# ---- Jaeger --------------------------------------------------------------------------------------------------------


def capture_jaeger(proxy: Proxy, dest: Path, w: Window, up_since: Callable[[], datetime | None]) -> dict[str, Any]:
    """The traces of every service over the window, one gzip JSON-lines file. Traces have no cadence, so what covers
    the window is the store: its container (it keeps traces in memory) ran since before the run began."""
    services = json.loads(proxy.get("/api/services")).get("data") or []
    since = up_since()
    if since is None or since > w.run:
        raise CaptureError(f"the store runs since {since}, after the run began: what it held before is lost")
    found_traces = 0

    def lines() -> Iterator[str]:
        nonlocal found_traces
        for svc in services:
            span = f"start={int(w.start.timestamp() * 1e6)}&end={int(w.end.timestamp() * 1e6)}"
            q = f"/api/traces?service={quote(svc, safe='')}&{span}&limit=500"
            if data := json.loads(proxy.get(q)).get("data") or []:
                found_traces += len(data)
                yield json.dumps({"service": svc, "traces": data}, separators=(",", ":"))

    def verify(path: Path) -> dict[str, Any]:
        held = sum(len(line["traces"]) for line in read_lines(path))
        if held != found_traces:
            raise RuntimeError(f"{path.name} holds {held} traces of {found_traces}")
        if not held:
            raise NothingHeld
        return {"traces": held}

    rel = "observability/jaeger.jsonl.gz"
    try:
        found = write_lines(dest / rel, lines(), JAEGER_MAX_BYTES, verify)
    except NothingHeld:
        return {"status": "empty", "services": len(services), "detail": "no service sent a trace in the window"}
    return {"status": "ok", "files": [rel], "services": len(services), **found}


class NothingHeld(Exception):
    """A capture read everything and there was nothing: no file is kept."""


# ---- pod logs ------------------------------------------------------------------------------------------------------


def capture_logs(
    kube_cmd: Callable[..., list[str]], namespaces: list[str], dest: Path, since: str, deadline: Deadline
) -> dict[str, Any]:
    """Logs of every container of every pod in the namespaces since the window start (and the previous container of
    a restarted one), one gzip file per pod that logged. ``silent`` pods logged nothing in the window; ``unread`` ones
    did not answer (a container that has not started has no log) and are named with the reason."""
    pods: list[tuple[str, str]] = []
    for ns in namespaces:
        cmd = kube_cmd("-n", ns, "get", "pods", "-o", "json")
        proc = subprocess.run(cmd, capture_output=True, timeout=deadline.within(TIMEOUT_S), check=False)
        if proc.returncode != 0:
            raise RuntimeError(f"list pods in {ns}: {proc.stderr.decode(errors='replace').strip()[-200:]}")
        pods += [(ns, pod["metadata"]["name"]) for pod in json.loads(proc.stdout).get("items", [])]

    def read(pod: tuple[str, str]) -> tuple[bytes, str]:
        ns, name = pod
        chunks, why = [], ""
        for flag in ((), ("--previous",)):
            args = ["-n", ns, "logs", name, "--all-containers", "--prefix", "--timestamps", *flag]
            if since:
                args.append(f"--since-time={since}")
            r = subprocess.run(
                kube_cmd(*args), capture_output=True, timeout=deadline.within(TIMEOUT_S * 3), check=False
            )
            if r.returncode == 0:
                chunks.append(r.stdout)
            elif not flag:
                why = r.stderr.decode(errors="replace").strip()[-160:] or f"exit {r.returncode}"
        return b"".join(chunks), why

    files: list[str] = []
    silent: list[str] = []
    unread: dict[str, str] = {}
    total = 0
    with ThreadPoolExecutor(max_workers=LOG_WORKERS) as pool:
        for (ns, name), (text, why) in zip(pods, pool.map(read, pods), strict=True):
            if why:
                unread[f"{ns}/{name}"] = why
            if not text:
                if not why:
                    silent.append(f"{ns}/{name}")
                continue
            rel = f"applogs/{ns}/{name}.log.gz"
            ce.atomic_write_gzip(dest / rel, text)
            if gzip.decompress((dest / rel).read_bytes()) != text:
                raise RuntimeError(f"{rel} does not read back")
            files.append(rel)
            total += (dest / rel).stat().st_size
            if total > LOGS_MAX_BYTES:
                raise CaptureError(f"pod logs exceed their {LOGS_MAX_BYTES} byte bound at {ns}/{name}")
    out: dict[str, Any] = {"files": files, "bytes": total, "silent": silent, "unread": unread}
    if files:
        return {"status": "ok", **out}
    return {"status": "error" if unread else "empty", "detail": f"{len(pods)} pods, {len(unread)} unread", **out}


# ---- all -----------------------------------------------------------------------------------------------------------


def capture_all(
    kube_cmd: Callable[..., list[str]],
    control: str,
    app: str,
    dest: Path,
    start: str,
    end: str,
    *,
    since: str = "",
    seconds: float = CAPTURE_S,
) -> dict[str, Any]:
    """Run every observability capture within ``seconds``; each records its own status in the returned receipt, which
    is on disk from the start (a capture not reached is an error) and again after every capture."""
    w = window(start, end, since)
    whole = Deadline(seconds)
    stamp = "%Y-%m-%dT%H:%M:%SZ"
    receipt: dict[str, Any] = {"window": [w.start.strftime(stamp), w.end.strftime(stamp)]}
    receipt |= {name: {"status": "error", "detail": "collect did not reach it"} for name in ce.CAPTURES}
    ce.atomic_write_json(dest / ce.OBSERVABILITY, receipt)

    def run(name: str, step: Callable[[Deadline], dict[str, Any]]) -> None:
        deadline = whole.share(SHARE[name])
        # Each capture only reads and writes its files atomically, so a transient failure (the store's pod
        # restarting, an API server stream error) is retried before it fails the cell's evidence.
        for attempt in range(1, ATTEMPTS + 1):
            try:
                receipt[name] = step(deadline)
                break
            except Exception as exc:
                receipt[name] = {"status": "error", "detail": str(exc)[:300]}
                final = isinstance(exc, CaptureError) or deadline.left() <= RETRY_AFTER_S
                if attempt == ATTEMPTS or final:
                    break
                _pause(RETRY_AFTER_S)
        ce.atomic_write_json(dest / ce.OBSERVABILITY, receipt)

    def store(service: tuple[str, str], read: Callable[[Proxy], dict[str, Any]], deadline: Deadline) -> dict[str, Any]:
        if not service_exists(kube_cmd, control, service[0]):
            return {"status": "absent", "detail": f"no service {service[0]} in {control}"}
        return read(Proxy(kube_cmd, control, service, deadline))

    run("prometheus", lambda d: store(PROMETHEUS, lambda p: capture_prometheus(p, dest, w), d))
    run(
        "jaeger",
        lambda d: store(
            JAEGER, lambda p: capture_jaeger(p, dest, w, lambda: running_since(kube_cmd, control, JAEGER[0], d)), d
        ),
    )
    run("applogs", lambda d: capture_logs(kube_cmd, [app, control], dest, receipt["window"][0], d))
    return receipt
