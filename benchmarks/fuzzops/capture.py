"""Collect-time capture of what a cell's evidence needs beyond the evidence sink: build identity, host load, the
observability stores for the window (Prometheus series, Jaeger traces) and the application pods' logs.

Every file is written atomically (``cellevidence.atomic_write``), compressed when large, and bounded by
``BUDGET_BYTES`` per cell so a cell never grows without limit. ``observability.json`` is the receipt the validator
reads: per capture ``status`` ``ok`` (with ``files``), ``absent`` (the stack is not installed) or ``error``.
Nothing here is under the repository; the files live in the run directory in sdo-exp.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from benchmarks.fuzzops import cellevidence as ce

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

BUDGET_BYTES = 64 * 1024 * 1024
PROMETHEUS = ("sdo-prometheus", "9090")
JAEGER = ("sdo-jaeger", "16686")
STEP_S = 15
LEAD = timedelta(minutes=5)
SKIP_METRICS = re.compile(r"^(go_|process_|promhttp_|net_conntrack_|prometheus_(tsdb|engine|sd|rule|notifications)_)")
TIMEOUT_S = 60
ATTEMPTS = 3
RETRY_AFTER_S = 15


def _pause(seconds: float) -> None:
    time.sleep(seconds)


SERIES_POSITION_FILE = "series-position.json"


@dataclass(frozen=True)
class CapturePlan:
    """What a collect step captures of the observability stores: ``defer`` leaves Prometheus and Jaeger to a later
    position, and ``start`` is where the captured window begins."""

    defer: bool
    start: str


def write_series_position(lane_dir: Path, *, position: int, total: int, started: str) -> None:
    """The series adapter tells the collect step where in the series the run is: the evidence of one series needs one
    capture of the stores, over the whole series, taken by its last position."""
    (lane_dir / SERIES_POSITION_FILE).write_text(
        json.dumps({"position": position, "total": total, "started": started}) + "\n"
    )


def capture_plan(lane_dir: Path, *, start: str) -> CapturePlan:
    path = lane_dir / SERIES_POSITION_FILE
    if not path.is_file():
        return CapturePlan(defer=False, start=start)
    marker = json.loads(path.read_text())
    if marker["position"] < marker["total"]:
        return CapturePlan(defer=True, start=start)
    return CapturePlan(defer=False, start=marker["started"])


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


def window(start: str, end: str) -> tuple[datetime, datetime]:
    s, e = _parse(start or ""), _parse(end or "")
    e = e or datetime.now(UTC)
    s = (s or e - timedelta(minutes=15)) - LEAD
    return s, e


class Proxy:
    """Reads a Service of the control namespace through the API server proxy (no port-forward, no pod exec)."""

    def __init__(self, kube_cmd: Callable[..., list[str]], namespace: str, service: tuple[str, str]) -> None:
        self.kube_cmd, self.namespace, self.service = kube_cmd, namespace, service

    def get(self, path: str) -> bytes:
        raw = f"/api/v1/namespaces/{self.namespace}/services/{self.service[0]}:{self.service[1]}/proxy{path}"
        proc = subprocess.run(self.kube_cmd("get", "--raw", raw), capture_output=True, timeout=TIMEOUT_S, check=False)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.decode(errors="replace").strip()[-200:])
        return proc.stdout


def service_exists(kube_cmd: Callable[..., list[str]], namespace: str, name: str) -> bool:
    proc = subprocess.run(
        kube_cmd("-n", namespace, "get", "service", name, "-o", "name"), capture_output=True, timeout=30, check=False
    )
    return proc.returncode == 0


def capture_prometheus(proxy: Proxy, dest: Path, start: datetime, end: datetime, budget: int) -> dict[str, Any]:
    """Every metric's series over the window (minus runtime noise), one gzip JSON-lines file: {metric, result}."""
    names = json.loads(proxy.get("/api/v1/label/__name__/values"))["data"]
    wanted = [n for n in names if not SKIP_METRICS.match(n)]

    def one(name: str) -> str:
        q = f"/api/v1/query_range?query={quote(name)}&start={start.timestamp()}&end={end.timestamp()}&step={STEP_S}"
        data = json.loads(proxy.get(q))
        return json.dumps({"metric": name, "result": data["data"]["result"]}, separators=(",", ":"))

    with ThreadPoolExecutor(max_workers=4) as pool:
        lines = list(pool.map(one, wanted))
    body = ("\n".join(lines) + "\n").encode()
    rel = "observability/prometheus.jsonl.gz"
    ce.atomic_write_gzip(dest / rel, body)
    size = (dest / rel).stat().st_size
    if size > budget:
        raise RuntimeError(f"prometheus capture {size} bytes exceeds the {budget} byte budget")
    return {"status": "ok", "files": [rel], "metrics": len(wanted), "bytes": size}


def capture_jaeger(proxy: Proxy, dest: Path, start: datetime, end: datetime, budget: int) -> dict[str, Any]:
    services = json.loads(proxy.get("/api/services")).get("data") or []
    lines = []
    for svc in services:
        span = f"start={int(start.timestamp() * 1e6)}&end={int(end.timestamp() * 1e6)}"
        q = f"/api/traces?service={quote(svc)}&{span}&limit=500"
        data = json.loads(proxy.get(q)).get("data") or []
        lines.append(json.dumps({"service": svc, "traces": data}, separators=(",", ":")))
    rel = "observability/jaeger.jsonl.gz"
    ce.atomic_write_gzip(dest / rel, ("\n".join(lines) + "\n").encode())
    size = (dest / rel).stat().st_size
    if size > budget:
        raise RuntimeError(f"jaeger capture {size} bytes exceeds the {budget} byte budget")
    return {"status": "ok", "files": [rel], "services": len(services), "bytes": size}


def capture_logs(
    kube_cmd: Callable[..., list[str]], namespaces: list[str], dest: Path, since: str, budget: int
) -> dict[str, Any]:
    """Logs of every container of every pod in the namespaces since the window start (and the previous container of
    a restarted one), one gzip file per pod."""
    files: list[str] = []
    total = 0
    for ns in namespaces:
        proc = subprocess.run(
            kube_cmd("-n", ns, "get", "pods", "-o", "json"), capture_output=True, timeout=TIMEOUT_S, check=False
        )
        if proc.returncode != 0:
            raise RuntimeError(f"list pods in {ns}: {proc.stderr.decode(errors='replace').strip()[-200:]}")
        for pod in json.loads(proc.stdout).get("items", []):
            name = pod["metadata"]["name"]
            chunks = []
            for flag in ((), ("--previous",)):
                args = ["-n", ns, "logs", name, "--all-containers", "--prefix", "--timestamps", *flag]
                if since:
                    args.append(f"--since-time={since}")
                r = subprocess.run(kube_cmd(*args), capture_output=True, timeout=TIMEOUT_S * 3, check=False)
                if r.returncode == 0:
                    chunks.append(r.stdout)
            rel = f"applogs/{ns}/{name}.log.gz"
            ce.atomic_write_gzip(dest / rel, b"".join(chunks))
            files.append(rel)
            total += (dest / rel).stat().st_size
            if total > budget:
                raise RuntimeError(f"pod logs exceed the {budget} byte budget at {ns}/{name}")
    return {"status": "ok", "files": files, "bytes": total}


def capture_all(
    kube_cmd: Callable[..., list[str]], control: str, app: str, dest: Path, start: str, end: str, *, defer: bool = False
) -> dict[str, Any]:
    """Run every observability capture; each records its own status in the returned receipt (also written)."""
    s, e = window(start, end)
    receipt: dict[str, Any] = {"window": [s.strftime("%Y-%m-%dT%H:%M:%SZ"), e.strftime("%Y-%m-%dT%H:%M:%SZ")]}
    left = BUDGET_BYTES

    def run(name: str, step: Callable[[], dict[str, Any]]) -> None:
        nonlocal left
        # Each capture only reads and writes its files atomically, so a transient failure (the store's pod
        # restarting, an API server stream error) is retried before it fails the cell's evidence.
        for attempt in range(1, ATTEMPTS + 1):
            try:
                receipt[name] = step()
                left -= int(receipt[name].get("bytes", 0))
                return
            except Exception as exc:
                receipt[name] = {"status": "error", "detail": str(exc)[:300]}
                if attempt < ATTEMPTS:
                    _pause(RETRY_AFTER_S)

    for key, service, fn in (("prometheus", PROMETHEUS, capture_prometheus), ("jaeger", JAEGER, capture_jaeger)):
        if defer:
            receipt[key] = {"status": "deferred", "detail": "captured with the series' last position, over the series"}
            continue
        if not service_exists(kube_cmd, control, service[0]):
            receipt[key] = {"status": "absent", "detail": f"no service {service[0]} in {control}"}
            continue
        proxy = Proxy(kube_cmd, control, service)
        run(key, lambda fn=fn, proxy=proxy: fn(proxy, dest, s, e, left))  # type: ignore[misc]
    run("applogs", lambda: capture_logs(kube_cmd, [app, control], dest, receipt["window"][0], left))
    ce.atomic_write_json(dest / ce.OBSERVABILITY, receipt)
    return receipt
