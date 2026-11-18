"""Independent user-health prober and the retroactive hold rule behind time to mitigate.

SREGym's mitigation oracle is a one-shot verdict, and SDO's own probes are SDO's. The runner therefore samples app
health itself every 2-5 s from injection to the end of the cell, and decides afterwards: users are healthy from the
start of the last unbroken run of passing samples that lasts at least ``hold_seconds`` and reaches the end of the
probing. A sampling gap breaks a run, because an unknown second is not a healthy one.
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from typing import IO, TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

MIN_INTERVAL_SECONDS = 2.0
MAX_INTERVAL_SECONDS = 5.0
DEFAULT_HOLD_SECONDS = 60.0


@dataclass(frozen=True)
class ProbeResult:
    healthy: bool
    detail: str = ""


@dataclass(frozen=True)
class ProbeSample:
    at: float
    healthy: bool
    detail: str = ""


@dataclass(frozen=True)
class HealthAssessment:
    #: Epoch seconds at which users became healthy and stayed so; ``None`` when no run lasted the hold.
    healthy_at: float | None
    #: Length of the final healthy run (0 when the last sample failed), even when shorter than the hold.
    held_seconds: float
    first_unhealthy_at: float | None
    unhealthy_seconds: float
    #: Healthy runs that broke before the final one.
    flaps: int
    #: Consecutive samples further apart than ``max_gap_seconds``.
    gaps: int
    samples: int


def assess_health(samples: Iterable[ProbeSample], *, hold_seconds: float, max_gap_seconds: float) -> HealthAssessment:
    if hold_seconds <= 0:
        raise ValueError("hold_seconds must be positive")
    if max_gap_seconds <= 0:
        raise ValueError("max_gap_seconds must be positive")
    ordered = sorted(samples, key=lambda sample: sample.at)
    first_unhealthy: float | None = None
    unhealthy = 0.0
    flaps = 0
    gaps = 0
    run_start: float | None = None
    previous: ProbeSample | None = None
    for sample in ordered:
        broken_by_gap = previous is not None and sample.at - previous.at > max_gap_seconds
        if broken_by_gap:
            gaps += 1
        if previous is not None and not previous.healthy:
            unhealthy += min(sample.at - previous.at, max_gap_seconds)
        if run_start is not None and (broken_by_gap or not sample.healthy):
            flaps += 1
            run_start = None
        if sample.healthy:
            if run_start is None:
                run_start = sample.at
        elif first_unhealthy is None:
            first_unhealthy = sample.at
        previous = sample
    held = 0.0
    healthy_at: float | None = None
    if previous is not None and previous.healthy and run_start is not None:
        held = previous.at - run_start
        if held >= hold_seconds:
            healthy_at = run_start
    return HealthAssessment(
        healthy_at=healthy_at,
        held_seconds=held,
        first_unhealthy_at=first_unhealthy,
        unhealthy_seconds=unhealthy,
        flaps=flaps,
        gaps=gaps,
        samples=len(ordered),
    )


class Prober:
    """Calls ``probe`` on a fixed cadence until stopped, appending each sample to a JSONL file as it is taken."""

    def __init__(
        self,
        probe: Callable[[], ProbeResult],
        path: Path,
        *,
        interval_seconds: float = 3.0,
        now: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not MIN_INTERVAL_SECONDS <= interval_seconds <= MAX_INTERVAL_SECONDS:
            raise ValueError(f"interval_seconds must be between {MIN_INTERVAL_SECONDS:g} and {MAX_INTERVAL_SECONDS:g}")
        self._probe = probe
        self._path = path
        self._interval = interval_seconds
        self._now = now
        self._sleep = sleep
        self._lock = threading.Lock()
        self._samples: list[ProbeSample] = []
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def samples(self) -> list[ProbeSample]:
        with self._lock:
            return list(self._samples)

    def sample_once(self, out: IO[str] | None = None) -> ProbeSample:
        at = self._now()
        try:
            result = self._probe()
        except Exception as exc:  # a probe that cannot answer is evidence of an unhealthy app, not a runner fault
            result = ProbeResult(False, f"{type(exc).__name__}: {exc}")
        sample = ProbeSample(at=at, healthy=result.healthy, detail=result.detail)
        with self._lock:
            self._samples.append(sample)
        if out is not None:
            out.write(json.dumps(asdict(sample)) + "\n")
            out.flush()
        return sample

    def run(self, stop: threading.Event) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as out:
            due = self._now()
            while True:
                self.sample_once(out)
                if stop.is_set():
                    break
                due += self._interval
                now = self._now()
                if now > due:
                    due = now  # behind schedule: never burst to catch up
                self._sleep(due - now)
            os.fsync(out.fileno())

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("prober already started")
        self._thread = threading.Thread(target=self.run, args=(self._stop,), name="eval-prober", daemon=True)
        self._thread.start()

    def stop(self) -> list[ProbeSample]:
        """End probing after one final sample and return every sample taken."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        return self.samples()


def http_probe(
    urls: Iterable[str],
    *,
    timeout_seconds: float = 2.0,
    opener: Callable[..., IO[bytes]] = urllib.request.urlopen,
) -> Callable[[], ProbeResult]:
    """A user-journey probe: every URL must answer below 400 within the timeout."""
    targets = tuple(urls)
    if not targets:
        raise ValueError("http_probe needs at least one URL")

    def probe() -> ProbeResult:
        for url in targets:
            try:
                with opener(url, timeout=timeout_seconds) as response:
                    status = getattr(response, "status", 200)
            except urllib.error.HTTPError as exc:
                return ProbeResult(False, f"{url}: HTTP {exc.code}")
            except (OSError, ValueError) as exc:
                return ProbeResult(False, f"{url}: {type(exc).__name__}: {exc}")
            if status >= 400:
                return ProbeResult(False, f"{url}: HTTP {status}")
        return ProbeResult(True)

    return probe
