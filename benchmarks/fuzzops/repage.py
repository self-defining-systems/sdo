"""When the plain-agent arm is paged again: users harmed anew after a clean stretch.

SDO keeps watching and opens a new incident for harm that follows a recovery, so a one-shot agent would be compared
with an always-on operator. The rule here reads only FuzzOps' live traffic stream (``FUZZOPS_TRAFFIC``: the run's own
workload and the grader's bad-second rule), never the fault: a harm episode ends once users had no bad second for
:data:`QUIET_SECONDS`, and a bad second after that starts a new one. One page per episode, and only while the episode
is still going when no agent is working.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

#: SDO's default ``detection.stability_window``: how long its alerts must stay clear before an incident is Recovered.
QUIET_SECONDS = 15.0
#: FuzzOps publishes a sample every second until the window ends; this long without one means it ended.
STALL_SECONDS = 15.0
POLL_SECONDS = 0.25

ONSET = "onset"
WINDOW_OVER = "window over"
COLLECTED = "collected"
UNSUPPORTED = "unsupported"
PAGE_LIMIT = "page limit"


def page_limit(fault: str) -> int:
    """The faults of a FuzzOps problem ID plus one. A composite lists its members as ``name[a&b&...]``."""
    members = fault.partition("[")[2].rpartition("]")[0]
    faults = [m for m in members.split("&") if m and not m.startswith("decoy")]
    return max(len(faults), 1) + 1


def parse_time(value: str) -> datetime:
    """A Go RFC 3339 timestamp (``Z``, up to nanoseconds)."""
    text = re.sub(r"\.(\d+)", lambda m: "." + m[1][:6].ljust(6, "0"), value.replace("Z", "+00:00"))
    return datetime.fromisoformat(text)


@dataclass
class Harm:
    """The harm episodes seen in the traffic samples, fed in order."""

    quiet: float = QUIET_SECONDS
    #: The first bad second of the newest episode, and the newest bad second.
    episode: datetime | None = None
    last_bad: datetime | None = None
    #: Whether the newest episode is still going: its quiet period has not passed.
    harmed: bool = False
    #: The episode of the latest page, and whether users have had a quiet period since.
    paged: datetime | None = None
    recovered: bool = False
    #: A FuzzOps pin without ``last_bad`` stops publishing at the first bad second: no later onset can be seen.
    unsupported: bool = False

    def observe(self, sample: Mapping[str, Any]) -> None:
        if "last_bad" not in sample:
            self.unsupported = True
            return
        quiet = timedelta(seconds=self.quiet)
        bad = sample["last_bad"]
        if bad:
            at = parse_time(bad["Start"])
            if self.last_bad is None:
                self.episode = parse_time(sample["first_bad"]["Start"])
            elif at - self.last_bad - timedelta(seconds=1) >= quiet:
                # The quiet period passed between two bad seconds, perhaps between two samples read at once.
                self.recovered = self.recovered or self.paged is not None
                self.episode = at
            if self.last_bad is None or at > self.last_bad:
                self.last_bad = at
        if self.last_bad is None:
            return
        # A bucket covers one second; ``through`` is the exclusive end of what has settled.
        self.harmed = parse_time(sample["through"]) - self.last_bad - timedelta(seconds=1) < quiet
        if not self.harmed and self.paged is not None:
            self.recovered = True

    def page(self) -> None:
        self.paged, self.recovered = self.episode, False

    def new_onset(self) -> bool:
        """Users are harmed in an episode that began after the one last paged."""
        return self.harmed and self.paged is not None and self.episode is not None and self.episode > self.paged


class Traffic:
    """The complete lines of the traffic stream, each returned once."""

    def __init__(self, path: Path):
        self.path = path
        self.offset = 0

    def drain(self) -> list[dict[str, Any]]:
        samples = []
        with self.path.open("rb") as source:
            source.seek(self.offset)
            for line in source:
                if not line.endswith(b"\n"):
                    break
                self.offset += len(line)
                samples.append(json.loads(line))
        return samples


def await_onset_again(
    traffic: Traffic,
    harm: Harm,
    deadline: float,
    stopped: Callable[[], bool],
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Wait for a new onset: :data:`ONSET`, or why there is nothing more to watch.

    A page needs a live window: the newest sample was observed within the stall bound. Samples that piled up while an
    agent worked are read first, so an episode that ended meanwhile pages nobody."""
    heard = clock()
    observed: float | None = None
    while True:
        if stopped():
            return COLLECTED
        samples = traffic.drain()
        for sample in samples:
            harm.observe(sample)
            observed = parse_time(sample["observed_at"]).timestamp()
        if harm.unsupported:
            return UNSUPPORTED
        now = clock()
        if samples:
            heard = now
        if now >= deadline or now - heard >= STALL_SECONDS:
            return WINDOW_OVER
        if observed is not None and now - observed < STALL_SECONDS and harm.new_onset():
            return ONSET
        sleep(POLL_SECONDS)
