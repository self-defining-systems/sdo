"""A commit series as data: where the commits come from, when they are pushed, and the faults between them.

The spec is a JSON file (see README.md). A series is a list of events, each one sequence position: a commit event pushes
one commit, a fault event injects one fault. ``Series.config`` yields the ``ReplayConfig`` for one commit so an adapter
can push commit *i*, watch SDO, then push commit *i + 1* with the replayer's own code.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from benchmarks.common.replay.replay import ReplayConfig


@dataclass(frozen=True)
class SeriesEvent:
    """One position of a series: push commit ``commit`` (1-based) or inject fault ``fault``, never both."""

    commit: int = 0
    fault: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.commit, int) or not isinstance(self.fault, str):
            raise TypeError("an event's commit is an int and its fault a string")
        if (self.commit != 0) == (self.fault != ""):
            raise ValueError("an event names exactly one of commit or fault (a non-empty fault id)")

    @property
    def kind(self) -> Literal["commit", "fault"]:
        return "commit" if self.commit else "fault"

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> SeriesEvent:
        if not isinstance(raw, dict) or len(raw) != 1 or not set(raw) <= {"commit", "fault"}:
            raise ValueError(f"an event names exactly one of commit or fault, not {raw!r}")
        if "fault" in raw and not raw["fault"]:
            raise ValueError("a fault event needs a fault id")
        return cls(commit=int(raw.get("commit", 0)), fault=str(raw.get("fault", "")))


@dataclass(frozen=True)
class Series:
    name: str
    description: str
    source: str
    repo: str
    base: str
    commits: tuple[str, ...]
    mode: Literal["diff", "tree"] = "diff"
    on_conflict: Literal["stop", "skip"] = "stop"
    #: A fault event after a release the subject held: ``stop`` runs no fault (the series ends there), ``continue`` runs
    #: it and labels its record as not comparable.
    on_held: Literal["stop", "continue"] = "stop"
    #: One offset (seconds from the series start) per commit; it times commit events only.
    schedule: tuple[float, ...] = ()
    #: The short form of ``events``: 1-based commit index -> fault id, a fault event right after that commit.
    faults: dict[int, str] = field(default_factory=dict)
    #: The positions in order. Empty: one commit event per commit, each followed by its ``faults`` entry, if any.
    events: tuple[SeriesEvent, ...] = ()

    def __post_init__(self) -> None:
        if not self.name or not self.description or not self.source:
            raise ValueError("a series needs name, description and source (why it is realistic, where it comes from)")
        object.__setattr__(self, "commits", tuple(self.commits))
        object.__setattr__(self, "schedule", tuple(self.schedule))
        for index in self.faults:
            if not 1 <= index <= len(self.commits):
                raise ValueError(f"fault point {index} is outside the series (1..{len(self.commits)})")
        if self.events and self.faults:
            raise ValueError("give events or faults, not both (faults is the short form of events)")
        if not self.events:
            expanded: list[SeriesEvent] = []
            for index in range(1, len(self.commits) + 1):
                expanded.append(SeriesEvent(commit=index))
                if index in self.faults:
                    expanded.append(SeriesEvent(fault=self.faults[index]))
            object.__setattr__(self, "events", tuple(expanded))
        object.__setattr__(self, "events", tuple(self.events))
        pushed = [e.commit for e in self.events if e.kind == "commit"]
        if outside := [i for i in pushed if not 1 <= i <= len(self.commits)]:
            raise ValueError(f"event commit {outside[0]} is outside the series (1..{len(self.commits)})")
        if sorted(pushed) != list(range(1, len(self.commits) + 1)):
            raise ValueError("events must push every commit exactly once")
        if pushed != sorted(pushed):
            raise ValueError("events must push the commits in order")
        if self.schedule and len(self.schedule) != len(self.commits):
            raise ValueError("schedule must give one offset (seconds) per commit")
        if self.on_held not in ("stop", "continue"):
            raise ValueError(f"on_held must be stop or continue, not {self.on_held!r}")
        if any(t < 0 for t in self.schedule) or list(self.schedule) != sorted(self.schedule):
            raise ValueError("schedule offsets must be non-negative and non-decreasing")
        # ReplayConfig validates commits, schedule, mode and conflict policy; build one now to fail at load time.
        self.config(1, "unused", "series-validation")

    def config(self, index: int, branch_remote: str, branch: str) -> ReplayConfig:
        """The replay of commit ``index`` (1-based) alone, scheduled by the adapter."""
        if not 1 <= index <= len(self.commits):
            raise ValueError(f"commit index {index} is outside the series (1..{len(self.commits)})")
        prev = self.base if index == 1 else self.commits[index - 2]
        return ReplayConfig(
            branch_remote=branch_remote,
            branch=branch,
            upstream_repo=self.repo,
            commits=(self.commits[index - 1],),
            base=prev,
            mode=self.mode,
            on_conflict=self.on_conflict,
        )

    def event(self, position: int) -> SeriesEvent:
        """The event at sequence ``position`` (1-based)."""
        if not 1 <= position <= len(self.events):
            raise ValueError(f"position {position} is outside the series' events (1..{len(self.events)})")
        return self.events[position - 1]

    def commits_before(self, position: int) -> int:
        """How many commits the series has pushed once ``position`` has run (the commit a fault event follows)."""
        return sum(1 for e in self.events[:position] if e.kind == "commit")

    @property
    def fault_ids(self) -> tuple[str, ...]:
        """Every fault the series injects, in order of first use."""
        return tuple(dict.fromkeys(e.fault for e in self.events if e.kind == "fault"))

    def offset(self, index: int) -> float:
        """Seconds from the series start at which commit ``index`` is pushed (0 with no schedule)."""
        return self.schedule[index - 1] if self.schedule else 0.0

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Series:
        known = {
            "name",
            "description",
            "source",
            "upstream",
            "mode",
            "on_conflict",
            "on_held",
            "schedule",
            "faults",
            "events",
        }
        if extra := sorted(set(raw) - known):
            raise ValueError(f"unknown series field(s): {', '.join(extra)}")
        up = raw.get("upstream") or {}
        if extra := sorted(set(up) - {"repo", "base", "commits"}):
            raise ValueError(f"unknown upstream field(s): {', '.join(extra)}")
        return cls(
            name=raw.get("name", ""),
            description=raw.get("description", ""),
            source=raw.get("source", ""),
            repo=up.get("repo", ""),
            base=up.get("base", ""),
            commits=tuple(up.get("commits", ())),
            mode=raw.get("mode", "diff"),
            on_conflict=raw.get("on_conflict", "stop"),
            on_held=raw.get("on_held", "stop"),
            schedule=tuple(raw.get("schedule", ())),
            faults={int(k): v for k, v in raw.get("faults", {}).items()},
            events=tuple(SeriesEvent.from_dict(e) for e in raw.get("events", ())),
        )

    @classmethod
    def load(cls, path: Path) -> Series:
        try:
            return cls.from_dict(json.loads(Path(path).read_text()))
        except (OSError, ValueError) as exc:
            raise ValueError(f"series spec {path}: {exc}") from exc
