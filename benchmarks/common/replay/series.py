"""A commit series as data: where the commits come from, when they are pushed, and the optional fault points.

The spec is a JSON file (see README.md). ``Series.config`` yields the ``ReplayConfig`` for one commit so an adapter
can push commit *i*, watch SDO, then push commit *i + 1* with the replayer's own code.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from benchmarks.common.replay.replay import ReplayConfig


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
    schedule: tuple[float, ...] = ()
    #: 1-based commit index -> fault id injected after that commit.
    faults: dict[int, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name or not self.description or not self.source:
            raise ValueError("a series needs name, description and source (why it is realistic, where it comes from)")
        object.__setattr__(self, "commits", tuple(self.commits))
        object.__setattr__(self, "schedule", tuple(self.schedule))
        for index in self.faults:
            if not 1 <= index <= len(self.commits):
                raise ValueError(f"fault point {index} is outside the series (1..{len(self.commits)})")
        if self.schedule and len(self.schedule) != len(self.commits):
            raise ValueError("schedule must give one offset (seconds) per commit")
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

    def offset(self, index: int) -> float:
        """Seconds from the series start at which commit ``index`` is pushed (0 with no schedule)."""
        return self.schedule[index - 1] if self.schedule else 0.0

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Series:
        known = {"name", "description", "source", "upstream", "mode", "on_conflict", "schedule", "faults"}
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
            schedule=tuple(raw.get("schedule", ())),
            faults={int(k): v for k, v in raw.get("faults", {}).items()},
        )

    @classmethod
    def load(cls, path: Path) -> Series:
        try:
            return cls.from_dict(json.loads(Path(path).read_text()))
        except (OSError, ValueError) as exc:
            raise ValueError(f"series spec {path}: {exc}") from exc
