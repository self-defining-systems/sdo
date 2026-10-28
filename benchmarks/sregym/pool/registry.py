"""The lane registry: the one file that says which lanes exist, where, and what they are doing.

Every write takes an exclusive flock, validates the whole document and replaces
the file atomically, so concurrent writers serialize and a reader never sees a
partial or inconsistent registry. No other lane list exists.
"""

from __future__ import annotations

import fcntl
import os
from collections import Counter
from contextlib import contextmanager
from pathlib import Path  # noqa: TC003 - Pydantic resolves this annotation at runtime.
from typing import TYPE_CHECKING, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterator

MAX_LANES_PER_HOST = 15
LaneState = Literal["preparing", "ready", "busy", "held"]


class RegistryError(ValueError):
    """A registry write would break a lane invariant; nothing was written."""


class Lane(BaseModel):
    """One kind cluster on one host."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(pattern=r"^[a-z][a-z0-9-]{0,31}$")
    host: str = Field(min_length=1)
    run_dir: Path
    kubeconfig: Path | None = None
    #: Booked host cores: the kind node container CPU caps.
    cpus: float = Field(default=4, gt=0)
    state: LaneState
    app: str = ""
    source_digest: str = ""
    build: str = ""
    #: The pair (busy) or deploy (preparing) the lane is doing.
    work: str = ""
    reason: str = ""

    @model_validator(mode="after")
    def state_facts(self) -> Self:
        if self.state in {"ready", "busy"} and not (self.app and self.source_digest and self.build and self.kubeconfig):
            raise ValueError(f"lane {self.id}: {self.state} requires app, source digest, build and kubeconfig")
        if self.state in {"busy", "preparing"} and not self.work:
            raise ValueError(f"lane {self.id}: {self.state} requires its work id")
        if self.state == "ready" and self.work:
            raise ValueError(f"lane {self.id}: ready lanes carry no work")
        if self.state == "held" and not self.reason:
            raise ValueError(f"lane {self.id}: held requires a reason")
        return self


class Registry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    version: Literal[1] = 1
    lanes: tuple[Lane, ...] = ()

    @model_validator(mode="after")
    def invariants(self) -> Self:
        ids = Counter(lane.id for lane in self.lanes)
        if duplicates := sorted(name for name, count in ids.items() if count > 1):
            raise ValueError(f"lane ids already registered: {duplicates}")
        hosts = Counter(lane.host for lane in self.lanes)
        if crowded := sorted(host for host, count in hosts.items() if count > MAX_LANES_PER_HOST):
            raise ValueError(f"more than {MAX_LANES_PER_HOST} lanes on {crowded}")
        return self

    def lane(self, lane_id: str) -> Lane:
        for lane in self.lanes:
            if lane.id == lane_id:
                return lane
        raise RegistryError(f"pool: unknown lane {lane_id}")


class LaneRegistry:
    def __init__(self, path: Path) -> None:
        self.path = path

    def read(self) -> Registry:
        with self._locked(exclusive=False):
            return self._load()

    def _load(self) -> Registry:
        if not self.path.exists():
            return Registry()
        return Registry.model_validate_json(self.path.read_text())

    @contextmanager
    def _locked(self, *, exclusive: bool) -> Iterator[None]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.with_suffix(".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            yield

    def update(self, change: Callable[[Registry], tuple[Lane, ...]]) -> Registry:
        with self._locked(exclusive=True):
            try:
                registry = Registry(lanes=change(self._load()))
            except ValueError as error:
                raise RegistryError(f"pool: registry update refused: {error}") from error
            temporary = self.path.with_suffix(f".{os.getpid()}.tmp")
            with temporary.open("w") as handle:
                handle.write(registry.model_dump_json(indent=2) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            temporary.replace(self.path)
            return registry

    def add(self, lane: Lane) -> Lane:
        self.update(lambda registry: (*registry.lanes, lane))
        return lane

    def transition(self, lane_id: str, *, expect: Collection[LaneState], **changes: object) -> Lane:
        """Change one lane only if it is in an expected state; the whole update is validated."""
        result: list[Lane] = []

        def change(registry: Registry) -> tuple[Lane, ...]:
            current = registry.lane(lane_id)
            if current.state not in expect:
                raise RegistryError(f"pool: lane {lane_id} is {current.state}, expected {sorted(expect)}")
            updated = Lane.model_validate({**current.model_dump(), **changes})
            result.append(updated)
            return tuple(updated if lane.id == lane_id else lane for lane in registry.lanes)

        self.update(change)
        return result[0]
