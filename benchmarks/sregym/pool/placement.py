"""Pure placement of first+repeat pairs onto lanes; the scheduler executes the decisions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from benchmarks.sregym.pool.registry import MAX_LANES_PER_HOST

if TYPE_CHECKING:
    from collections.abc import Sequence

    from benchmarks.sregym.pool.registry import Lane

MAX_DEPLOYS_PER_HOST = 2
DEFAULT_LANE_CPUS = 4.0  # kind node container caps of one lane
DEFAULT_MAX_LOAD = 40.0


@dataclass(frozen=True)
class Pair:
    id: str
    problem: str
    app: str
    encounters: tuple[str, ...] = ("first", "repeat")


@dataclass(frozen=True)
class HostRoom:
    """One host's admission facts at this tick."""

    name: str
    #: Cores of the pool's coord lease on this host.
    budget_cores: float
    load1: float
    max_load: float = DEFAULT_MAX_LOAD

    def __post_init__(self) -> None:
        if self.budget_cores < 0 or self.max_load <= 0:
            raise ValueError("host budget must be non-negative and the load gate positive")

    @property
    def open(self) -> bool:
        return self.load1 < self.max_load


@dataclass(frozen=True)
class Run:
    pair: Pair
    lane: str


@dataclass(frozen=True)
class Deploy:
    """Deploy ``pair.app`` on an idle lane (``lane``) or on a new lane of ``host`` (``lane=None``)."""

    pair: Pair
    host: str
    lane: str | None


def place(
    pairs: Sequence[Pair],
    lanes: Sequence[Lane],
    hosts: Sequence[HostRoom],
    *,
    build: str,
    lane_cpus: float = DEFAULT_LANE_CPUS,
) -> list[Run | Deploy]:
    """Place waiting pairs in stream order.

    1. A ready lane with the pair's app on the current build runs it.
    2. A lane already deploying an app covers one waiting pair of that app.
    3. Otherwise deploy: first on an idle lane no waiting pair can use, then a
       new lane on an open host whose booked cores fit its coord budget. Each
       host has at most 2 deploys in flight and 15 lanes; no heavy step starts
       on a host at or above its load gate.
    """
    rooms = {room.name: room for room in hosts}
    idle = [lane for lane in lanes if lane.state == "ready" and rooms.get(lane.host, None) and rooms[lane.host].open]
    idle.sort(key=lambda lane: (rooms[lane.host].load1, lane.host, lane.id))
    decisions: list[Run | Deploy] = []
    waiting: list[Pair] = []
    for pair in pairs:
        match = next((lane for lane in idle if lane.app == pair.app and lane.build == build), None)
        if match is None:
            waiting.append(pair)
            continue
        idle.remove(match)
        decisions.append(Run(pair, match.id))
    covering = [lane.app for lane in lanes if lane.state == "preparing" and lane.build == build]
    uncovered: list[Pair] = []
    for pair in waiting:
        if pair.app in covering:
            covering.remove(pair.app)
        else:
            uncovered.append(pair)
    booked = {room: sum(lane.cpus for lane in lanes if lane.host == room) for room in rooms}
    counts = {room: sum(1 for lane in lanes if lane.host == room) for room in rooms}
    deploying = {room: sum(1 for lane in lanes if lane.host == room and lane.state == "preparing") for room in rooms}
    order = sorted(rooms.values(), key=lambda room: (room.load1, room.name))
    for pair in uncovered:
        open_hosts = [room.name for room in order if room.open and deploying[room.name] < MAX_DEPLOYS_PER_HOST]
        spare = next((lane for lane in idle if lane.host in open_hosts), None)
        if spare is not None:
            idle.remove(spare)
            deploying[spare.host] += 1
            decisions.append(Deploy(pair, spare.host, spare.id))
            continue
        host = next(
            (
                name
                for name in open_hosts
                if counts[name] < MAX_LANES_PER_HOST and booked[name] + lane_cpus <= rooms[name].budget_cores
            ),
            None,
        )
        if host is None:
            continue
        booked[host] += lane_cpus
        counts[host] += 1
        deploying[host] += 1
        decisions.append(Deploy(pair, host, None))
    return decisions
