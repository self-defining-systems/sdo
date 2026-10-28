"""Placement: reuse a deployed lane, else deploy where the host has room."""

from __future__ import annotations

from pathlib import Path

from benchmarks.sregym.pool.placement import Deploy, HostRoom, Pair, Run, place
from benchmarks.sregym.pool.registry import Lane


def ready(lane_id: str, host: str, app: str, build: str = "h1", cpus: float = 4) -> Lane:
    return Lane(
        id=lane_id,
        host=host,
        run_dir=Path(f"/l/{lane_id}"),
        kubeconfig=Path("/k"),
        state="ready",
        app=app,
        source_digest="sha256:a",
        build=build,
        cpus=cpus,
    )


def busy(lane_id: str, host: str, app: str) -> Lane:
    return ready(lane_id, host, app).model_copy(update={"state": "busy", "work": "p"})


def preparing(lane_id: str, host: str, app: str) -> Lane:
    return Lane(id=lane_id, host=host, run_dir=Path("/l"), state="preparing", work="prep", app=app, build="h1")


def pair(index: int, app: str) -> Pair:
    return Pair(id=f"p{index}", problem=f"problem-{index}", app=app)


def room(name: str, budget: float = 48, load1: float = 1, max_load: float = 40) -> HostRoom:
    return HostRoom(name=name, budget_cores=budget, load1=load1, max_load=max_load)


def test_prefers_a_ready_lane_with_the_app_and_build() -> None:
    lanes = [ready("a", "c5", "hotel"), ready("b", "c4", "social"), ready("old", "c5", "social", build="h0")]
    decisions = place([pair(0, "social"), pair(1, "hotel")], lanes, [room("c5"), room("c4")], build="h1")
    assert decisions == [Run(pair(0, "social"), "b"), Run(pair(1, "hotel"), "a")]


def test_redeploys_an_idle_lane_whose_app_has_no_pending_work_before_creating() -> None:
    lanes = [ready("a", "c5", "hotel"), busy("b", "c4", "social")]
    decisions = place([pair(0, "social")], lanes, [room("c5"), room("c4")], build="h1")
    assert decisions == [Deploy(pair(0, "social"), "c5", lane="a")]


def test_creates_on_the_host_with_room_when_no_lane_is_idle() -> None:
    lanes = [busy("a", "c5", "hotel")] + [busy(f"x{i}", "c4", "hotel") for i in range(11)]
    hosts = [room("c5", budget=8, load1=5), room("c4", budget=48, load1=30)]
    # c5 has 4 booked cores of 8 -> one more lane; c4 has 44 of 48 -> one more lane; lower load wins first.
    decisions = place([pair(0, "social"), pair(1, "social"), pair(2, "social")], lanes, hosts, build="h1")
    assert decisions == [Deploy(pair(0, "social"), "c5", lane=None), Deploy(pair(1, "social"), "c4", lane=None)]


def test_lanes_capped_lower_book_their_own_cores() -> None:
    lanes = [ready(f"x{i}", "c5", "hotel", cpus=2) for i in range(4)]
    lanes = [lane.model_copy(update={"state": "busy", "work": "p"}) for lane in lanes]
    decisions = place([pair(0, "social")], lanes, [room("c5", budget=12)], build="h1", lane_cpus=4)
    assert decisions == [Deploy(pair(0, "social"), "c5", lane=None)]


def test_never_more_than_fifteen_lanes_per_host() -> None:
    lanes = [busy(f"x{i}", "c5", "hotel") for i in range(15)]
    assert place([pair(0, "social")], lanes, [room("c5", budget=1000)], build="h1", lane_cpus=1) == []


def test_at_most_two_concurrent_deploys_per_host() -> None:
    lanes = [preparing("p0", "c5", "hotel"), preparing("p1", "c5", "hotel")]
    pairs = [pair(0, "hotel"), pair(1, "hotel"), pair(2, "hotel"), pair(3, "social")]
    decisions = place(pairs, lanes, [room("c5")], build="h1")
    # Each preparing lane covers one waiting pair of its app; nothing else may start deploying on c5.
    assert decisions == []
    decisions = place(pairs, lanes[:1], [room("c5")], build="h1")
    assert decisions == [Deploy(pair(1, "hotel"), "c5", lane=None)]


def test_load_gate_blocks_heavy_steps() -> None:
    lanes = [ready("a", "c5", "hotel")]
    assert place([pair(0, "hotel"), pair(1, "social")], lanes, [room("c5", load1=41)], build="h1") == []


def test_keeps_each_app_in_stream_order() -> None:
    lanes = [ready("a", "c5", "hotel")]
    decisions = place([pair(3, "hotel"), pair(7, "hotel")], lanes, [room("c5", budget=4)], build="h1")
    assert decisions == [Run(pair(3, "hotel"), "a")]
