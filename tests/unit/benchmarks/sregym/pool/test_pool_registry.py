"""The lane registry is the single, validated source of lane truth."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from benchmarks.sregym.pool.registry import MAX_LANES_PER_HOST, Lane, LaneRegistry, RegistryError


def lane(lane_id: str, host: str = "chelan5", **changes: object) -> Lane:
    values: dict[str, object] = {
        "id": lane_id,
        "host": host,
        "run_dir": Path(f"/lanes/{lane_id}"),
        "state": "preparing",
        "work": "prepare-1",
    }
    values.update(changes)
    return Lane.model_validate(values)


def ready(lane_id: str, host: str = "chelan5", app: str = "toy") -> Lane:
    return lane(
        lane_id,
        host,
        state="ready",
        work="",
        app=app,
        source_digest="sha256:a",
        build="h1",
        kubeconfig=Path(f"/lanes/{lane_id}/kubeconfig"),
    )


def test_round_trip_and_transitions(tmp_path: Path) -> None:
    registry = LaneRegistry(tmp_path / "lanes.json")
    registry.add(lane("a"))
    registry.transition(
        "a",
        expect={"preparing"},
        state="ready",
        work="",
        app="toy",
        source_digest="sha256:a",
        build="h1",
        kubeconfig=Path("/k"),
    )
    registry.transition("a", expect={"ready"}, state="busy", work="pair-0")
    reloaded = LaneRegistry(tmp_path / "lanes.json").read()
    assert [(item.id, item.state, item.work) for item in reloaded.lanes] == [("a", "busy", "pair-0")]


def test_transition_refuses_unexpected_state(tmp_path: Path) -> None:
    registry = LaneRegistry(tmp_path / "lanes.json")
    registry.add(ready("a"))
    with pytest.raises(RegistryError, match="expected"):
        registry.transition("a", expect={"busy"}, state="ready", work="")
    assert registry.read().lanes[0].state == "ready"


@pytest.mark.parametrize(
    "changes",
    [
        {"state": "ready", "work": ""},  # ready without a deployed app and source digest
        {"state": "busy", "work": "", "app": "toy", "source_digest": "d", "build": "h1"},
        {"state": "held", "work": "", "reason": ""},
        {"state": "ready", "work": "x", "app": "toy", "source_digest": "d", "build": "h1"},
    ],
)
def test_lane_states_carry_their_facts(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="lane a"):
        Lane.model_validate({"id": "a", "host": "chelan5", "run_dir": "/l", "work": "p", **changes})


def test_unique_ids_and_host_cap(tmp_path: Path) -> None:
    registry = LaneRegistry(tmp_path / "lanes.json")
    registry.add(ready("a"))
    with pytest.raises(RegistryError, match="already"):
        registry.add(ready("a", host="chelan4"))
    for index in range(1, MAX_LANES_PER_HOST):
        registry.add(ready(f"l{index}"))
    with pytest.raises(RegistryError, match="15"):
        registry.add(ready("over"))
    registry.add(ready("other", host="chelan4"))


def test_concurrent_writers_never_lose_a_lane(tmp_path: Path) -> None:
    path = tmp_path / "lanes.json"
    errors: list[BaseException] = []

    def add(index: int) -> None:
        try:
            LaneRegistry(path).add(ready(f"l{index}", host=f"h{index % 2}"))
        except BaseException as error:  # pragma: no cover - surfaced below
            errors.append(error)

    threads = [threading.Thread(target=add, args=(index,)) for index in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert len(LaneRegistry(path).read().lanes) == 20
