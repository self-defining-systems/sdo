"""The plan: cells per pass, recorded random order, dependencies, lane configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from benchmarks.sregym.eval.plan import AppConfig, EvalConfig, Plan, build_plan, merge_id

if TYPE_CHECKING:
    from pathlib import Path


def config(tmp_path: Path, **overrides: object) -> EvalConfig:
    values: dict[str, object] = {
        "stage": "small",
        "runs_root": tmp_path,
        "apps": (
            AppConfig(name="hotel", problems=("a", "b", "c"), sdo_lanes=("h1", "h2"), vanilla_lanes=("hv1",)),
            AppConfig(name="social", problems=("x", "y"), sdo_lanes=("s1",), vanilla_lanes=("sv1",)),
        ),
        "seed": 7,
    }
    return EvalConfig(**{**values, **overrides})  # type: ignore[arg-type]


def test_every_pass_has_one_cell_per_problem(tmp_path: Path) -> None:
    plan = build_plan(config(tmp_path))
    counts = {(c.app, c.pass_) for c in plan.cells}
    assert counts == {(a, p) for a in ("hotel", "social") for p in ("vanilla", "pass1", "pass2", "pass2-own")}
    assert len([c for c in plan.cells if c.app == "hotel" and c.pass_ == "pass1"]) == 3
    assert len(plan.cells) == (3 + 2) * 4


def test_order_is_a_seeded_permutation_recorded_in_the_plan(tmp_path: Path) -> None:
    first = build_plan(config(tmp_path))
    again = build_plan(config(tmp_path))
    assert first.order == again.order
    assert sorted(first.order["hotel"]) == ["a", "b", "c"]
    pass1 = [c.problem for c in first.cells if c.app == "hotel" and c.pass_ == "pass1"]
    assert tuple(pass1) == first.order["hotel"]


def test_snapshots_and_dependencies(tmp_path: Path) -> None:
    plan = build_plan(config(tmp_path))
    by_id = {c.id: c for c in plan.cells}
    assert by_id["hotel/pass1/a"].snapshot == "base"
    assert by_id["hotel/pass1/a"].needs == ()
    assert by_id["hotel/pass2/a"].snapshot == "merged"
    assert by_id["hotel/pass2/a"].needs == (merge_id("hotel"),)
    assert by_id["hotel/pass2-own/a"].snapshot == "own:a"
    assert by_id["hotel/pass2-own/a"].needs == ("hotel/pass1/a",)
    assert by_id["hotel/vanilla/a"].snapshot is None
    assert by_id["hotel/vanilla/a"].arm == "vanilla"
    assert by_id["hotel/vanilla/a"].needs == ()


def test_cell_directory_layout(tmp_path: Path) -> None:
    plan = build_plan(config(tmp_path))
    cell = next(c for c in plan.cells if c.id == "hotel/pass2-own/b")
    assert cell.directory(plan.config) == tmp_path / "small" / "hotel" / "pass2-own" / "b"


def test_plan_round_trips(tmp_path: Path) -> None:
    plan = build_plan(config(tmp_path))
    plan.write(tmp_path / "plan.json")
    assert Plan.load(tmp_path / "plan.json") == plan


def test_pass_two_requires_pass_one(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="pass 2 needs pass 1"):
        config(tmp_path, passes=("pass2",))


def test_selected_passes_need_their_lanes(tmp_path: Path) -> None:
    apps = (AppConfig(name="hotel", problems=("a",), sdo_lanes=("h1",)),)
    with pytest.raises(ValidationError, match="vanilla_lanes"):
        config(tmp_path, apps=apps)
    assert build_plan(config(tmp_path, apps=apps, passes=("pass1",))).cells


def test_a_lane_serves_one_kind(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="both SDO and vanilla"):
        AppConfig(name="hotel", problems=("a",), sdo_lanes=("l",), vanilla_lanes=("l",))


def test_probe_interval_stays_in_the_agreed_range(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        config(tmp_path, probe_interval_seconds=10)
