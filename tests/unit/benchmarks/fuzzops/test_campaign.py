"""Campaign lanes: round planning, the idle gate between rounds, and the table built from grades and outcomes."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.fuzzops.campaign import CampaignError, Plan, PlanItem, choose, idle_reason, instance_ids, wait_idle
from benchmarks.fuzzops.table import (
    Row,
    auto_applied,
    build_rows,
    outcomes_in,
    ratios,
    reach_ok,
    toward_best,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_plan_takes_two_instances_per_template_as_first_and_variant() -> None:
    pairs = [("scale-to-zero", "scale-to-zero@a"), ("scale-to-zero", "scale-to-zero@b"), ("cache-garbage", "g@x")]
    items = choose(pairs, None)
    assert items == [PlanItem("scale-to-zero", "scale-to-zero@a", "scale-to-zero@b"), PlanItem("cache-garbage", "g@x")]
    assert [r for r, _ in items[0].rounds()] == ["first", "repeat", "variant"]
    assert [r for r, _ in items[1].rounds()] == ["first", "repeat"]


def test_plan_drops_masked_instances_and_keeps_the_requested_order() -> None:
    pairs = [("a", "a@1"), ("a", "a@2"), ("b", "b@1")]
    assert choose(pairs, ["b", "a"], visible={"a@2", "b@1"}) == [PlanItem("b", "b@1"), PlanItem("a", "a@2")]


def test_plan_round_trips_and_rejects_a_variant_equal_to_first(tmp_path: Path) -> None:
    plan = Plan("hotel", "/i.json", (PlanItem("a", "a@1", "a@2"),))
    (tmp_path / "p.json").write_text(plan.dump())
    assert Plan.load(tmp_path / "p.json") == plan
    with pytest.raises(ValueError, match="another instance"):
        PlanItem("a", "a@1", "a@1")


def test_instance_ids_read_fuzzops_instances() -> None:
    raw = [
        {"template": "t", "target": {"id": "dependency:deploy/x:1"}, "params": {"k": "v"}},
        {"template": "u", "id": "u@y"},
    ]
    assert instance_ids(raw) == [("t", "t@dependency:deploy/x:1?k=v"), ("u", "u@y")]


@pytest.mark.parametrize(
    ("state", "why"),
    [
        (None, "no controller state yet"),
        ({"incident_open": True}, "incident open"),
        ({"pending_closure": {"request": {}}}, "closure pending"),
        ({"queued_reflection_incident_ids": ["i"]}, "reflection queued"),
        ({"queuedReflectionsUnknown": True}, "reflection queue unknown"),
        ({"version": "sdo.controller/v1"}, ""),
    ],
)
def test_idle_reason(state: dict[str, Any] | None, why: str) -> None:
    assert idle_reason(state) == why


def test_wait_idle_needs_two_idle_reads_and_times_out() -> None:
    states = iter([{"incident_open": True}, {}, {"incident_open": True}, {}, {}])
    clock = iter(range(100))
    assert wait_idle(lambda: next(states), sleep=lambda _: None, clock=lambda: next(clock), log=lambda _: None) >= 0
    with pytest.raises(CampaignError):
        wait_idle(
            lambda: {"incident_open": True},
            timeout=3,
            sleep=lambda _: None,
            clock=lambda: next(clock),
            log=lambda _: None,
        )


def test_toward_best_places_a_run_between_noop_and_best_fix() -> None:
    assert toward_best(100, 600, 50) == pytest.approx(0.91, abs=0.01)
    assert toward_best(600, 600, 50) == 0
    assert toward_best(10, 12, 10) is None  # references too close to tell


def test_auto_applied_reads_the_resolution_path() -> None:
    assert auto_applied({"resolution_path": "deterministic"}) == "yes"
    assert auto_applied({"resolution_path": "responder", "plan_applications": [{"applied": True}]}) == "partial"
    assert auto_applied({"resolution_path": "responder"}) == "no"


def test_reach_ok() -> None:
    assert reach_ok("out-of-reach", "out-of-reach") == "yes"
    assert reach_ok("in-scope", "") == "not stated"
    assert reach_ok("in-scope", "out-of-reach") == "no"


def test_outcomes_in_picks_incidents_detected_in_the_run() -> None:
    o = [
        {"incident_id": i, "timestamps": {"detected_at": t}}
        for i, t in (
            ("before", "2026-01-01T11:59:00Z"),
            ("in", "2026-01-01T12:01:00Z"),
            ("after", "2026-01-01T13:00:00Z"),
        )
    ]
    start, end = datetime(2026, 1, 1, 12, tzinfo=UTC), datetime(2026, 1, 1, 12, 30, tzinfo=UTC)
    assert [x["incident_id"] for x in outcomes_in(o, start, end)] == ["in"]


def _lane(tmp: Path) -> Path:
    lane = tmp / "lane1"
    (lane / "memory" / ".sdo" / "outcomes").mkdir(parents=True)
    usage = {"input_tokens": 1_000_000, "cached_input_tokens": 800_000, "output_tokens": 10_000}
    for i, (round_, hour, path) in enumerate((("first", 12, "responder"), ("repeat", 13, "deterministic"))):
        run = lane / "runs" / f"00{i}-t-{round_}"
        run.mkdir(parents=True)
        (run / "run.json").write_text(json.dumps({"template": "t", "round": round_, "instance": "t@x"}))
        inject = f"2026-01-01T{hour}:00:00Z"
        (run / "record.json").write_text(
            json.dumps(
                {
                    "valid": True,
                    "seed": i,
                    "started": inject,
                    "seconds": 600,
                    "window": {"BadSeconds": 100 // (i + 1)},
                    "tail": {"BadSeconds": 0},
                    "lifecycle": {"inject_at": inject},
                }
            )
        )
        (lane / "memory" / ".sdo" / "outcomes" / f"{i}.json").write_text(
            json.dumps(
                {
                    "incident_id": f"inc-{round_}",
                    "classification": "success",
                    "resolution_path": path,
                    "usage": usage if round_ == "first" else {},
                    "timestamps": {
                        "detected_at": f"2026-01-01T{hour}:00:10Z",
                        "completed_at": f"2026-01-01T{hour}:02:00Z",
                    },
                    "confirmed_root_causes": [{"cause_class": "config-value-wrong", "reach": "in-scope"}],
                }
            )
        )
    return lane


def _cell(op: str, bad: float, seed: int, recovery: float) -> dict[str, Any]:
    return {
        "Fault": "t@x",
        "Operator": op,
        "Grade": "better: cause removed",
        "Runs": 1,
        "NoopRuns": 3,
        "Bad": bad,
        "NoopBad": 600.0,
        "Lost": 0,
        "NoopLost": 0,
        "Life": {
            "Reach": "in-scope",
            "Runs": [
                {
                    "Seed": seed,
                    "DetectSec": {"V": 5, "OK": True},
                    "RecoverySec": {"V": recovery, "OK": True},
                    "UsersHurt": True,
                    "Diagnosed": "yes",
                    "Harmful": None,
                }
            ],
        },
    }


def test_build_rows_joins_grades_and_outcomes_and_ratios_compare_rounds(tmp_path: Path) -> None:
    grades = [
        _cell("exec:sdo-first", 100, 0, 120),
        _cell("exec:sdo-repeat", 50, 1, 48),
        {"Fault": "t@x", "Operator": "best-fix", "Runs": 1, "Bad": 40.0},
    ]
    rows = build_rows("hotel", "v1", [_lane(tmp_path)], grades)
    first, repeat = rows
    assert (first.round, first.bad, first.noop_bad, first.best_bad) == ("first", 100.0, 600.0, 40.0)
    assert (first.incidents, first.cause_class, first.reach_ok) == (["inc-first"], "config-value-wrong", "yes")
    assert first.cost_usd > 0
    assert (first.auto_applied, first.closed_s) == ("no", 120)
    assert (repeat.auto_applied, repeat.mitigate_s) == ("yes", 48)
    (r,) = ratios(rows)
    assert (r.round, r.time_ratio, r.cost_ratio) == ("repeat", 0.4, 0.0)


def test_rows_without_a_record_are_kept_invalid(tmp_path: Path) -> None:
    lane = _lane(tmp_path)
    (lane / "runs" / "000-t-first" / "record.json").write_text("")
    rows = build_rows("hotel", "v1", [lane], [])
    assert isinstance(rows[0], Row)
    assert (rows[0].valid, rows[0].note) == (False, "no record")
