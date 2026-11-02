"""Synthetic cost states and metric-specific pair denominators."""

from __future__ import annotations

import pytest

from benchmarks.sregym.analysis.stream_accounting import RoleCost, summarize_pairs


@pytest.mark.parametrize(
    ("usage", "state", "usd"),
    [
        ({}, "missing", None),
        ({"usd": 0, "usd_complete": False}, "missing", None),
        ({"usd": 0.2, "usd_complete": False}, "missing", None),
        ({"usd": 0.2, "usd_complete": True}, "measured", 0.2),
        ({"usd": 0, "usd_complete": True, "no_model_dispatched": True}, "model-free", 0),
    ],
)
def test_cost_states(usage: dict, state: str, usd: float | None) -> None:
    result = RoleCost.from_usage(usage)
    assert result.state == state
    assert result.usd == usd


def test_inconsistent_model_free_measurement_is_rejected() -> None:
    with pytest.raises(ValueError, match="model-free"):
        RoleCost("model-free", 0.2)


def row(responder: float | None, reflection: float | None, time: float = 10) -> dict:
    return {
        "status": "settled",
        "injection_started_at": "2026-10-28T00:00:00Z",
        "mount_sha": "build-a",
        "health_clear_seconds": time,
        "responder_usage": {"usd": responder, "usd_complete": responder is not None},
        "reflection_usage": {"usd": reflection, "usd_complete": reflection is not None},
    }


def test_missing_costs_excluded_per_role_and_zero_denominators_counted() -> None:
    pairs = [
        {"problem_id": "mf__toy__n1__seed", "first": row(0.2, 0.4), "repeat": row(None, 0.2)},
        {"problem_id": "mf__toy__n1__seed", "first": row(0.4, None), "repeat": row(0.2, 0.8)},
        {"problem_id": "mf__toy__n1__seed", "first": row(0, 0), "repeat": row(0, 0)},
    ]
    group = next(g for g in summarize_pairs(pairs) if g["build"] == "build-a" and g["kind"] == "single")
    responder = group["metrics"]["responder_usd"]
    assert responder["first"] == {"median": 0.2, "n": 3, "missing": 0}
    assert responder["repeat"] == {"median": 0.1, "n": 2, "missing": 1}
    assert responder["ratio"] == {"median": 0.5, "n": 1, "missing": 1, "zero_denominator": 1, "unpaired": 0}
    reflection = group["metrics"]["reflection_usd"]
    assert reflection["ratio"]["median"] == 0.5
    assert reflection["ratio"]["missing"] == 1
    assert group["metrics"]["total_usd"]["ratio"]["n"] == 0


def test_cross_build_pairs_never_count_as_within_build_ratios() -> None:
    first, repeat = row(0.2, 0.4), row(0.1, 0.2)
    repeat["mount_sha"] = "build-b"
    groups = summarize_pairs([{"problem_id": "mf__toy__n2__seed", "first": first, "repeat": repeat}])
    for group in groups:
        if group["build"] != "all":
            assert group["metrics"]["responder_usd"]["ratio"]["n"] == 0
            assert group["metrics"]["responder_usd"]["ratio"]["unpaired"] == 1


def test_non_injected_placeholders_do_not_enter_learning_statistics() -> None:
    first, repeat = row(0.2, 0.4), row(0, 0)
    repeat.pop("injection_started_at")
    groups = summarize_pairs([{"problem_id": "mf__toy__n1__seed", "first": first, "repeat": repeat}])
    group = next(g for g in groups if g["build"] == "all" and g["kind"] == "single")
    assert group["metrics"]["responder_usd"]["repeat"]["n"] == 0
    assert group["metrics"]["responder_usd"]["ratio"]["n"] == 0
