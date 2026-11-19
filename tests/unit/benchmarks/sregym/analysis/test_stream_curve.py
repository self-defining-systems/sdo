"""Stream learning-curve aggregation: excluded incidents drop out of both arms."""

from __future__ import annotations

from typing import TYPE_CHECKING

from benchmarks.sregym.analysis.stream_curve import Row, exclude_incidents, firing_columns, load_firing_stream

if TYPE_CHECKING:
    from pathlib import Path


def _row(arm: str, index: int) -> Row:
    return Row(
        arm=arm,
        index=index,
        problem_id="p",
        kind="exact",
        family="f",
        passed=True,
        ttd_s=1.0,
        ttm_s=2.0,
        raw_incl_judge_s=3.0,
        tokens_raw=10,
        tokens_weighted=5.0,
        usd=0.0,
        requests=1,
    )


def test_excluded_incident_indices_are_removed_from_every_arm() -> None:
    rows = [_row(arm, i) for arm in ("sdo", "codex") for i in range(4)]
    kept = exclude_incidents(rows, {1, 3})
    assert sorted((r.arm, r.index) for r in kept) == [("codex", 0), ("codex", 2), ("sdo", 0), ("sdo", 2)]


def _timeline_entry(detector_id: str, detector_class: str, relation: str) -> dict[str, object]:
    return {"detector_id": detector_id, "detector_class": detector_class, "relation": relation}


def test_firing_columns_report_detectors_and_booleans_from_the_receipt() -> None:
    receipt = {
        "incident_id": "demo-1",
        "detector_firing_available": True,
        "detector_timeline": [
            _timeline_entry("learned-crashloop", "incident", "before_dispatch"),
            _timeline_entry("health-objective", "health", "no_incident"),
        ],
        "learned_detector_fired_before_dispatch": True,
        "learned_detector_fired_after_dispatch": False,
        "no_learned_detector_fired": False,
    }

    columns = firing_columns(receipt, [])

    assert columns == {
        "firing_telemetry": True,
        "learned_detector_fired_before_dispatch": True,
        "learned_detector_fired_after_dispatch": False,
        "no_learned_detector_fired": False,
        "fired_detectors": "learned-crashloop:before_dispatch;health-objective:no_incident",
    }


def test_firing_columns_for_a_run_without_telemetry_are_unknown_not_false() -> None:
    columns = firing_columns({"incident_id": "demo-1"}, [])

    assert columns["firing_telemetry"] is None
    assert columns["learned_detector_fired_before_dispatch"] is None
    assert columns["learned_detector_fired_after_dispatch"] is None
    assert columns["no_learned_detector_fired"] is None
    assert columns["fired_detectors"] == ""


def test_firing_columns_fall_back_to_the_stream_when_the_receipt_lacks_a_timeline() -> None:
    stream = [
        {"event": "activated", "detector_id": "learned", "detector_class": "learned", "incident_id": ""},
        {
            "event": "batched",
            "detector_id": "learned",
            "detector_class": "learned",
            "incident_id": "demo-1",
            "dispatch_relation": "before_dispatch",
        },
        {
            "event": "activated",
            "detector_id": "late",
            "detector_class": "learned",
            "incident_id": "demo-1",
            "dispatch_relation": "after_dispatch",
        },
        {"event": "batched", "detector_id": "other", "detector_class": "learned", "incident_id": "demo-2"},
    ]

    columns = firing_columns({"incident_id": "demo-1"}, stream)

    assert columns["firing_telemetry"] is True
    assert columns["learned_detector_fired_before_dispatch"] is True
    assert columns["learned_detector_fired_after_dispatch"] is True
    assert columns["no_learned_detector_fired"] is None
    assert columns["fired_detectors"] == "learned:before_dispatch;late:after_dispatch"


def test_load_firing_stream_skips_torn_lines_and_missing_files(tmp_path: Path) -> None:
    assert load_firing_stream(tmp_path / "absent.jsonl") == []
    stream = tmp_path / "detector_firings.jsonl"
    stream.write_text('{"event": "activated"}\n{"event": "tor', encoding="utf-8")

    assert load_firing_stream(stream) == [{"event": "activated"}]
