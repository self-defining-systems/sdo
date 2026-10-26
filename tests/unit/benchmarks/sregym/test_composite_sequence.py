from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.analysis.composite_sequence import first_activations, learned_before_dispatch, load_firings

if TYPE_CHECKING:
    from pathlib import Path


def _firing(detector_class: str, fingerprint: str, relation: str, event: str = "activated") -> dict[str, object]:
    return {
        "event": event,
        "detector_class": detector_class,
        "fingerprint": fingerprint,
        "dispatch_relation": relation,
    }


def test_learned_detector_before_dispatch_is_distinguished_from_health() -> None:
    firings = [
        _firing("health", "health-objective/configmap-missing/hotel-reservation/mongodb-geo", "before_dispatch"),
        _firing("incident", "required-configmap/hotel-reservation/mongodb-geo", "before_dispatch"),
        _firing(
            "health", "health-objective/network-policy/hotel-reservation/deny-all-recommendation", "after_dispatch"
        ),
    ]
    assert learned_before_dispatch(firings, "mongodb-geo")
    assert not learned_before_dispatch(firings, "recommendation")
    assert set(first_activations(firings, "recommendation")) == {"health"}


def test_cleared_events_do_not_count_as_activation() -> None:
    firings = [_firing("incident", "x/geo", "before_dispatch", event="cleared")]
    assert first_activations(firings, "geo") == {}


def test_load_firings_keeps_only_the_composites_own_firings(tmp_path: Path) -> None:
    # The controller's firing log is cumulative across a persistent sequence.
    run = tmp_path / "000_problem"
    run.mkdir()
    old = {"event": "activated", "recorded_at": "2026-10-01T07:00:00.5Z", "fingerprint": "old"}
    new = {"event": "activated", "recorded_at": "2026-10-01T07:10:00.5Z", "fingerprint": "new"}
    (run / "detector_firings.jsonl").write_text(json.dumps(old) + "\n" + json.dumps(new) + "\n", encoding="utf-8")

    assert [f["fingerprint"] for f in load_firings(tmp_path)] == ["old", "new"]
    assert [f["fingerprint"] for f in load_firings(tmp_path, since="2026-10-01T07:05:00Z")] == ["new"]


def test_sequence_uses_recorded_generated_description(tmp_path: Path, capsys) -> None:
    from benchmarks.sregym.analysis.composite_sequence import main
    from benchmarks.sregym.faults.description import FaultSpec, ProblemDescription

    problem = "mf__generated"
    (tmp_path / "composite_000_generated.json").write_text(json.dumps({"problem_id": problem}))
    (tmp_path / "incidents.jsonl").write_text(
        json.dumps(
            {
                "injection": {
                    "state": "injected",
                    "started_at": "2026-10-01T07:00:00Z",
                    "finished_at": "2026-10-01T07:00:01Z",
                }
            }
        )
    )
    description = ProblemDescription(
        problem_id=problem, composite=True, faults=(FaultSpec("target_port", "geo"),), injection_order=(0,)
    )
    (tmp_path / "fault_descriptions.json").write_text(json.dumps({problem: description.model_dump(mode="json")}))
    assert main([str(tmp_path)]) == 0
    assert "target_port:geo: learned=no" in capsys.readouterr().out


def test_sequence_excludes_not_injected_and_legacy_synthetic_times(tmp_path: Path, capsys) -> None:
    from benchmarks.sregym.analysis.composite_sequence import main

    (tmp_path / "composite_000_generated.json").write_text(json.dumps({"problem_id": "toy"}))
    for event, expected in (
        ({"state": "not_injected", "stage": "pre_injection"}, "not_injected stage=pre_injection"),
        (None, "injection_unverified"),
    ):
        row = {
            "injection_started_at": "2026-10-26T02:20:51.969907Z",
            "injection_finished_at": "2026-10-26T02:20:51.969911Z",
        }
        if event is not None:
            row["injection"] = event
        (tmp_path / "incidents.jsonl").write_text(json.dumps(row))
        assert main([str(tmp_path)]) == 0
        output = capsys.readouterr().out
        assert expected in output
        assert "learned=" not in output
