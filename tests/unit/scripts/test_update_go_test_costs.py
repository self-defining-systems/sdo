"""Hosted timing imports retain real parent work and fail closed on bad evidence."""

import importlib.util
import json
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "update_go_costs", Path(__file__).resolve().parents[3] / "scripts/update_go_test_costs.py"
)
assert SPEC is not None
assert SPEC.loader is not None
costs = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(costs)


def event(action: str, second: int, test: str = "TestParent", **extra: object) -> dict[str, object]:
    return {
        "Time": f"2026-10-27T20:00:{second:02d}Z",
        "Action": action,
        "Package": "sdo.dev/operator/runtime",
        "Test": test,
        **extra,
    }


def test_parent_active_wall_time_excludes_pause_and_keeps_parallel_children() -> None:
    events = [
        event("run", 0),
        event("pause", 2),
        event("cont", 12),
        event("run", 13, "TestParent/child"),
        event("pass", 25, "TestParent/child", Elapsed=12),
        event("pass", 30, Elapsed=1),
    ]
    assert costs.measured_costs(events) == {("sdo.dev/operator/runtime", "TestParent"): 20.0}


def test_transcript_parser_uses_json_events_and_ignores_unfinished_work() -> None:
    lines = [
        "checkout noise",
        "job\tstep\t" + json.dumps(event("run", 0)),
        "job\tstep\t" + json.dumps(event("pass", 8, Elapsed=8)),
        json.dumps(event("run", 9, "TestUnfinished")),
    ]
    assert costs.measured_costs(costs.read_events(lines)) == {("sdo.dev/operator/runtime", "TestParent"): 8.0}


def test_missing_or_invalid_timing_evidence_fails_closed() -> None:
    with pytest.raises(ValueError, match="completed"):
        costs.measured_costs([event("run", 0)])
    with pytest.raises(ValueError, match="finite"):
        costs.measured_costs([event("pass", 8, Elapsed=float("nan"))])


def test_operator_only_refresh_preserves_runtime_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    profiles = tmp_path / "scripts"
    profiles.mkdir()
    runtime = profiles / "runtime_test_costs.json"
    runtime.write_text("unchanged runtime profile")
    operator = profiles / "operator_test_costs.json"
    operator.write_text(json.dumps({"costs": {"Old": 1}}))
    log = tmp_path / "events.log"
    done = event("pass", 8, Elapsed=8)
    done["Package"] = "sdo.dev/operator/broker"
    log.write_text(json.dumps(done))
    monkeypatch.setattr(costs, "ROOT", tmp_path)
    monkeypatch.setattr(
        "sys.argv", ["update", "--scope", "operator", "--log", str(log), "--run", "123", "--head", "a" * 40, "--write"]
    )
    costs.main()
    assert runtime.read_text() == "unchanged runtime profile"
    result = json.loads(operator.read_text())
    assert result["costs"]["sdo.dev/operator/broker/TestParent"] == 8


def test_subsecond_tests_remain_measured_instead_of_unknown_forever() -> None:
    done = event("pass", 0, "TestFast", Elapsed=0.02)
    assert costs.measured_costs([done]) == {("sdo.dev/operator/runtime", "TestFast"): 0.02}


def test_zero_resolution_completion_has_positive_cost_floor() -> None:
    assert costs.measured_costs([event("pass", 0, "TestInstant", Elapsed=0)]) == {
        ("sdo.dev/operator/runtime", "TestInstant"): 0.001,
    }
