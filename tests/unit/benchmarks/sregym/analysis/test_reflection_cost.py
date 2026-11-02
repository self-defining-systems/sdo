from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

from benchmarks.sregym.analysis.incident_cost import RECEIPT_NAME, TokenUsage, logged_reflection_cost
from benchmarks.sregym.analysis.reflection_cost import main, recompute_row

if TYPE_CHECKING:
    from pathlib import Path

INCIDENT = "hotel-reservation-1"
OTHER_INCIDENT = "hotel-reservation-0"


def _digest(incident_id: str) -> str:
    return hashlib.sha256(incident_id.encode()).hexdigest()[:32]


def _turn(input_tokens: int, output_tokens: int, requests: int, cost: float) -> dict[str, object]:
    return {
        "session_id": "provider-session",
        "cost_usd": cost,
        "usage": {
            "llm_calls": 1,
            "input_tokens": input_tokens,
            "cache_read_input_tokens": input_tokens // 2,
            "cached_input_tokens": input_tokens // 2,
            "output_tokens": output_tokens,
            "model_requests": requests,
        },
    }


def _write_log(run: Path, incident_id: str, turns: list[dict[str, object]]) -> None:
    log = run / "sdo_runtime" / "sessions" / _digest(incident_id) / "reflection-usage.jsonl"
    log.parent.mkdir(parents=True)
    log.write_text("".join(json.dumps(turn) + "\n" for turn in turns), encoding="utf-8")


def _row(tmp_path: Path) -> Path:
    """One row whose receipt kept only the last of three reflection turns, plus a stale session of another incident."""

    run = tmp_path / "row"
    run.mkdir()
    _write_log(run, INCIDENT, [_turn(1000, 100, 10, 0.10), _turn(2000, 200, 20, 0.20), _turn(400, 40, 4, 0.03)])
    _write_log(run, OTHER_INCIDENT, [_turn(9000, 900, 90, 0.90)])
    receipt = {
        "incident_id": INCIDENT,
        "reflection_usage": {"input_tokens": "400", "cache_read_input_tokens": "200", "output_tokens": "40"},
    }
    (run / RECEIPT_NAME).write_text(json.dumps(receipt), encoding="utf-8")
    return run


def test_logged_cost_sums_every_turn_of_only_this_incidents_session(tmp_path: Path) -> None:
    run = _row(tmp_path)
    cost = logged_reflection_cost(run, INCIDENT)
    assert cost is not None
    assert cost.usage == TokenUsage(3400, 1700, 340, requests=34)
    assert cost.turns == 3
    assert cost.session_ids == ("provider-session",)
    assert cost.cost_usd == 0.33


def test_logged_cost_is_none_without_a_log(tmp_path: Path) -> None:
    assert logged_reflection_cost(tmp_path, INCIDENT) is None


def test_recompute_row_flags_a_receipt_that_kept_only_the_last_turn(tmp_path: Path) -> None:
    row = recompute_row(_row(tmp_path))
    assert row["turns"] == 3
    assert row["recorded_input_tokens"] == 400
    assert row["recomputed_input_tokens"] == 3400
    assert row["understated"] is True


def test_cli_writes_json_for_old_rows(tmp_path: Path) -> None:
    run = _row(tmp_path)
    out = tmp_path / "out.json"
    assert main([str(run), "--json", str(out)]) == 0
    assert json.loads(out.read_text(encoding="utf-8"))[0]["recomputed_output_tokens"] == 340
