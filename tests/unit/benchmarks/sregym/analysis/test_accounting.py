"""One accountant for both arms: unique request ids, one price table, the cache split, online and offline."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.analysis import accounting
from benchmarks.sregym.analysis.accounting import Spend

if TYPE_CHECKING:
    from pathlib import Path

MODEL = "claude-sonnet-5-5"
SID = "11111111-2222-3333-4444-555555555555"
AGENT = "a0123456789abcdef"
# sonnet-5-5 per million tokens: input 2.0, cache read 0.1, cache write 2.5, output 10.0
ONE = {"input_tokens": 100, "cache_read_input_tokens": 1000, "cache_creation_input_tokens": 200, "output_tokens": 50}
ONE_USD = 100 * 2e-6 + 1000 * 0.1e-6 + 200 * 2.5e-6 + 50 * 10e-6


def _t(second: int) -> str:
    return f"2026-01-01T00:00:{second:02d}Z"


def _assistant(message_id: str, second: int, usage: dict[str, int] | None = None, model: str = MODEL) -> dict[str, Any]:
    return {
        "type": "assistant",
        "timestamp": _t(second),
        "uuid": f"u-{message_id}-{second}",
        "message": {"id": message_id, "model": model, "usage": usage or ONE},
    }


def _write(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r) + "\n" for r in records))


def _session(home: Path, records: list[dict[str, Any]], sidechain: list[dict[str, Any]] | None = None) -> None:
    main = home / "projects" / "p" / f"{SID}.jsonl"
    _write(main, records)
    if sidechain is not None:
        _write(home / "projects" / "p" / SID / "subagents" / f"agent-{AGENT}.jsonl", sidechain)


def test_a_request_is_one_message_id_and_a_resumed_stream_is_not_counted_twice(tmp_path: Path) -> None:
    _session(
        tmp_path,
        # the same API response written twice while streaming: the later one has the final output count
        [_assistant("m1", 1, {**ONE, "output_tokens": 5}), _assistant("m1", 2), _assistant("m2", 3)],
        sidechain=[_assistant("s1", 4), _assistant("m2", 5)],
    )
    spend = accounting.spend_of(accounting.ledger_of(accounting.transcript_files(tmp_path)), None)
    assert spend.requests == 3, "m1, m2 and the sub-agent's s1; the repeated ids are one request each"
    assert (spend.uncached_input, spend.cache_read, spend.cache_write, spend.output) == (300, 3000, 600, 150)
    assert spend.usd == pytest.approx(3 * ONE_USD)


def test_usd_comes_from_the_price_table_and_an_unpriced_model_is_unknown_not_zero(tmp_path: Path) -> None:
    _session(tmp_path, [_assistant("m1", 1), _assistant("m2", 2, model="not-a-model")])
    spend = accounting.spend_of(accounting.ledger_of(accounting.transcript_files(tmp_path)), None)
    assert spend.requests == 2
    assert spend.usd is None
    assert spend.unpriced_requests == 1


def test_a_vertex_versioned_model_id_prices_like_its_family() -> None:
    assert accounting.normalize_model("claude-sonnet-5-5@20260101") == "claude-sonnet-5-5"
    accounting.require_price("claude-sonnet-5-5@20260101")
    with pytest.raises(accounting.AccountingError, match="no price"):
        accounting.require_price("claude-unknown-9")


def test_a_window_splits_online_from_offline_by_request_time(tmp_path: Path) -> None:
    _session(tmp_path, [_assistant("m1", 1), _assistant("m2", 2), _assistant("m3", 10), _assistant("m4", 11)])
    ledger = accounting.ledger_of(accounting.transcript_files(tmp_path))
    cut = accounting.epoch(_t(10))
    online = accounting.spend_of(ledger, None, end=cut)
    offline = accounting.spend_of(ledger, None, start=cut)
    assert (online.requests, offline.requests) == (2, 2)
    assert online + offline == accounting.spend_of(ledger, None)


def _incident(evidence: Path, name: str, incident_id: str, *, cut: str | None, records: list[dict[str, Any]]) -> Path:
    directory = evidence / "incidents" / name
    directory.mkdir(parents=True)
    (directory / "incident.json").write_text(json.dumps({"incident_id": incident_id}))
    if cut:
        (directory / "ledger.json").write_text(json.dumps({"incident_id": incident_id, "reflection_dispatch_at": cut}))
    _write(directory / "claude" / "projects" / "p" / f"{SID}.jsonl", records)
    return directory


def test_the_sdo_cell_splits_the_shared_session_at_the_closing_turn_and_counts_investigators_online(
    tmp_path: Path,
) -> None:
    evidence = tmp_path / "evidence"
    _incident(
        evidence,
        "a" * 32,
        "inc-1",
        cut=_t(10),
        records=[_assistant("r1", 1), _assistant("r2", 2), _assistant("c1", 11)],
    )
    inv = evidence / "incidents" / ("b" * 32)
    inv.mkdir(parents=True)
    (inv / "incident.json").write_text(json.dumps({"incident_id": "inc-1-inv-1"}))
    _write(inv / "claude" / "projects" / "p" / "99999999-0000-0000-0000-000000000000.jsonl", [_assistant("i1", 3)])
    life = evidence / "lifecycle" / "designer-1" / "claude" / "projects" / "p"
    _write(life / "88888888-0000-0000-0000-000000000000.jsonl", [_assistant("d1", 0)])

    cell = accounting.account_sdo_cell(evidence)
    assert cell.online.requests == 3, "two responder requests and the investigator's"
    assert cell.offline.requests == 1, "the closing turn"
    assert {role: spend.requests for role, spend in cell.roles.items()} == {
        "responder": 2,
        "closing": 1,
        "investigators": 1,
    }
    assert cell.lifecycle.requests == 1, "onboarding and lifecycle agents are one-time per app, outside the cell"
    assert cell.online.usd == pytest.approx(3 * ONE_USD)


def _result(session: str, usd: float, **usage: int) -> dict[str, Any]:
    return {"type": "result", "session_id": session, "total_cost_usd": usd, "usage": usage or ONE, "num_turns": 1}


def test_the_vanilla_cell_is_accounted_exactly_like_the_sdo_cell_from_the_same_transcripts(tmp_path: Path) -> None:
    records = [_assistant("m1", 1), _assistant("m2", 2)]
    side = [_assistant("s1", 3)]
    cell_dir = tmp_path / "vanilla"
    _session(cell_dir / "session-dir", records, side)
    _write(cell_dir / "transcript.jsonl", [{"type": "system", "session_id": SID}, _result(SID, 0.01)])
    sdo = tmp_path / "evidence"
    _incident(sdo, "a" * 32, "inc-1", cut=None, records=records)
    side_path = (
        sdo / "incidents" / ("a" * 32) / "claude" / "projects" / "p" / SID / "subagents" / f"agent-{AGENT}.jsonl"
    )
    _write(side_path, side)

    vanilla = accounting.account_vanilla_cell(cell_dir)
    arm_sdo = accounting.account_sdo_cell(sdo)
    assert vanilla.online == arm_sdo.online
    assert vanilla.offline == Spend()
    assert vanilla.online.requests == 3


def test_the_streams_result_usage_is_a_cross_check_never_the_count(tmp_path: Path) -> None:
    """``result.usage`` covers only the last turn and no sub-agent; ``total_cost_usd`` includes sub-agents and CLI-side
    auxiliary requests the transcript lacks: both are compared, neither is summed."""
    cell_dir = tmp_path / "vanilla"
    _session(cell_dir / "session-dir", [_assistant("m1", 1), _assistant("m2", 2), _assistant("m3", 3)])
    _write(cell_dir / "transcript.jsonl", [_result(SID, 3 * ONE_USD + 0.001, input_tokens=1, output_tokens=1)])
    cell = accounting.account_vanilla_cell(cell_dir)
    assert cell.online.requests == 3
    assert cell.cross_check is not None
    assert cell.cross_check["cli_usd"] == pytest.approx(3 * ONE_USD + 0.001)
    assert cell.cross_check["mismatch"] is False
    _write(cell_dir / "transcript.jsonl", [_result(SID, 5.0)])
    assert accounting.account_vanilla_cell(cell_dir).cross_check["mismatch"] is True  # type: ignore[index]


def test_a_killed_vanilla_session_with_no_transcript_file_is_counted_from_its_stream_and_flagged(
    tmp_path: Path,
) -> None:
    cell_dir = tmp_path / "vanilla"
    (cell_dir / "session-dir").mkdir(parents=True)
    _write(
        cell_dir / "transcript.jsonl",
        [{"type": "system", "session_id": SID}, {**_assistant("m1", 1), "session_id": SID}],
    )
    cell = accounting.account_vanilla_cell(cell_dir)
    assert cell.online.requests == 1
    assert cell.online.partial is True
    assert any("session-dir" in note for note in cell.notes)


def test_token_row_has_the_eval_runner_field_names() -> None:
    row = Spend(uncached_input=1, cache_read=2, cache_write=3, output=4, requests=5, usd=0.5).token_row()
    assert row == {"uncached_input": 1, "cache_read": 2, "cache_write": 3, "output": 4, "requests": 5, "usd": 0.5}
