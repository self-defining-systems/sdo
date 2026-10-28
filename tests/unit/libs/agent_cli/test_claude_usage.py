"""Synthetic Claude evidence shapes from TRIAL2; no recorded run data."""

from __future__ import annotations

from agentshim import default_pricing, price_for

from libs.agent_cli.claude_usage import ClaudeLedger

MODEL = "claude-opus-5-5"


def assistant(identity: str, output: int = 10, timestamp: str = "2026-10-28T00:00:01Z") -> dict:
    return {
        "type": "assistant",
        "timestamp": timestamp,
        "message": {
            "id": identity,
            "model": MODEL,
            "usage": {
                "input_tokens": 100,
                "cache_read_input_tokens": 200,
                "cache_creation_input_tokens": 30,
                "cache_creation": {"ephemeral_1h_input_tokens": 10, "ephemeral_5m_input_tokens": 20},
                "output_tokens": output,
            },
        },
    }


def state(requests: int, dollars: float) -> dict:
    return {
        "type": "cost-state",
        "totalCostUSD": dollars,
        "modelUsage": {
            MODEL: {
                "inputTokens": 100 * requests,
                "cacheReadInputTokens": 200 * requests,
                "cacheCreationInputTokens": 30 * requests,
                "outputTokens": 10 * requests,
            }
        },
    }


def ledger(*records: dict) -> ClaudeLedger:
    result = ClaudeLedger()
    for record in records:
        result.observe(record)
    return result


def test_unique_requests_replace_stream_fragments_and_export_duplicates() -> None:
    capture = ledger(assistant("a", 2), assistant("a"), assistant("a"), assistant("b"))
    bill = capture.since(ClaudeLedger(), default_pricing())
    assert bill.tokens.input_tokens == 660
    assert bill.tokens.output_tokens == 20
    assert bill.tokens.cache_write_1h_input_tokens == 20
    assert bill.requests == 2
    assert bill.usd_complete


def test_resumed_billing_is_differenced_and_final_unbilled_response_is_priced() -> None:
    before = ledger(assistant("a"), state(1, 0.00082))
    after = ledger(assistant("a"), state(1, 0.00082), assistant("b"), state(2, 0.00164), assistant("c"))
    bill = after.since(before, default_pricing())
    entry = price_for("claude", MODEL, default_pricing())
    assert entry is not None
    from agentshim import cost_usd

    # One billed response plus the final response not yet in CLI counters.
    assert (
        abs(
            bill.usd
            - (0.00082 + cost_usd(ledger(assistant("c")).since(ClaudeLedger(), default_pricing()).tokens, entry))
        )
        < 1e-10
    )
    assert bill.requests == 2
    assert bill.tokens.input_tokens == 660
    assert bill.usd_complete


def test_conflicting_or_unpriced_evidence_never_claims_complete() -> None:
    capture = ledger(assistant("a"), state(2, 0.00164))
    capture.states[-1].models[MODEL]["inputTokens"] = 50
    assert not capture.since(ClaudeLedger(), default_pricing()).usd_complete
    unknown = assistant("a")
    unknown["message"]["model"] = "unknown-model"
    assert not ledger(unknown).since(ClaudeLedger(), default_pricing()).usd_complete


def test_real_stream_capture_publishes_cancelled_usage_and_deduplicates(tmp_path, monkeypatch) -> None:
    import json

    import pytest
    from agentshim.testing import FakeRun

    from libs.agent_cli.structured import StructuredTurnError, run_structured_turn
    from tests.structured_turns import ScriptedAgent

    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude"))
    import hashlib

    session = tmp_path / "export" / "sdo_runtime" / "sessions" / hashlib.sha256(b"incident").hexdigest()[:32]
    monkeypatch.setenv("SDO_SESSION_DIR", str(session))
    checkpoints = []
    run = FakeRun(
        stdout=[
            json.dumps({"type": "system", "subtype": "init", "session_id": "synthetic"}),
            json.dumps(assistant("a")),
            json.dumps(assistant("a")),
        ],
        returncode=1,
    )
    with pytest.raises(StructuredTurnError):
        run_structured_turn(
            "claude",
            "prompt",
            output_schema={"type": "object"},
            cwd=tmp_path,
            access="danger-full-access",
            model=MODEL,
            executor=ScriptedAgent(lambda _: run).executor,
            on_usage=lambda usage, complete: checkpoints.append((usage, complete)),
        )
    assert checkpoints[-1][0]["input_tokens"] == 330
    assert checkpoints[-1][0]["model_requests"] == 1
    assert checkpoints[-1][0]["total_cost_usd"] == 0.00082
    events = [json.loads(line) for line in (session / "claude-turns.jsonl").read_text().splitlines()]
    assert [event["event"] for event in events] == ["start", "error"]
    assert events[-1]["usage"]["input_tokens"] == 330
    assert events[-1]["usd_complete"]
    from benchmarks.sregym.analysis.claude_cost import reconcile_claude_row

    row = {
        "provider": "claude",
        "model": MODEL,
        "incident_id": "incident",
        "results": str(tmp_path),
        "status": "settled",
    }
    reconcile_claude_row(row)
    assert row["cost_complete"]
    assert row["responder_usage"]["usd"] == 0.00082


def test_resumed_live_turn_ignores_cumulative_result_and_retains_counter_lag(tmp_path, monkeypatch) -> None:
    import json

    from agentshim import TokenUsage
    from agentshim.testing import FakeRun, scripted_turn

    from libs.agent_cli.structured import run_structured_turn, turn_usage
    from tests.structured_turns import ScriptedAgent

    home = tmp_path / "claude"
    transcript = home / "projects" / "synthetic" / "resumed.jsonl"
    transcript.parent.mkdir(parents=True)
    before = [assistant("a"), state(1, 0.00082)]
    transcript.write_text("".join(json.dumps(record) + "\n" for record in before))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(home))

    def respond(_):
        transcript.write_text(
            "".join(
                json.dumps(record) + "\n" for record in [*before, assistant("b"), state(2, 0.00164), assistant("c")]
            )
        )
        run = scripted_turn(
            "claude",
            session_id="resumed",
            usage=TokenUsage(input_tokens=990, output_tokens=30, turns=3),
            structured_output={"answer": "ok"},
        )
        lines = list(run.stdout)
        final = json.loads(lines[-1])
        final["total_cost_usd"] = 0.00246
        lines[-1] = json.dumps(final)
        return FakeRun(stdout=[json.dumps(assistant("b")), json.dumps(assistant("c")), *lines])

    turn = run_structured_turn(
        "claude",
        "prompt",
        output_schema={"type": "object"},
        cwd=tmp_path,
        access="danger-full-access",
        model=MODEL,
        resume_session_id="resumed",
        executor=ScriptedAgent(respond).executor,
    )
    assert turn_usage(turn)["input_tokens"] == 660
    assert turn_usage(turn)["model_requests"] == 2
    assert turn.usage.total_cost_usd == 0.00164
    assert turn.usd_complete


def test_counter_dominance_preserves_usd_and_exposes_unknown_cache_ttl() -> None:
    capture = ledger(assistant("a"), state(2, 0.00164))
    bill = capture.since(ClaudeLedger(), default_pricing())
    assert bill.usd == 0.00164
    assert bill.usd_complete
    assert not bill.ttl_complete
    assert bill.tokens.input_tokens == 660


def test_request_deduplication_and_resume_partition_sweep() -> None:
    # Sweep resume boundaries, export duplication, streaming updates and final-counter lag.
    for requests in range(1, 9):
        for split in range(requests):
            before = ledger(
                *(record for i in range(split) for record in [assistant(str(i)), state(i + 1, (i + 1) * 0.00082)])
            )
            after = ledger(
                *(
                    record
                    for i in range(requests)
                    for record in [
                        assistant(str(i), 2),
                        assistant(str(i)),
                        assistant(str(i)),
                        *([state(i + 1, (i + 1) * 0.00082)] if i < requests - 1 else []),
                    ]
                )
            )
            bill = after.since(before, default_pricing())
            assert bill.requests == requests - split
            assert bill.tokens.input_tokens == 330 * (requests - split)
            assert abs(bill.usd - 0.00082 * (requests - split)) < 1e-10
            assert bill.usd_complete
