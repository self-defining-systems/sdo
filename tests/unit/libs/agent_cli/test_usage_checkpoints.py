"""Usage is durable before a scripted turn returns, including interrupted turns."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import pytest
from agentshim import normalized_usage
from agentshim.testing import FakeRun, scripted_turn

from libs.agent_cli.structured import StructuredTurnTimeout, run_structured_turn
from libs.agent_cli.usage_checkpoint import UsageCheckpoint
from tests.structured_turns import ScriptedAgent


@pytest.mark.parametrize("provider", ["codex", "claude"])
def test_usage_checkpoint_normal_completion(provider: str, tmp_path: Path) -> None:
    checkpoint = UsageCheckpoint(tmp_path / "usage", "incident", "task", "responder")

    def worker(_request: object) -> FakeRun:
        run = scripted_turn(
            provider,
            structured_output={"answer": "ok"},
            session_id="synthetic-session",
            usage=normalized_usage(input_tokens=120, output_tokens=9, turns=1),
        )
        return run

    agent = ScriptedAgent(worker)
    run_structured_turn(
        provider,
        "prompt",
        output_schema={
            "type": "object",
            "properties": {"answer": {"type": "string"}},
            "required": ["answer"],
            "additionalProperties": False,
        },
        cwd=tmp_path,
        access="danger-full-access",
        executor=agent.executor,
        on_usage=checkpoint.publish,
    )
    observed = [json.loads(path.read_text()) for path in (tmp_path / "usage").glob("*.json")]
    assert len(observed) == 1
    assert observed[0]["task_id"] == "task"
    assert observed[0]["phase"] == "responder"
    assert observed[0]["complete"] is True
    assert observed[0]["usage"]["input_tokens"] == 120


def test_checkpoints_replace_same_turn_and_preserve_restart(tmp_path: Path) -> None:
    first = UsageCheckpoint(tmp_path, "incident", "task", "responder")
    first.publish({"input_tokens": 10, "output_tokens": 1}, False)
    first.publish({"input_tokens": 30, "output_tokens": 2}, False)
    restarted = UsageCheckpoint(tmp_path, "incident", "task", "responder")
    restarted.publish({"input_tokens": 20, "output_tokens": 3}, True)
    documents = [json.loads(path.read_text()) for path in tmp_path.glob("*.json")]
    assert len(documents) == 2
    assert sum(document["usage"]["input_tokens"] for document in documents) == 50
    assert sum(document["complete"] is False for document in documents) == 1


def test_interrupted_codex_checkpoints_before_return_and_differences_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import threading

    from agentshim import CliTimeoutError
    from agentshim.testing import FakeExecutor

    home = tmp_path / "codex"
    monkeypatch.setenv("CODEX_HOME", str(home))
    session_id = "scripted-responder"
    rollout = home / "sessions" / f"rollout-fixture-{session_id}.jsonl"
    rollout.parent.mkdir(parents=True)
    schema = {"type": "object", "properties": {}, "required": [], "additionalProperties": False}
    cumulative = 0

    def run_attempt(attempt: int, added: int, cumulative: int) -> None:
        checkpoint = UsageCheckpoint(
            tmp_path / "usage", "incident", "incident" if attempt < 2 else "incident-continue-1", "responder"
        )
        published = threading.Event()

        def persist(usage: dict[str, int | float], complete: bool) -> None:
            checkpoint.publish(usage, complete)
            published.set()

        class Worker(FakeExecutor):
            def run(self, request, sink):
                sink.stdout(json.dumps({"type": "thread.started", "thread_id": session_id}) + "\n")
                with rollout.open("a") as stream:
                    stream.write(
                        json.dumps(
                            {
                                "type": "event_msg",
                                "payload": {
                                    "type": "token_count",
                                    "info": {
                                        "total_token_usage": {
                                            "input_tokens": cumulative,
                                            "output_tokens": cumulative // 10,
                                        }
                                    },
                                },
                            }
                        )
                        + "\n"
                    )
                # The real worker is still inside session.turn here. The
                # checkpoint must exist before timeout/retirement can occur.
                assert published.wait(2), "usage was deferred until turn return"
                snapshot = json.loads((checkpoint.directory / f"{checkpoint.turn_id}.json").read_text())
                assert snapshot["usage"]["input_tokens"] == added
                assert snapshot["complete"] is False
                if attempt < 2:
                    raise CliTimeoutError(request.argv, 1.0)
                run = scripted_turn(
                    "codex",
                    session_id=session_id,
                    structured_output={},
                    usage=normalized_usage(input_tokens=cumulative, output_tokens=cumulative // 10, turns=1),
                )
                # FakeExecutor still drives the real provider parser.
                return FakeExecutor(run).run(request, sink)

        options = {"resume_session_id": session_id} if attempt else {}
        if attempt < 2:
            with pytest.raises(StructuredTurnTimeout, match="timed out"):
                run_structured_turn(
                    "codex",
                    "prompt",
                    output_schema=schema,
                    cwd=tmp_path,
                    access="danger-full-access",
                    executor=Worker([]),
                    on_usage=persist,
                    **options,
                )
        else:
            turn = run_structured_turn(
                "codex",
                "prompt",
                output_schema=schema,
                cwd=tmp_path,
                access="danger-full-access",
                executor=Worker([]),
                on_usage=persist,
                **options,
            )
            assert turn.usage.tokens.input_tokens == 40

    for attempt, added in enumerate([120, 80, 40]):
        cumulative += added
        run_attempt(attempt, added, cumulative)
    documents = [json.loads(path.read_text()) for path in (tmp_path / "usage").glob("*.json")]
    assert sum(document["usage"]["input_tokens"] for document in documents) == 240
    assert [document["complete"] for document in documents].count(False) == 2


def test_interrupted_claude_retains_incremental_assistant_usage(tmp_path: Path) -> None:
    from agentshim import CliTimeoutError
    from agentshim.testing import FakeExecutor

    checkpoint = UsageCheckpoint(tmp_path / "usage", "incident", "incident", "responder")

    class Worker(FakeExecutor):
        def run(self, request, sink):
            for input_tokens in [100, 200]:
                sink.stdout(
                    json.dumps(
                        {
                            "type": "assistant",
                            "message": {
                                "content": [{"type": "text", "text": "working"}],
                                "usage": {
                                    "input_tokens": input_tokens,
                                    "cache_read_input_tokens": 20,
                                    "cache_creation_input_tokens": 10,
                                    "output_tokens": 5,
                                },
                            },
                        }
                    )
                    + "\n"
                )
            snapshot = json.loads((checkpoint.directory / f"{checkpoint.turn_id}.json").read_text())
            assert snapshot["complete"] is False
            assert snapshot["usage"]["input_tokens"] == 360
            assert snapshot["usage"]["cache_read_input_tokens"] == 40
            assert snapshot["usage"]["llm_calls"] == 2
            assert snapshot["usage"]["model_requests"] == 2
            assert "total_cost_usd" not in snapshot["usage"]
            raise CliTimeoutError(request.argv, 1.0)

    with pytest.raises(StructuredTurnTimeout):
        run_structured_turn(
            "claude",
            "prompt",
            output_schema={"type": "object"},
            cwd=tmp_path,
            access="danger-full-access",
            executor=Worker([]),
            on_usage=checkpoint.publish,
        )


@pytest.mark.parametrize("unavailable", [True, False])
def test_checkpointed_codex_resume_requires_cumulative_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, unavailable: bool
) -> None:
    from libs.agent_cli.structured import StructuredTurnError

    home = tmp_path / "codex"
    monkeypatch.setenv("CODEX_HOME", str(home))
    if not unavailable:
        # A response count without cumulative tokens cannot define a delta.
        rollout = home / "sessions" / "rollout-fixture-resume-session.jsonl"
        rollout.parent.mkdir(parents=True)
        rollout.write_text(
            json.dumps({"payload": {"type": "token_count", "info": {"last_token_usage": {"input_tokens": 10}}}}) + "\n"
        )
    checkpoint = UsageCheckpoint(tmp_path / "usage", "incident", "incident", "responder")
    agent = ScriptedAgent(lambda _: scripted_turn("codex", session_id="resume-session", structured_output={}))
    with pytest.raises(StructuredTurnError, match="cumulative.*baseline"):
        run_structured_turn(
            "codex",
            "prompt",
            output_schema={"type": "object", "properties": {}, "required": [], "additionalProperties": False},
            cwd=tmp_path,
            access="danger-full-access",
            resume_session_id="resume-session",
            executor=agent.executor,
            on_usage=checkpoint.publish,
        )
    assert not agent.requests, "an unaccountable resumed turn must not start model work"
    assert not list((tmp_path / "usage").glob("*.json"))


def test_checkpointed_codex_resume_never_publishes_ambiguous_session_total(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from agentshim import TokenUsage

    from libs.agent_cli.structured import StructuredTurnError

    home = tmp_path / "codex"
    monkeypatch.setenv("CODEX_HOME", str(home))
    rollout = home / "sessions" / "rollout-fixture-resume-session.jsonl"
    rollout.parent.mkdir(parents=True)
    rollout.write_text(
        json.dumps(
            {
                "payload": {
                    "type": "token_count",
                    "info": {"total_token_usage": {"input_tokens": 100, "output_tokens": 10}},
                }
            }
        )
        + "\n"
    )

    def worker(_request: object) -> FakeRun:
        rollout.unlink()
        return scripted_turn(
            "codex",
            session_id="resume-session",
            structured_output={},
            usage=TokenUsage(input_tokens=150, output_tokens=15),
        )

    checkpoint = UsageCheckpoint(tmp_path / "usage", "incident", "incident", "responder")
    with pytest.raises(StructuredTurnError, match="cumulative.*unavailable"):
        run_structured_turn(
            "codex",
            "prompt",
            output_schema={"type": "object", "properties": {}, "required": [], "additionalProperties": False},
            cwd=tmp_path,
            access="danger-full-access",
            resume_session_id="resume-session",
            executor=ScriptedAgent(worker).executor,
            on_usage=checkpoint.publish,
        )
    assert not list(checkpoint.directory.glob("*.json")), "whole-session counters were billed as a new turn"


def test_checkpoint_reads_the_process_effective_codex_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from agentshim import TokenUsage

    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "unrelated-home"))
    home = tmp_path / "worker-home"
    rollout = home / "sessions" / "rollout-fixture-effective-session.jsonl"
    rollout.parent.mkdir(parents=True)

    def append_total(input_tokens: int) -> None:
        with rollout.open("a") as stream:
            stream.write(
                json.dumps(
                    {
                        "payload": {
                            "type": "token_count",
                            "info": {
                                "total_token_usage": {"input_tokens": input_tokens, "output_tokens": input_tokens // 10}
                            },
                        }
                    }
                )
                + "\n"
            )

    append_total(100)

    def worker(_request: object) -> FakeRun:
        append_total(200)
        return scripted_turn(
            "codex",
            session_id="effective-session",
            structured_output={},
            usage=TokenUsage(input_tokens=200, output_tokens=20),
        )

    checkpoint = UsageCheckpoint(tmp_path / "usage", "incident", "incident", "responder")
    turn = run_structured_turn(
        "codex",
        "prompt",
        output_schema={"type": "object", "properties": {}, "required": [], "additionalProperties": False},
        cwd=tmp_path,
        access="danger-full-access",
        resume_session_id="effective-session",
        environment={"CODEX_HOME": str(home)},
        executor=ScriptedAgent(worker).executor,
        on_usage=checkpoint.publish,
    )
    assert turn.usage.tokens.input_tokens == 100
    snapshot = json.loads((checkpoint.directory / f"{checkpoint.turn_id}.json").read_text())
    assert snapshot["complete"] is True
    assert snapshot["usage"]["input_tokens"] == 100
