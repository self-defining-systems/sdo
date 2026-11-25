"""Usage is durable before a scripted turn returns, including interrupted turns."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
from agentshim import normalized_usage
from agentshim.testing import FakeRun, scripted_turn

from libs.agent_cli.structured import StructuredTurnTimeout, run_structured_turn
from libs.agent_cli.usage_checkpoint import INCIDENT_PHASES, SETUP_PHASES, UsageCheckpoint
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
            tmp_path / "usage",
            "incident",
            "incident" if attempt < 2 else "reflection-task",
            "responder" if attempt < 2 else "reflect",
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
    assert sum(document["usage"]["input_tokens"] for document in documents if document["phase"] == "responder") == 200
    assert sum(document["usage"]["input_tokens"] for document in documents if document["phase"] == "reflect") == 40
    assert [document["complete"] for document in documents].count(False) == 2


def test_interrupted_claude_retains_incremental_assistant_usage(tmp_path: Path) -> None:
    from agentshim.testing import FakeExecutor
    from agentshim.testing.claude_stream import ClaudeFramePeer

    checkpoint = UsageCheckpoint(tmp_path / "usage", "incident", "incident", "reflect")

    def assistant(input_tokens: int) -> dict:
        usage = {
            "input_tokens": input_tokens,
            "cache_read_input_tokens": 20,
            "cache_creation_input_tokens": 10,
            "output_tokens": 5,
        }
        return {
            "type": "assistant",
            "message": {"id": f"m{input_tokens}", "content": [{"type": "text", "text": "working"}], "usage": usage},
        }

    # The CLI streams two model responses and then stalls until the turn's time runs out.
    peer = ClaudeFramePeer(
        [[{"type": "system", "subtype": "init", "session_id": "s-1"}, assistant(100), assistant(200)]]
    )

    with pytest.raises(StructuredTurnTimeout):
        run_structured_turn(
            "claude",
            "prompt",
            output_schema={"type": "object"},
            cwd=tmp_path,
            access="danger-full-access",
            executor=FakeExecutor([], peers=lambda _spawn: peer),
            timeout_seconds=1.0,
            on_usage=checkpoint.publish,
        )

    snapshot = json.loads((checkpoint.directory / f"{checkpoint.turn_id}.json").read_text())
    assert snapshot["complete"] is False
    assert snapshot["usage"]["input_tokens"] == 360
    assert snapshot["usage"]["cache_read_input_tokens"] == 40
    assert snapshot["usage"]["llm_calls"] == 2
    assert snapshot["usage"]["model_requests"] == 2
    assert "total_cost_usd" not in snapshot["usage"]


@pytest.mark.parametrize("unavailable", [True, False])
def test_codex_resume_without_cumulative_baseline_runs_with_partial_usage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, unavailable: bool
) -> None:
    home = tmp_path / "codex"
    monkeypatch.setenv("CODEX_HOME", str(home))
    if not unavailable:
        # A response count without cumulative tokens cannot define a delta.
        rollout = home / "sessions" / "rollout-fixture-resume-session.jsonl"
        rollout.parent.mkdir(parents=True)
        rollout.write_text(
            json.dumps({"payload": {"type": "token_count", "info": {"last_token_usage": {"input_tokens": 10}}}}) + "\n"
        )
    checkpoint = UsageCheckpoint.open(tmp_path / "usage", "incident", "incident", "responder")
    assert checkpoint is not None
    agent = ScriptedAgent(lambda _: scripted_turn("codex", session_id="resume-session", structured_output={}))
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
    assert len(agent.requests) == 1, "accounting must not stop the turn"
    [document] = [json.loads(path.read_text()) for path in (tmp_path / "usage").glob("*.json")]
    assert (document["complete"], document["usage"]) == (False, {})


def test_checkpointed_codex_resume_never_publishes_ambiguous_session_total(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from agentshim import TokenUsage

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


SCHEMA = {"type": "object", "properties": {}, "required": [], "additionalProperties": False}
REPOSITORY = Path(__file__).resolve().parents[4]


def _turn(provider: str, cwd: Path, **options: Any) -> Any:
    agent = ScriptedAgent(
        lambda _: scripted_turn(
            provider,
            structured_output={},
            session_id="synthetic-session",
            usage=normalized_usage(input_tokens=120, output_tokens=9, turns=1),
        )
    )
    return run_structured_turn(
        provider,  # type: ignore[arg-type]
        "prompt",
        output_schema=SCHEMA,
        cwd=cwd,
        access="danger-full-access",
        executor=agent.executor,
        **options,
    )


def _setup_environment(session: Path, phase: str) -> dict[str, str]:
    return {
        "SDO_SETUP_USAGE_PHASE": phase,
        "SDO_SESSION_DIR": str(session),
        "SDO_USAGE_TASK_ID": f"setup-task-{phase}",
    }


@pytest.mark.parametrize("provider", ["codex", "claude"])
@pytest.mark.parametrize("phase", sorted(SETUP_PHASES))
def test_setup_turn_is_identified_by_its_task(provider: str, phase: str, tmp_path: Path) -> None:
    session = tmp_path / "session"
    _turn(provider, tmp_path, environment=_setup_environment(session, phase))
    [path] = (session / "usage" / phase).glob("*.json")
    document = json.loads(path.read_text())
    # The Go reader's test reads this same file, so the two sides cannot disagree on the shape.
    golden = json.loads((REPOSITORY / f"sdo/operator/memory/testdata/setup-usage/{phase}.json").read_text())
    assert set(document) == set(golden)
    assert set(document["usage"]) >= {"input_tokens", "output_tokens"}
    for key in ("version", "incident_id", "task_id", "phase", "complete", "partial_reason"):
        assert document[key] == golden[key], key


@pytest.mark.parametrize("provider", ["codex", "claude"])
@pytest.mark.parametrize("phase", sorted(INCIDENT_PHASES))
def test_incident_turn_requires_its_incident(provider: str, phase: str, tmp_path: Path) -> None:
    checkpoint = UsageCheckpoint.open(tmp_path / "usage", "incident", "task", phase)
    assert checkpoint is not None
    _turn(provider, tmp_path, on_usage=checkpoint.publish)
    [path] = (tmp_path / "usage").glob("*.json")
    document = json.loads(path.read_text())
    assert (document["incident_id"], document["phase"], document["complete"]) == ("incident", phase, True)
    with pytest.raises(ValueError, match="incident id"):
        UsageCheckpoint(tmp_path, "", "task", phase)


@pytest.mark.parametrize("provider", ["codex", "claude"])
@pytest.mark.parametrize("phase", [*sorted(SETUP_PHASES), "unknown-phase"])
def test_unwritable_or_unknown_setup_accounting_logs_once_and_runs(
    provider: str, phase: str, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    session = tmp_path / "session"
    session.mkdir()
    (session / "usage").write_text("not a directory")
    turn = _turn(provider, tmp_path, environment=_setup_environment(session, phase))
    assert turn.session_id == "synthetic-session"
    warnings = [record.getMessage() for record in caplog.records if "usage checkpoint" in record.getMessage()]
    assert len(warnings) == 1
    assert str(session / "usage" / phase) in warnings[0]
    assert f"phase={phase}" in warnings[0]


@pytest.mark.parametrize("identity", [("", "task", "respond"), ("incident", "", "respond"), ("incident", "t", "new")])
def test_open_never_raises(identity: tuple[str, str, str], tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    assert UsageCheckpoint.open(tmp_path, *identity) is None
    assert f"phase={identity[2]}" in caplog.text


@pytest.mark.parametrize("provider", ["codex", "claude"])
def test_failing_usage_callback_does_not_fail_the_turn(provider: str, tmp_path: Path) -> None:
    def broken(_usage: object, _complete: bool) -> None:
        raise OSError("volume is gone")

    assert _turn(provider, tmp_path, on_usage=broken).session_id == "synthetic-session"


def test_unwritable_usage_log_does_not_fail_the_turn(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "file").write_text("not a directory")
    monkeypatch.setenv("SDO_TURN_USAGE_LOG", str(tmp_path / "file" / "log.jsonl"))
    for provider in ("codex", "claude"):
        assert _turn(provider, tmp_path).session_id == "synthetic-session"


def test_every_setup_phase_go_sets_has_a_rule() -> None:
    """A phase added on the Go side needs an identity rule, an exported path and a reader."""
    operator = REPOSITORY / "sdo/operator"
    written = set()
    for path in operator.rglob("*.go"):
        if not path.name.endswith("_test.go"):
            uses = re.findall(r'"SDO_SETUP_USAGE_PHASE"(?:\]\s*=\s*"(\w+)")?', path.read_text())
            assert "" not in uses, f"{path} sets a setup phase this test cannot read"
            written.update(uses)
    assert written == SETUP_PHASES
    allowlist = (operator / "evidencesink/allowlist.go").read_text()
    for function, phases in (("setup", SETUP_PHASES), ("closing", {"reflect", "source_repair"})):
        body = allowlist.split(f"func {function}UsagePhase(", 1)[1].split("\n}", 1)[0]
        assert set(re.findall(r'"(\w+)"', body)) == phases
    reader = (operator / "memory/usage.go").read_text()
    for phase in SETUP_PHASES | (INCIDENT_PHASES - {"responder"}):
        assert f'"{phase}"' in reader
