"""Provider throttling is infrastructure wait, across turns and workers."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from libs.agent_cli.provider_capacity import OpusPool, ProviderCapacity, ProviderRateLimited, RateLimitPolicy
from libs.agent_cli.structured import StructuredTurnError, run_structured_turn
from tests.structured_turns import ScriptedAgent, failure, reply

SCHEMA = {"type": "object", "properties": {"answer": {"type": "string"}}}
# Claude Code 2.1.311's OAuth limit line and Anthropic API error envelopes.
LIMITS = [
    "You've hit your limit · resets 5pm (America/Los_Angeles)",
    'API Error: 429 {"type":"error","error":{"type":"rate_limit_error","message":"Rate limit reached"}}',
    'API Error: 529 {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}',
]


class Clock:
    seconds = 0.0

    def monotonic(self) -> float:
        return self.seconds

    def now(self) -> datetime:
        return datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=self.seconds)

    def sleep(self, seconds: float) -> None:
        self.seconds += seconds


def capacity(tmp_path: Path, clock: Clock) -> ProviderCapacity:
    return ProviderCapacity(
        environment={"SDO_PROVIDER_WAIT_LOG": str(tmp_path / "waits.jsonl")},
        monotonic=clock.monotonic,
        now=clock.now,
        sleep=clock.sleep,
    )


@pytest.mark.parametrize("text", LIMITS)
@pytest.mark.parametrize("stream", ["stderr", "stdout", "result"])
def test_real_claude_throttle_formats_retry_without_losing_the_session(tmp_path: Path, text: str, stream: str) -> None:
    clock = Clock()
    pool = capacity(tmp_path, clock)
    calls = 0

    def respond(_request):
        nonlocal calls
        calls += 1
        if calls == 1:
            if stream == "result":
                return failure(
                    stdout=json.dumps({"type": "result", "is_error": True, "result": text, "session_id": "s-1"}),
                    returncode=0,
                )
            return failure(**{stream: text})
        return reply("claude", {"answer": "ok"}, session_id="s-1")

    agent = ScriptedAgent(respond)
    turn = run_structured_turn(
        "claude",
        "prompt",
        output_schema=SCHEMA,
        cwd=tmp_path,
        access="read-only",
        model="sonnet",
        executor=agent.executor,
        provider_capacity=pool,
        timeout_seconds=10,
    )
    assert json.loads(turn.output_json) == {"answer": "ok"}
    assert clock.seconds == 30
    assert turn.rate_limit_wait_seconds == 30
    assert len(agent.requests) == 2


def test_backoff_doubles_to_ten_minutes_then_exhausts_at_thirty(tmp_path: Path) -> None:
    clock = Clock()
    pool = capacity(tmp_path, clock)
    agent = ScriptedAgent(lambda _: failure(stderr=LIMITS[0]))
    with pytest.raises(ProviderRateLimited, match="rate_limited"):
        run_structured_turn(
            "claude",
            "prompt",
            output_schema=SCHEMA,
            cwd=tmp_path,
            access="read-only",
            model="sonnet",
            executor=agent.executor,
            provider_capacity=pool,
        )
    events = [json.loads(line) for line in (tmp_path / "waits.jsonl").read_text().splitlines()]
    finished = [event for event in events if event["state"] == "finished"]
    assert [event["duration_seconds"] for event in finished] == [30, 60, 120, 240, 480, 600, 270]
    assert clock.seconds == 1800
    assert events[-1]["state"] == "exhausted"


def test_thirty_minute_retry_cap_includes_time_spent_in_retry_attempts(tmp_path: Path) -> None:
    clock = Clock()
    manager = capacity(tmp_path, clock)
    manager.backoff(0)
    clock.sleep(1750)  # A retry ran for this long before the next overload.
    with pytest.raises(ProviderRateLimited, match="budget exhausted"):
        manager.backoff(1)
    assert clock.seconds == 1800
    assert manager.rate_limit_wait_seconds == 50


def test_ordinary_failures_never_retry(tmp_path: Path) -> None:
    clock = Clock()
    agent = ScriptedAgent(lambda _: failure(stderr="Permission denied"))
    with pytest.raises(StructuredTurnError, match="Permission denied"):
        run_structured_turn(
            "claude",
            "prompt",
            output_schema=SCHEMA,
            cwd=tmp_path,
            access="read-only",
            executor=agent.executor,
            model="sonnet",
            provider_capacity=capacity(tmp_path, clock),
        )
    assert len(agent.requests) == 1
    assert clock.seconds == 0


def test_pool_default_capacity_and_shared_capacity_must_agree(tmp_path: Path) -> None:
    pool = OpusPool(tmp_path)
    held = [pool.try_acquire() for _ in range(8)]
    assert all(handle is not None for handle in held)
    assert OpusPool(tmp_path).try_acquire() is None
    with pytest.raises(ValueError, match="capacity"):
        OpusPool(tmp_path, capacity=9).try_acquire()
    for handle in held:
        assert handle is not None
        handle.close()
    handle = OpusPool(tmp_path).try_acquire()
    assert handle is not None
    handle.close()


def test_admission_wait_is_separate_and_bounded(tmp_path: Path) -> None:
    clock = Clock()
    root = tmp_path / "pool"
    held = OpusPool(root, capacity=1).try_acquire()
    assert held is not None
    manager = ProviderCapacity(
        environment={
            "SDO_OPUS_POOL_DIR": str(root),
            "SDO_OPUS_MAX_SESSIONS": "1",
            "SDO_PROVIDER_WAIT_LOG": str(tmp_path / "provider-waits.jsonl"),
        },
        policy=RateLimitPolicy(initial_delay_seconds=1, max_delay_seconds=1, max_wait_seconds=3),
        monotonic=clock.monotonic,
        now=clock.now,
        sleep=clock.sleep,
    )
    try:
        with pytest.raises(ProviderRateLimited, match="admission budget exhausted"):
            with manager.opus_session("opus"):
                pytest.fail("full pool admitted a session")
    finally:
        held.close()
    assert manager.concurrency_wait_seconds == 3
    assert manager.rate_limit_wait_seconds == 0
    events = [json.loads(line) for line in (tmp_path / "provider-waits.jsonl").read_text().splitlines()]
    assert events[-1]["kind"] == "concurrency"
    assert events[-1]["state"] == "exhausted"


def test_tool_output_mentioning_a_rate_limit_is_not_a_provider_error(tmp_path: Path) -> None:
    from agentshim.testing import FakeRun

    tool_frame = {"type": "user", "message": {"content": [{"type": "tool_result", "content": LIMITS[1]}]}}
    agent = ScriptedAgent(
        lambda _: FakeRun(stdout=[json.dumps(tool_frame)], stderr=["invalid local settings"], returncode=1)
    )
    with pytest.raises(StructuredTurnError, match="invalid local settings") as raised:
        run_structured_turn(
            "claude",
            "prompt",
            output_schema=SCHEMA,
            cwd=tmp_path,
            access="read-only",
            model="sonnet",
            executor=agent.executor,
            provider_capacity=capacity(tmp_path, Clock()),
        )
    assert not isinstance(raised.value, ProviderRateLimited)
    assert len(agent.requests) == 1


def test_opus_refuses_to_run_without_shared_pool(tmp_path: Path) -> None:
    agent = ScriptedAgent(lambda _: reply("claude", {"answer": "ok"}, session_id="s-1"))
    with pytest.raises(ProviderRateLimited, match="shared.*pool"):
        run_structured_turn(
            "claude",
            "prompt",
            output_schema=SCHEMA,
            cwd=tmp_path,
            access="read-only",
            model="claude-opus-5-5",
            executor=agent.executor,
            provider_capacity=capacity(tmp_path, Clock()),
        )
    assert not agent.requests


@pytest.mark.parametrize(
    "kwargs", [{"initial_delay_seconds": 0}, {"max_delay_seconds": -1}, {"max_wait_seconds": float("inf")}]
)
def test_retry_policy_rejects_invalid_configuration(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="positive and finite"):
        RateLimitPolicy(**kwargs)


def test_shared_pool_releases_a_token_when_its_process_dies(tmp_path: Path) -> None:
    code = """
import sys
import pathlib
from libs.agent_cli.provider_capacity import OpusPool
handle = OpusPool(pathlib.Path(sys.argv[1]), capacity=1).try_acquire()
assert handle is not None
print('acquired', flush=True)
sys.stdin.read()
"""
    with subprocess.Popen(
        [sys.executable, "-c", code, str(tmp_path)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    ) as child:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "acquired"
        assert OpusPool(tmp_path, capacity=1).try_acquire() is None
        child.kill()
        child.wait(timeout=10)
    token = OpusPool(tmp_path, capacity=1).try_acquire()
    assert token is not None
    token.close()


def test_backoff_releases_opus_token_and_retry_resumes_the_existing_session(tmp_path: Path) -> None:
    clock = Clock()
    pool_root = tmp_path / "pool"
    calls = 0
    sleeps = []

    def sleep(seconds: float) -> None:
        token = OpusPool(pool_root, capacity=1).try_acquire()
        assert token is not None
        token.close()
        sleeps.append(seconds)
        clock.sleep(seconds)

    manager = ProviderCapacity(
        environment={"SDO_OPUS_POOL_DIR": str(pool_root), "SDO_OPUS_MAX_SESSIONS": "1"},
        monotonic=clock.monotonic,
        sleep=sleep,
        now=clock.now,
    )

    def respond(request):
        nonlocal calls
        calls += 1
        assert OpusPool(pool_root, capacity=1).try_acquire() is None
        if calls == 1:
            return failure(
                stdout=json.dumps({"type": "system", "subtype": "init", "session_id": "s-1"}), stderr=LIMITS[0]
            )
        assert list(request.argv)[list(request.argv).index("--resume") + 1] == "s-1"
        return reply("claude", {"answer": "ok"}, session_id="s-1")

    agent = ScriptedAgent(respond)
    run_structured_turn(
        "claude",
        "prompt",
        output_schema=SCHEMA,
        cwd=tmp_path,
        access="read-only",
        model="opus",
        executor=agent.executor,
        provider_capacity=manager,
    )
    assert sleeps == [30]


def test_retry_keeps_prior_tool_audit_and_paid_usage(tmp_path: Path) -> None:
    from agentshim import TokenUsage
    from agentshim.testing import FakeRun, scripted_turn

    from libs.agent_cli.structured import turn_usage

    first = scripted_turn(
        "claude",
        session_id="s-1",
        usage=TokenUsage(input_tokens=7, output_tokens=3, turns=1),
        tool_calls=[("Bash", {"command": "cat source.py"}, "source")],
    )
    lines = list(first.stdout)
    final = json.loads(lines[-1])
    final.update(is_error=True, result=LIMITS[0])
    final.pop("structured_output", None)
    lines[-1] = json.dumps(final)
    attempts = [
        FakeRun(stdout=lines, returncode=1),
        scripted_turn(
            "claude",
            session_id="s-1",
            usage=TokenUsage(input_tokens=11, output_tokens=5, turns=1),
            structured_output={"answer": "ok"},
        ),
    ]
    agent = ScriptedAgent(lambda _: attempts.pop(0))
    turn = run_structured_turn(
        "claude",
        "prompt",
        output_schema=SCHEMA,
        cwd=tmp_path,
        access="read-only",
        model="sonnet",
        executor=agent.executor,
        provider_capacity=capacity(tmp_path, Clock()),
    )
    assert turn.shell_commands == ("cat source.py",)
    assert turn.tool_calls == 1
    assert turn.model_requests == 2
    assert turn_usage(turn)["input_tokens"] == 18
    assert turn.usage.tokens.output_tokens == 8


def test_fake_claude_executable_reports_the_real_cli_limit_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    script = tmp_path / "claude"
    marker = tmp_path / "called"
    script.write_text(f"""#!{sys.executable}
import sys, pathlib, json
if '--help' in sys.argv:
    raise SystemExit(0)
marker = pathlib.Path({str(marker)!r})
sys.stdin.read()
if not marker.exists():
    marker.touch()
    print({LIMITS[0]!r}, file=sys.stderr)
    raise SystemExit(1)
print(json.dumps({{"type":"system","subtype":"init","session_id":"fake-session"}}))
print(json.dumps({{"type":"result","subtype":"success","is_error":False,"session_id":"fake-session",
                  "result":"ok","structured_output":{{"answer":"ok"}},"num_turns":1}}))
""")
    script.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ["PATH"])
    clock = Clock()
    turn = run_structured_turn(
        "claude",
        "prompt",
        output_schema=SCHEMA,
        cwd=tmp_path,
        access="read-only",
        model="sonnet",
        provider_capacity=capacity(tmp_path, clock),
    )
    assert turn.session_id == "fake-session"
    assert turn.rate_limit_wait_seconds == 30
