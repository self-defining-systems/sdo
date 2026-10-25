"""Usage is durable before a scripted turn returns, including interrupted turns."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from agentshim import normalized_usage
from agentshim.testing import FakeRun, scripted_turn

from libs.agent_cli.structured import run_structured_turn
from libs.agent_cli.usage_checkpoint import UsageCheckpoint
from tests.structured_turns import ScriptedAgent


@pytest.mark.parametrize("provider", ["codex", "claude"])
def test_usage_checkpoint_published_before_turn_returns(provider: str, tmp_path: Path) -> None:
    checkpoint = UsageCheckpoint(tmp_path / "usage", "incident", "task", "responder")
    observed: list[dict[str, object]] = []

    def worker(request: object) -> FakeRun:
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
