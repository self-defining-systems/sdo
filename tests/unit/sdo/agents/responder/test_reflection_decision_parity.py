"""Result-boundary rules must be available in the complete local memory check."""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

import pytest

from sdo.agents.responder.reflection import CodexSessionBackend
from tests.structured_turns import ScriptedAgent, reply

if TYPE_CHECKING:
    from pathlib import Path

    from agentshim import CommandRequest
    from agentshim.testing import FakeRun


def invalid_decision(kind: str) -> dict[str, object]:
    result: dict[str, object] = {
        "summary": "Reviewed",
        "learning_decision": "sdk_gap",
        "no_change_reason": "Capability missing",
        "sdk_gaps": [
            {
                "fault_class": "catalog",
                "primitive": "catalog read",
                "reason": "Capability missing",
                "scratch_script": "inspect catalog",
            }
        ],
    }
    if kind == "missing gaps":
        result["sdk_gaps"] = []
    elif kind == "no change with gaps":
        result["learning_decision"] = "no_change"
    elif kind == "empty gap":
        result["sdk_gaps"] = [
            {
                "fault_class": "",
                "primitive": "catalog read",
                "reason": "Capability missing",
                "scratch_script": "inspect catalog",
            }
        ]
    elif kind == "deprecated proposal":
        result["artifact_proposal"] = {}
    return result


@pytest.mark.parametrize("kind", ["missing gaps", "no change with gaps", "empty gap", "deprecated proposal"])
def test_memory_cli_rejects_worker_result_contract_failures(
    tmp_path: Path, prepare_reflection_memory, kind: str
) -> None:
    prepare_reflection_memory(tmp_path)
    decision_path = tmp_path.with_suffix(".decision.json")
    decision_path.write_text(json.dumps(invalid_decision(kind)))
    completed = subprocess.run(
        ["sdo-operator", "memory", "check", "--json", "--decision", str(decision_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == 1, completed.stdout + completed.stderr
    assert any(item["rule"] == "reflection.decision" for item in json.loads(completed.stdout)["diagnostics"])


@pytest.mark.parametrize("kind", ["missing gaps", "no change with gaps", "empty gap", "deprecated proposal"])
def test_worker_corrects_result_contract_before_python_conversion(
    tmp_path: Path,
    prepare_reflection_memory,
    kind: str,
) -> None:
    prepare_reflection_memory(tmp_path)
    calls = 0

    def respond(_: CommandRequest) -> FakeRun:
        nonlocal calls
        calls += 1
        payload = (
            invalid_decision(kind)
            if calls == 1
            else {
                "summary": "Reviewed",
                "learning_decision": "no_change",
                "no_change_reason": "No new verified repair",
            }
        )
        return reply("codex", payload, session_id="reflection-session", branch="reflection")

    agent = ScriptedAgent(respond)
    result = CodexSessionBackend(executor=agent.executor).fresh(
        worktree=tmp_path,
        prompt="reflect",
        idempotency_key="single-dispatch",
    )
    assert result.learning_decision == "no_change"
    assert calls == 2
    assert "reflection.decision" in agent.prompts[1]
    assert "reflection-session" in agent.requests[1].argv
