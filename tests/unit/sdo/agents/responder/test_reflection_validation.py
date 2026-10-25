"""Reflection validates through one blocking tool call, without shell polling."""

from __future__ import annotations

import asyncio
import os
import sys
from typing import TYPE_CHECKING

import pytest
from agentshim.testing import installed_mcp_servers
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from sdo.agents.responder.reflection import ClaudeSessionBackend, CodexSessionBackend
from tests.structured_turns import ScriptedAgent, reply

if TYPE_CHECKING:
    from pathlib import Path

    from agentshim import CommandRequest
    from agentshim.testing import FakeRun

    from libs.agent_cli.structured import AgentProvider


def _validator(tmp_path: Path, body: str, monkeypatch: pytest.MonkeyPatch) -> Path:
    executable = tmp_path / "sdo-operator"
    executable.write_text(f"#!{sys.executable}\n{body}")
    executable.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    return tmp_path


async def _call(root: Path, *, timeout: float = 2) -> dict[str, object]:
    parameters = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "sdo.agents.responder.reflection_validation",
            "--root",
            str(root),
            "--timeout-seconds",
            str(timeout),
        ],
        env=dict(os.environ),
    )
    async with stdio_client(parameters) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("check_memory", {"decision_path": "decision.json"})
            assert not result.isError, result
            assert result.structuredContent is not None
            return result.structuredContent


@pytest.mark.asyncio
async def test_validation_tool_waits_for_final_diagnostics_in_one_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _validator(
        tmp_path,
        """import time, json, sys, os
assert sys.argv[1:] == ["memory", "check", "--json", "--decision", "decision.json"]
assert os.getcwd() == os.environ["EXPECTED_CHECKOUT"]
time.sleep(0.1)
print(json.dumps({"diagnostics": [{"rule": "program.replay", "message": "wrong verdict"}]}))
sys.exit(1)
""",
        monkeypatch,
    )
    monkeypatch.setenv("EXPECTED_CHECKOUT", str(tmp_path))
    result = await _call(tmp_path)
    assert result["exit_code"] == 1
    assert not result["timed_out"]
    assert "wrong verdict" in result["stdout"]


@pytest.mark.asyncio
async def test_validation_tool_timeout_returns_failure_and_stops_children(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = tmp_path / "child-finished"
    child_code = f"import time; from pathlib import Path; time.sleep(1.2); Path({str(marker)!r}).touch()"
    _validator(
        tmp_path,
        f"""import subprocess, sys, time
subprocess.Popen([sys.executable, "-c", {child_code!r}])
print("checking replay", flush=True)
time.sleep(20)
""",
        monkeypatch,
    )
    result = await _call(tmp_path, timeout=0.3)
    assert result["timed_out"]
    assert result["exit_code"] != 0
    assert "checking replay" in result["stdout"]
    await asyncio.sleep(1.5)
    assert not marker.exists()


@pytest.mark.parametrize(
    ("provider", "backend_type"), [("codex", CodexSessionBackend), ("claude", ClaudeSessionBackend)]
)
@pytest.mark.parametrize("resume", [False, True])
def test_reflection_backend_installs_blocking_validation_tool(
    tmp_path: Path,
    provider: AgentProvider,
    backend_type: type[CodexSessionBackend],
    resume: bool,
    prepare_reflection_memory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = prepare_reflection_memory(tmp_path)
    monkeypatch.setenv("SDO_ACTOR", "responder")
    monkeypatch.setenv("SDO_PHASE", "reflection")
    monkeypatch.setenv("PYTHONPATH", "/opt/sdo")
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-reach-validator")

    def respond(request: CommandRequest) -> FakeRun:
        servers = installed_mcp_servers(provider, request, tmp_path)
        server = servers["sdo_reflection"]
        assert server["command"] == sys.executable
        assert server["env"]["SDO_ACTOR"] == "responder"
        assert server["env"]["SDO_PHASE"] == "reflection"
        assert server["env"]["SDO_BASE_COMMIT"] == base
        assert server["env"]["PYTHONPATH"] == "/opt/sdo"
        assert "OPENAI_API_KEY" not in server["env"]
        assert server["args"] == ["-m", "sdo.agents.responder.reflection_validation", "--root", str(tmp_path)]
        if provider == "codex":
            assert server["tool_timeout_s"] > 300
        else:
            assert int(request.env["MCP_TOOL_TIMEOUT"]) > 300_000
        return reply(
            provider,
            {
                "summary": "covered",
                "learning_decision": "no_change",
                "no_change_reason": "covered",
                "proposed_artifact_ids": [],
            },
            session_id="session-1",
            branch="reflection",
        )

    agent = ScriptedAgent(respond)
    backend = backend_type(executor=agent.executor)
    kwargs = {"worktree": tmp_path, "prompt": "reflect", "idempotency_key": "reflection:test"}
    if resume:
        backend.resume(session_id="session-1", **kwargs)
    else:
        backend.fresh(**kwargs)


def test_worker_image_ships_reflection_validation_transport() -> None:
    from pathlib import Path

    from tests.unit.sdo.operator.test_image_python_pins import _PIN, _closure

    root = Path(__file__).resolve().parents[5]
    image = (root / "sdo/operator/Dockerfile.runtime").read_text()
    pinned = {name for name, _ in _PIN.findall(image)}
    assert "mcp" in pinned
    assert _closure({"mcp"}, "3.11") <= pinned


@pytest.mark.asyncio
async def test_validation_tool_accepts_typed_decision_without_a_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _validator(
        tmp_path,
        """import json, sys
from pathlib import Path
assert sys.argv[1:4] == ["memory", "check", "--json"]
result = json.loads(Path(sys.argv[5]).read_text())
assert result == {"summary": "covered", "learning_decision": "no_change",
                  "no_change_reason": "already covered", "decline_reason": "",
                  "proposed_artifact_ids": [], "sdk_gaps": []}
""",
        monkeypatch,
    )
    from sdo.agents.responder.reflection_validation import validation_server

    server = validation_server(tmp_path)
    result = await server.call_tool(
        "check_memory",
        {
            "learning_decision": "no_change",
            "summary": "covered",
            "no_change_reason": "already covered",
        },
    )
    assert result[1]["exit_code"] == 0
