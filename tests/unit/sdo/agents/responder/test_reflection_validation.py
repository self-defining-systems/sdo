"""Reflection validates through one blocking tool call, without shell polling."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from agentshim.testing import installed_mcp_servers
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from sdo.agents.responder.reflection import ClaudeSessionBackend, CodexSessionBackend
from tests.structured_turns import ScriptedAgent, reply

if TYPE_CHECKING:
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
    from sdo.agents.responder.reflection_validation import validation_server

    ready = tmp_path / "child-ready"
    child_code = (
        f"import os, time; from pathlib import Path; Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(60)"
    )
    _validator(
        tmp_path,
        f"""import subprocess, sys, time
print("checking replay", flush=True)
subprocess.Popen([sys.executable, "-c", {child_code!r}])
time.sleep(60)
""",
        monkeypatch,
    )
    create_process = asyncio.create_subprocess_exec

    async def start_ready_validator(*args: Any, **kwargs: Any) -> asyncio.subprocess.Process:
        process = await create_process(*args, **kwargs)
        # Start the short validation deadline only after the real descendant
        # exists. Interpreter startup under host load is not under test.
        try:
            async with asyncio.timeout(30):
                while not ready.exists():
                    await asyncio.sleep(0.01)
        except BaseException:
            os.killpg(process.pid, signal.SIGKILL)
            await process.communicate()
            raise
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", start_ready_validator)
    server = validation_server(tmp_path, timeout_seconds=0.1)
    _, result = await server.call_tool("check_memory", {"decision_path": "decision.json"})
    assert result["timed_out"]
    assert result["exit_code"] != 0
    assert "checking replay" in result["stdout"]
    # A killed orphan may remain a zombie until the host reaps it. Either
    # absence or zombie state proves it cannot continue validation work.
    status = Path(f"/proc/{ready.read_text()}/stat")
    async with asyncio.timeout(10):
        while True:
            try:
                if status.read_text().split(") ", 1)[1].split()[0] == "Z":
                    break
            except FileNotFoundError:
                break
            await asyncio.sleep(0.01)


@pytest.mark.parametrize(
    ("provider", "backend_type"), [("codex", CodexSessionBackend), ("claude", ClaudeSessionBackend)]
)
@pytest.mark.parametrize("resume", [False, True])
def test_reflection_backend_installs_blocking_validation_tool(
    tmp_path: Path,
    provider: AgentProvider,
    backend_type: type[CodexSessionBackend],
    resume: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SDO_ACTOR", "responder")
    monkeypatch.setenv("SDO_PHASE", "reflection")
    monkeypatch.setenv("SDO_BASE_COMMIT", "accepted-outcome")
    monkeypatch.setenv("PYTHONPATH", "/opt/sdo")
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-reach-validator")

    def respond(request: CommandRequest) -> FakeRun:
        servers = installed_mcp_servers(provider, request, tmp_path)
        server = servers["sdo_reflection"]
        assert server["command"] == sys.executable
        assert server["env"]["SDO_ACTOR"] == "responder"
        assert server["env"]["SDO_PHASE"] == "reflection"
        assert server["env"]["SDO_BASE_COMMIT"] == "accepted-outcome"
        assert server["env"]["PYTHONPATH"] == "/opt/sdo"
        assert "OPENAI_API_KEY" not in server["env"]
        assert server["args"] == ["-m", "sdo.agents.responder.reflection_validation", "--root", str(tmp_path)]
        if provider == "codex":
            assert server["tool_timeout_s"] > 300
        else:
            assert int(request.env["MCP_TOOL_TIMEOUT"]) > 300_000
        return reply(
            provider,
            {"summary": "covered", "learning_decision": "no_change", "no_change_reason": "covered", "playbook_ids": []},
            session_id="session-1",
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
