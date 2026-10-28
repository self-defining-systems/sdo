"""Reflection validates through one blocking tool call, without shell polling."""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import shutil
import sys
from typing import TYPE_CHECKING

import pytest
from agentshim.testing import installed_mcp_servers
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.server.fastmcp import FastMCP

from sdo.agents.responder.reflection import ClaudeSessionBackend, CodexSessionBackend
from sdo.agents.responder.reflection_validation import validation_server
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
            (block,) = result.content
            return json.loads(block.text)


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
    started = tmp_path / "validator-started"
    _validator(
        tmp_path,
        f"""import os, subprocess, sys, time
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
print("checking replay", flush=True)
with open({str(started)!r} + ".tmp", "w") as handle:
    handle.write(str(child.pid))
os.replace({str(started)!r} + ".tmp", {str(started)!r})
time.sleep(600)
""",
        monkeypatch,
    )

    async def expire_once_started(_seconds: float) -> None:
        # The deadline fires only after the validator has output and a child.
        while not started.exists():
            await asyncio.sleep(0.01)

    server = validation_server(tmp_path, timeout_seconds=0.3, expire=expire_once_started)
    (block,) = await server.call_tool("check_memory", {"decision_path": "decision.json"})
    result = json.loads(block.text)
    assert result["timed_out"]
    assert result["exit_code"] != 0
    assert "checking replay" in result["stdout"]
    assert "timed out after 0.3s" in result["stderr"]
    child = pathlib.Path("/proc") / started.read_text()
    async with asyncio.timeout(60):  # Bounds only a failure: a killed child is already exiting.
        while child.exists() and (child / "stat").read_text().split(") ")[-1][0] != "Z":
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
    (block,) = result
    assert json.loads(block.text)["exit_code"] == 0


@pytest.mark.asyncio
async def test_reflection_tools_return_one_copy_of_each_result(tmp_path: Path) -> None:
    # A resumed session carries every tool result; a second structured copy doubles it.
    from sdo.agents.responder.reflection_validation import validation_server

    server = validation_server(tmp_path)
    assert {tool.name: tool.outputSchema for tool in await server.list_tools()} == {
        tool.name: None for tool in await server.list_tools()
    }
    result = await server.call_tool("authoring_reference", {"topic": "sdk"})
    assert not isinstance(result, tuple)
    (block,) = result
    assert "program-sdk.md" in block.text


@pytest.mark.asyncio
async def test_every_reflection_tool_reply_stays_within_the_shared_budget(tmp_path: Path, monkeypatch) -> None:
    # Every reply is resent on each later reflection request, so oversized replies go to a file.
    from sdo.agents.responder.reflection_validation import validation_server
    from sdo.agents.responder.tool_output import MAX_TOOL_REPLY_BYTES

    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path / "scratch"))
    server = validation_server(tmp_path)
    full = "\n".join(f"line {index} " + "x" * 80 for index in range(400))

    def big_evidence() -> str:
        """Synthetic oversized reply."""
        return full

    from sdo.agents.context_output import budgets

    monkeypatch.setitem(budgets(), "tool_reply.big_evidence", MAX_TOOL_REPLY_BYTES)
    monkeypatch.setitem(budgets(), "tool_description.big_evidence", 256)
    server.add_tool(big_evidence)
    (block,) = await server.call_tool("big_evidence", {})
    assert len(block.text.encode()) <= MAX_TOOL_REPLY_BYTES
    (path,) = [
        line.removeprefix("Full: ").replace("$SDO_SESSION_SCRATCH", str(tmp_path / "scratch"))
        for line in block.text.splitlines()
        if line.startswith("Full: ")
    ]
    assert pathlib.Path(path).read_text() == full
    assert "line 399" not in block.text
    # The bound sits on the server's single dispatch path, so no registered tool can bypass it.
    assert type(server).call_tool is not FastMCP.call_tool


@pytest.mark.skipif(shutil.which("gofmt") is None, reason="needs the Go toolchain")
async def test_check_memory_formats_changed_memory_go_before_checking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Formatting is mechanical: the tool fixes changed sources instead of reporting go.format."""
    import subprocess

    checkout = tmp_path / "checkout"
    (checkout / ".sdo/playbooks/fixture").mkdir(parents=True)
    accepted = checkout / ".sdo/playbooks/fixture/accepted.go"
    accepted.write_text("package fixture\nfunc  Accepted() {}\n")
    subprocess.run(["git", "init", "-q"], cwd=checkout, check=True)
    subprocess.run(["git", "add", "."], cwd=checkout, check=True)
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "accepted"], cwd=checkout, check=True
    )
    changed = checkout / ".sdo/playbooks/fixture/diagnose.go"
    changed.write_text("package fixture\nfunc  Diagnose() {\n x := 1\n _ = x\n}\n")
    outside = checkout / "main.go"
    outside.write_text("package main\nfunc  main() {}\n")
    _validator(
        tmp_path,
        """import sys
from pathlib import Path
source = Path(".sdo/playbooks/fixture/diagnose.go").read_text()
sys.exit(0 if source == "package fixture\\n\\nfunc Diagnose() {\\n\\tx := 1\\n\\t_ = x\\n}\\n" else 3)
""",
        monkeypatch,
    )
    server = validation_server(checkout)
    result = await server.call_tool("check_memory", {"learning_decision": "updated", "summary": "s"})
    (block,) = result
    assert json.loads(block.text)["exit_code"] == 0
    assert accepted.read_text() == "package fixture\nfunc  Accepted() {}\n", "accepted bytes are never rewritten"
    assert outside.read_text() == "package main\nfunc  main() {}\n", "only operational memory is formatted"


@pytest.mark.parametrize(("exit_code", "expected"), [(0, "passed"), (1, "fix each diagnostic")])
async def test_check_memory_reply_leads_with_the_next_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exit_code: int, expected: str
) -> None:
    """Replay notes ride along with a pass; the reply says plainly they are not work."""
    _validator(
        tmp_path,
        f'import sys\nprint(\'{{"diagnostics": [], "mitigation_replay_notes": [1]}}\')\nsys.exit({exit_code})\n',
        monkeypatch,
    )
    server = validation_server(tmp_path)
    (block,) = await server.call_tool(
        "check_memory", {"learning_decision": "updated", "summary": "s", "proposed_artifact_ids": ["book"]}
    )
    reply = json.loads(block.text)
    assert next(iter(reply)) == "next", "the instruction leads so a capped reply keeps it"
    assert expected in reply["next"]
    if exit_code == 0:
        assert "information" in reply["next"]
        assert '"updated"' in reply["next"]
        assert '["book"]' in reply["next"]


async def test_check_memory_pass_names_programs_whose_replay_disagrees(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each new program should Match its fault capture and NoMatch healthy; a pass says which do not."""
    notes = [
        {"program_id": "good", "case_id": "a", "effects": ["open diagnosis: Match", "healthy diagnosis: NoMatch"]},
        {"program_id": "weak", "case_id": "b", "effects": ["open diagnosis: Unknown", "healthy diagnosis: Unknown"]},
    ]
    body = json.dumps({"diagnostics": [], "mitigation_replay_notes": notes})
    _validator(tmp_path, f"print({body!r})\n", monkeypatch)
    server = validation_server(tmp_path)
    (block,) = await server.call_tool(
        "check_memory", {"learning_decision": "updated", "summary": "s", "proposed_artifact_ids": ["good", "weak"]}
    )
    step = json.loads(block.text)["next"]
    assert "weak" in step
    assert "open Unknown" in step
    assert "healthy Unknown" in step
    assert "good" not in step.split("passed")[1].split("Each program")[0]
