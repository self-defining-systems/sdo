"""Test doubles for structured agent turns.

SDO runs agents through agentshim, so tests script turns with agentshim's
``FakeExecutor``: each run replays the provider's real stream format through
the real parser, and the test sees the exact argv, prompt and cwd.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
import textwrap
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from agentshim import CommandRequest as _CommandRequest
from agentshim.execution.process import ProcessExited, StderrLine, StdoutLine
from agentshim.testing import FakeExecutor, FakeRun, scripted_turn

from libs.agent_cli.structured import CODEX_HOME_ROOT_ENV

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

    import pytest
    from agentshim import CommandRequest


def turn_schema(request: CommandRequest) -> dict[str, Any]:
    """The output schema a scripted turn was asked to satisfy."""
    argv = list(request.argv)
    if "--output-schema" in argv:
        return json.loads(Path(argv[argv.index("--output-schema") + 1]).read_text(encoding="utf-8"))
    return json.loads(argv[argv.index("--json-schema") + 1])


def reply(
    provider: str,
    output: Mapping[str, Any],
    *,
    session_id: str | None,
    commands: Sequence[str] = (),
    branch: str | None = None,
) -> FakeRun:
    """A successful turn that ran *commands* and returned *output* (wrapped when *branch* is given)."""
    tool = "execute" if provider == "codex" else "Bash"
    return scripted_turn(
        provider,
        session_id=session_id,
        structured_output=dict(output) if branch is None else wrapped(output, branch),
        tool_calls=[(tool, {"command": command}, "") for command in commands],
    )


def wrapped(output: Mapping[str, Any], branch: str) -> dict[str, Any]:
    """*output* as the session wrapper object the responder and reflection turns share."""
    return {"incident_result": None, "reflection": None, branch: dict(output)}


def failure(stdout: str = "", stderr: str = "", returncode: int = 1) -> FakeRun:
    """A turn whose CLI exited nonzero."""
    return FakeRun(stdout=[stdout] if stdout else [], stderr=[stderr] if stderr else [], returncode=returncode)


def _is_frame(line: str) -> bool:
    try:
        return isinstance(json.loads(line), dict)
    except ValueError:
        return False


def _is_result(line: str) -> bool:
    return _is_frame(line) and json.loads(line).get("type") == "result"


class _ClaudeStreamScript:
    """The far end of a long-lived ``claude`` process that answers each user message with a scripted run.

    A scripted run is the CLI's frames for one turn, as ``scripted_turn`` writes them. The peer
    hands the run's lines back after the user message, so a test scripts a Claude turn the same
    way whether the process lives for one turn or several.
    """

    def __init__(
        self, argv: Sequence[str], cwd: str | None, env: Mapping[str, str], respond: Callable[[CommandRequest], FakeRun]
    ) -> None:
        self._argv = list(argv)
        self._cwd = cwd
        self._env = env
        self._respond = respond

    def on_start(self) -> Sequence[Any]:
        return ()

    def on_stdin(self, data: str) -> Sequence[Any]:
        out: list[Any] = []
        for raw in data.splitlines():
            if raw.strip():
                out.extend(self._react(json.loads(raw)))
        return out

    def on_stdin_closed(self) -> Sequence[Any]:
        return [ProcessExited(0)]

    def _react(self, message: Mapping[str, Any]) -> list[Any]:
        kind = message.get("type")
        request = message.get("request") or {}
        if kind == "control_request" and request.get("subtype") == "initialize":
            body = {"commands": [], "agents": []}
            reply = {"subtype": "success", "request_id": message.get("request_id"), "response": body}
            return [StdoutLine(json.dumps({"type": "control_response", "response": reply}) + "\n")]
        if kind != "user":
            return []
        content = (message.get("message") or {}).get("content")
        prompt = (
            content
            if isinstance(content, str)
            else "".join(part.get("text", "") for part in content or [] if isinstance(part, dict))
        )
        run = self._respond(_CommandRequest(self._argv, prompt, self._cwd, self._env, None))
        out: list[Any] = [
            StdoutLine(line if line.endswith("\n") else line + "\n") for line in run.stdout if _is_frame(line)
        ]
        out.extend(StderrLine(line) for line in run.stderr)
        if run.returncode != 0:
            out.append(ProcessExited(run.returncode))
        return out


@dataclass
class ScriptedAgent:
    """A ``FakeExecutor`` driven by a function of each turn request.

    Codex turns are commands; Claude turns are user messages to a long-lived process. Either way
    ``respond`` sees a ``CommandRequest`` whose ``stdin`` is the prompt and whose ``argv`` is the
    process's.
    """

    respond: Callable[[CommandRequest], FakeRun]
    executor: FakeExecutor = field(init=False)
    schemas: list[dict[str, Any]] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        self.executor = FakeExecutor(self._record_then_respond, peers=self._peer)

    def _peer(self, spawn: Any) -> _ClaudeStreamScript:
        return _ClaudeStreamScript(spawn.argv, spawn.cwd, spawn.env, self._record_then_respond_claude(spawn))

    def _record_then_respond_claude(self, spawn: Any) -> Callable[[CommandRequest], FakeRun]:
        def respond(request: CommandRequest) -> FakeRun:
            self.schemas.append(turn_schema(request))
            self.executor.requests.append(request)
            return self.respond(request)

        return respond

    def _record_then_respond(self, request: CommandRequest) -> FakeRun:
        # A schema file exists only while its turn runs, so read it now.
        self.schemas.append(turn_schema(request))
        return self.respond(request)

    @property
    def requests(self) -> list[CommandRequest]:
        return self.executor.requests

    @property
    def argvs(self) -> list[list[str]]:
        return [list(request.argv) for request in self.executor.requests]

    @property
    def prompts(self) -> list[str]:
        return [request.stdin or "" for request in self.executor.requests]


@contextmanager
def fake_codex_login(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeCodexLogin]:
    """Give Codex workspace-write turns a login and a home root outside ``/tmp``.

    SDO copies the login into a per-turn ``CODEX_HOME``, which Codex and
    agentshim refuse under the system temp dir, where pytest's ``tmp_path``
    lives, so the roots go under the user's cache dir instead.
    """
    cache = Path.home() / ".cache"
    cache.mkdir(parents=True, exist_ok=True)
    base = Path(tempfile.mkdtemp(prefix="sdo-test-codex-", dir=cache))
    try:
        login_home = base / "login"
        login_home.mkdir()
        (login_home / "auth.json").write_text('{"tokens": "fake"}', encoding="utf-8")
        homes_root = base / "homes"
        monkeypatch.setenv("CODEX_HOME", str(login_home))
        monkeypatch.setenv(CODEX_HOME_ROOT_ENV, str(homes_root))
        yield FakeCodexLogin(auth=login_home / "auth.json", homes_root=homes_root)
    finally:
        shutil.rmtree(base, ignore_errors=True)


@dataclass(frozen=True)
class FakeCodexLogin:
    auth: Path
    homes_root: Path


def write_stream_claude(path: Path, body: str) -> None:
    """An executable that speaks Claude Code's ``stream-json`` protocol; *body* runs once per user message.

    *body* may call ``say(frame)`` and read ``RESUME`` (the id after ``--resume=``, or ``None``).
    """
    header = f"""#!{sys.executable}
import json, os, pathlib, sys
if '--help' in sys.argv:
    raise SystemExit(0)
RESUME = next((a.split('=', 1)[1] for a in sys.argv if a.startswith('--resume=')), None)
def say(frame):
    print(json.dumps(frame), flush=True)
def turn():
"""
    footer = """
for raw in sys.stdin:
    if not raw.strip():
        continue
    message = json.loads(raw)
    request = message.get('request') or {}
    if message.get('type') == 'control_request' and request.get('subtype') == 'initialize':
        say({'type': 'control_response', 'response': {'subtype': 'success',
             'request_id': message.get('request_id'), 'response': {'commands': [], 'agents': []}}})
    elif message.get('type') == 'user':
        turn()
"""
    path.write_text(header + textwrap.indent(textwrap.dedent(body), "    ") + footer, encoding="utf-8")
    path.chmod(0o755)
