"""Test doubles for structured agent turns.

SDO runs agents through agentshim, so tests script turns with agentshim's
``FakeExecutor``: each run replays the provider's real stream format through
the real parser, and the test sees the exact argv, prompt and cwd.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

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


@dataclass
class ScriptedAgent:
    """A ``FakeExecutor`` driven by a function of each turn request."""

    respond: Callable[[CommandRequest], FakeRun]
    executor: FakeExecutor = field(init=False)
    schemas: list[dict[str, Any]] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        self.executor = FakeExecutor(self._record_then_respond)

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
