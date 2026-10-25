"""The structured-turn contract SDO's agents run through.

Every SDO role picks a provider and an access level; these tests pin what
each combination actually asks the CLI for, and how failures surface.
"""

from __future__ import annotations

import contextlib
import json
import os
import shlex
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from agentshim import StdioMcpServer, TokenUsage
from agentshim.providers.codex import BYPASS_FLAG, RULES_FILENAME, CodexSandboxConfig, parse_rules, parse_sandbox
from agentshim.testing import FakeRun, scripted_turn

from libs.agent_cli.structured import (
    ACCESS_MODES,
    AGENT_PROVIDERS,
    DETECTOR_GATEWAY_COMMAND,
    MEMORY_CHECK_COMMAND,
    TURN_USAGE_LOG_ENV,
    StructuredTurnError,
    StructuredTurnTimeout,
    run_structured_turn,
    turn_usage,
)
from tests.structured_turns import FakeCodexLogin, ScriptedAgent, failure, fake_codex_login, reply

if TYPE_CHECKING:
    from collections.abc import Iterator

    from agentshim import CommandRequest

_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
    "additionalProperties": False,
}


@pytest.fixture(autouse=True)
def codex_login(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeCodexLogin]:
    with fake_codex_login(monkeypatch) as login:
        yield login


def _run(
    provider: str,
    tmp_path: Path,
    run: FakeRun | None = None,
    **options: Any,
) -> tuple[Any, ScriptedAgent]:
    agent = ScriptedAgent(lambda _request: run or reply(provider, {"answer": "ok"}, session_id="s-1"))
    turn = run_structured_turn(
        provider,  # type: ignore[arg-type]
        "prompt",
        output_schema=_SCHEMA,
        cwd=tmp_path,
        access=options.pop("access", "read-only"),
        executor=agent.executor,
        **options,
    )
    return turn, agent


def _settings(argv: list[str]) -> dict[str, Any] | None:
    if "--settings" not in argv:
        return None
    return json.loads(argv[argv.index("--settings") + 1])


@pytest.mark.parametrize("provider", AGENT_PROVIDERS)
@pytest.mark.parametrize("access", ACCESS_MODES)
def test_every_combination_returns_the_structured_output_and_session(
    provider: str, access: str, tmp_path: Path
) -> None:
    turn, agent = _run(provider, tmp_path, access=access)

    assert json.loads(turn.output_json) == {"answer": "ok"}
    assert turn.session_id == "s-1"
    (request,) = agent.requests
    assert request.cwd == str(tmp_path.resolve())
    assert request.stdin == "prompt"
    assert agent.schemas == [_SCHEMA]


class TestCodexAccess:
    @pytest.mark.parametrize("access", ACCESS_MODES)
    def test_each_level_is_codex_own_sandbox_mode(self, access: str, tmp_path: Path) -> None:
        _, agent = _run("codex", tmp_path, access=access)
        (argv,) = agent.argvs
        assert BYPASS_FLAG not in argv
        assert parse_sandbox(argv) == CodexSandboxConfig(mode=access)  # type: ignore[arg-type]


class TestCodexGatewayHome:
    """Codex reads the detector-gateway exemption only from ``$CODEX_HOME/rules``."""

    @staticmethod
    def _snapshot(request: CommandRequest) -> dict[str, Any]:
        """What the per-turn home holds while the CLI runs; it is gone afterwards."""
        home = Path(request.env["CODEX_HOME"])
        rules = home / "rules" / RULES_FILENAME
        auth = home / "auth.json"
        return {
            "home": home,
            "rules": parse_rules(rules.read_text(encoding="utf-8")) if rules.is_file() else None,
            "auth": auth.read_text(encoding="utf-8") if auth.is_file() else None,
            "auth_mode": auth.stat().st_mode & 0o777 if auth.is_file() else None,
            "entries": sorted(path.name for path in home.iterdir()),
        }

    def _run_capturing(self, tmp_path: Path, run: Any, **options: Any) -> tuple[list[dict[str, Any]], Any]:
        seen: list[dict[str, Any]] = []

        def respond(request: CommandRequest) -> Any:
            seen.append(self._snapshot(request))
            return run

        agent = ScriptedAgent(respond)
        try:
            turn = run_structured_turn(
                "codex",
                "prompt",
                output_schema=_SCHEMA,
                cwd=tmp_path,
                access="workspace-write",
                executor=agent.executor,
                **options,
            )
        except StructuredTurnError as exc:
            return seen, exc
        return seen, turn

    def test_workspace_write_exempts_the_gateway_through_a_private_home(
        self, tmp_path: Path, codex_login: FakeCodexLogin
    ) -> None:
        (seen,), turn = self._run_capturing(tmp_path, reply("codex", {"answer": "ok"}, session_id="s-1"))

        assert json.loads(turn.output_json) == {"answer": "ok"}
        assert seen["rules"] == [DETECTOR_GATEWAY_COMMAND.split(), MEMORY_CHECK_COMMAND.split()]
        assert seen["auth"] == codex_login.auth.read_text(encoding="utf-8")
        assert seen["auth_mode"] == 0o600
        assert seen["entries"] == ["auth.json", "rules"]
        home = seen["home"]
        assert home.is_absolute()
        assert home.parent == codex_login.homes_root
        assert home != codex_login.auth.parent

    @pytest.mark.parametrize("succeeds", [True, False])
    def test_a_login_refreshed_during_the_turn_is_kept(
        self, tmp_path: Path, codex_login: FakeCodexLogin, *, succeeds: bool
    ) -> None:
        """Codex rotates refresh tokens, so dropping the copy would log the user out."""

        def refresh_then_respond(request: CommandRequest) -> Any:
            (Path(request.env["CODEX_HOME"]) / "auth.json").write_text('{"tokens": "rotated"}', encoding="utf-8")
            return reply("codex", {"answer": "ok"}, session_id="s-1") if succeeds else failure(stderr="boom")

        agent = ScriptedAgent(refresh_then_respond)
        with contextlib.suppress(StructuredTurnError):
            run_structured_turn(
                "codex",
                "prompt",
                output_schema=_SCHEMA,
                cwd=tmp_path,
                access="workspace-write",
                executor=agent.executor,
            )

        assert codex_login.auth.read_text(encoding="utf-8") == '{"tokens": "rotated"}'
        assert codex_login.auth.stat().st_mode & 0o777 == 0o600

    def test_an_unchanged_login_is_not_rewritten(self, tmp_path: Path, codex_login: FakeCodexLogin) -> None:
        before = codex_login.auth.stat().st_mtime_ns
        self._run_capturing(tmp_path, reply("codex", {"answer": "ok"}, session_id="s-1"))
        assert codex_login.auth.stat().st_mtime_ns == before

    def test_the_private_home_carries_the_sdo_skills_and_no_others(
        self, tmp_path: Path, codex_login: FakeCodexLogin
    ) -> None:
        for name in ("sdo-memory", "personal-skill"):
            skill = codex_login.auth.parent / "skills" / name
            skill.mkdir(parents=True)
            (skill / "SKILL.md").write_text(f"---\nname: {name}\ndescription: d\n---\n", encoding="utf-8")

        def snapshot(request: CommandRequest) -> list[str]:
            return sorted(path.name for path in (Path(request.env["CODEX_HOME"]) / "skills").iterdir())

        listed: list[list[str]] = []

        def respond(request: CommandRequest) -> Any:
            listed.append(snapshot(request))
            return reply("codex", {"answer": "ok"}, session_id="s-1")

        run_structured_turn(
            "codex",
            "prompt",
            output_schema=_SCHEMA,
            cwd=tmp_path,
            access="workspace-write",
            executor=ScriptedAgent(respond).executor,
        )

        assert listed == [["sdo-memory"]]

    def test_the_private_home_is_outside_the_workspace_and_tmp(
        self, tmp_path: Path, codex_login: FakeCodexLogin
    ) -> None:
        (seen,), _ = self._run_capturing(tmp_path, reply("codex", {"answer": "ok"}, session_id="s-1"))

        home = os.path.realpath(seen["home"])
        for writable in (str(tmp_path), "/tmp"):
            parent = os.path.realpath(writable)
            assert os.path.commonpath([home, parent]) != parent

    @pytest.mark.parametrize(
        "run",
        [
            pytest.param(reply("codex", {"answer": "ok"}, session_id="s-1"), id="success"),
            pytest.param(failure(stderr="boom\n"), id="cli-failure"),
            pytest.param(reply("codex", {"answer": "ok"}, session_id=None), id="no-session"),
        ],
    )
    def test_the_private_home_is_removed_after_the_turn(
        self, tmp_path: Path, codex_login: FakeCodexLogin, run: Any
    ) -> None:
        (seen,), _ = self._run_capturing(tmp_path, run)

        assert not seen["home"].exists()
        assert list(codex_login.homes_root.iterdir()) == []

    def test_a_missing_login_is_reported_without_an_api_key(
        self, tmp_path: Path, codex_login: FakeCodexLogin, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        codex_login.auth.unlink()
        monkeypatch.delenv("CODEX_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)

        seen, error = self._run_capturing(tmp_path, reply("codex", {"answer": "ok"}, session_id="s-1"))

        assert seen == []
        assert isinstance(error, StructuredTurnError)
        assert "auth.json" in str(error)
        assert "codex login" in str(error)

    @pytest.mark.parametrize("variable", ["CODEX_API_KEY", "OPENAI_API_KEY"])
    def test_an_api_key_replaces_the_login(
        self, tmp_path: Path, codex_login: FakeCodexLogin, monkeypatch: pytest.MonkeyPatch, variable: str
    ) -> None:
        codex_login.auth.unlink()
        monkeypatch.delenv("CODEX_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        monkeypatch.setenv(variable, "sk-test")

        (seen,), turn = self._run_capturing(tmp_path, reply("codex", {"answer": "ok"}, session_id="s-1"))

        assert json.loads(turn.output_json) == {"answer": "ok"}
        assert seen["entries"] == ["rules"]
        assert seen["rules"] == [DETECTOR_GATEWAY_COMMAND.split(), MEMORY_CHECK_COMMAND.split()]

    @pytest.mark.parametrize("access", ["read-only", "danger-full-access"])
    def test_other_levels_keep_the_launching_codex_home(
        self, access: str, tmp_path: Path, codex_login: FakeCodexLogin
    ) -> None:
        _, agent = _run("codex", tmp_path, access=access)

        (request,) = agent.requests
        assert request.env["CODEX_HOME"] == str(codex_login.auth.parent)
        assert not codex_login.homes_root.exists()

    def test_a_workspace_write_turn_cannot_be_resumed(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="cannot be resumed"):
            _run("codex", tmp_path, access="workspace-write", resume_session_id="earlier")


class TestClaudeAccess:
    def test_full_access_passes_no_settings(self, tmp_path: Path) -> None:
        _, agent = _run("claude", tmp_path, access="danger-full-access")
        (argv,) = agent.argvs
        assert _settings(argv) is None
        assert "--disallowedTools" not in argv

    def test_read_only_denies_writes_and_the_edit_tools(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-home"))
        _, agent = _run("claude", tmp_path, access="read-only")
        (request,) = agent.requests
        argv = list(request.argv)
        settings = _settings(argv)
        assert settings is not None
        root = str(tmp_path.resolve())
        assert settings["sandbox"]["filesystem"]["denyWrite"] == [root]
        assert argv[argv.index("--disallowedTools") + 1] == "Edit,Write,NotebookEdit"
        assert shlex.split(settings["hooks"]["PreToolUse"][0]["hooks"][0]["command"])[-2:] == [
            root,
            str(tmp_path / "claude-home" / "skills"),
        ]
        assert request.env["CLAUDE_BASH_MAINTAIN_PROJECT_WORKING_DIR"] == "1"

    @pytest.mark.parametrize("access", ["read-only", "workspace-write"])
    def test_confined_reads_still_reach_the_provisioned_skills(
        self, access: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-home"))
        (tmp_path / "work").mkdir()
        _, agent = _run("claude", tmp_path / "work", access=access)
        (request,) = agent.requests
        settings = _settings(list(request.argv))
        assert settings is not None
        confined = [
            shlex.split(entry["hooks"][0]["command"])[-2:]
            for entry in settings["hooks"]["PreToolUse"]
            if entry.get("matcher") != "Bash"
        ]
        assert confined == [[str((tmp_path / "work").resolve()), str(tmp_path / "claude-home" / "skills")]]

    def test_workspace_write_routes_compilation_through_the_detector_gateway(self, tmp_path: Path) -> None:
        _, agent = _run("claude", tmp_path, access="workspace-write")
        (request,) = agent.requests
        argv = list(request.argv)
        settings = _settings(argv)
        assert settings is not None
        assert "filesystem" not in settings["sandbox"]
        assert settings["sandbox"]["excludedCommands"] == [DETECTOR_GATEWAY_COMMAND, MEMORY_CHECK_COMMAND]
        assert "--disallowedTools" not in argv
        matchers = [entry.get("matcher") for entry in settings["hooks"]["PreToolUse"]]
        assert "Bash" in matchers
        assert request.env["CLAUDE_BASH_MAINTAIN_PROJECT_WORKING_DIR"] == "1"

    @pytest.mark.parametrize(
        ("command", "denied"),
        [
            ("go test ./...", True),
            ("cd .sdo/diagnostics && go build ./...", True),
            ("GOFLAGS=-mod=mod go vet ./...", True),
            (DETECTOR_GATEWAY_COMMAND, False),
            (MEMORY_CHECK_COMMAND, False),
            ("gofmt -l .", False),
            ("cat go.mod", False),
        ],
    )
    def test_the_rendered_bash_hook_denies_direct_go_only(self, tmp_path: Path, command: str, *, denied: bool) -> None:
        """Run the hook exactly as Claude Code would, through a shell."""
        _, agent = _run("claude", tmp_path, access="workspace-write")
        settings = _settings(agent.argvs[0])
        assert settings is not None
        (bash_hook,) = [entry for entry in settings["hooks"]["PreToolUse"] if entry.get("matcher") == "Bash"]
        completed = subprocess.run(
            ["/bin/sh", "-c", bash_hook["hooks"][0]["command"]],
            input=json.dumps({"tool_name": "Bash", "tool_input": {"command": command}}),
            capture_output=True,
            text=True,
            check=True,
        )
        decision = (
            json.loads(completed.stdout)["hookSpecificOutput"]["permissionDecision"] if completed.stdout else None
        )
        assert (decision == "deny") is denied


@pytest.mark.parametrize("provider", AGENT_PROVIDERS)
def test_a_resumed_turn_continues_the_named_session(provider: str, tmp_path: Path) -> None:
    turn, agent = _run(
        provider, tmp_path, resume_session_id="earlier", run=reply(provider, {"answer": "ok"}, session_id=None)
    )
    (argv,) = agent.argvs
    if provider == "codex":
        assert argv[1:5] == ["exec", "resume", "earlier", "-"]
    else:
        assert argv[argv.index("--resume") + 1] == "earlier"
    assert turn.session_id == "earlier"


@pytest.mark.parametrize("provider", AGENT_PROVIDERS)
def test_every_shell_command_is_reported_in_order(provider: str, tmp_path: Path) -> None:
    commands = ["ls", "cat /etc/hostname", "git status"]
    turn, _ = _run(provider, tmp_path, run=reply(provider, {"answer": "ok"}, session_id="s", commands=commands))
    assert list(dict.fromkeys(turn.shell_commands)) == commands


@pytest.mark.parametrize("provider", AGENT_PROVIDERS)
def test_turn_reports_its_own_installed_skill_directory(provider: str, tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / provider
    variable = "CODEX_HOME" if provider == "codex" else "CLAUDE_CONFIG_DIR"
    monkeypatch.setenv(variable, str(home))
    turn, _ = _run(provider, tmp_path, access="read-only")
    assert turn.skill_directory == home / "skills"


@pytest.mark.parametrize("provider", AGENT_PROVIDERS)
def test_a_failed_cli_reports_both_streams(provider: str, tmp_path: Path) -> None:
    with pytest.raises(StructuredTurnError) as raised:
        _run(provider, tmp_path, run=failure(stdout="event log detail\n", stderr="the real cause\n"))
    message = str(raised.value)
    assert "the real cause" in message
    assert "event log detail" in message
    assert message.index("the real cause") < message.index("event log detail")


@pytest.mark.parametrize("provider", AGENT_PROVIDERS)
def test_a_timeout_is_its_own_error(provider: str, tmp_path: Path) -> None:
    with pytest.raises(StructuredTurnTimeout) as raised:
        _run(provider, tmp_path, run=FakeRun(timeout=True), timeout_seconds=12)
    assert raised.value.timeout_seconds == 12


@pytest.mark.parametrize("provider", AGENT_PROVIDERS)
def test_a_turn_without_structured_output_is_rejected(provider: str, tmp_path: Path) -> None:
    with pytest.raises(StructuredTurnError, match="no structured output"):
        _run(provider, tmp_path, run=scripted_turn(provider, text="no json here", session_id="s"))


@pytest.mark.parametrize("provider", AGENT_PROVIDERS)
def test_a_fresh_turn_without_a_session_id_is_rejected(provider: str, tmp_path: Path) -> None:
    with pytest.raises(StructuredTurnError, match="session id"):
        _run(provider, tmp_path, run=reply(provider, {"answer": "ok"}, session_id=None))


def test_the_launching_environment_reaches_the_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Responder images put credentials in the process env, not a login shell."""
    monkeypatch.setenv("SDO_TEST_CREDENTIAL", "present")
    _, agent = _run("codex", tmp_path)
    assert agent.requests[0].env["SDO_TEST_CREDENTIAL"] == "present"


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"provider": "gemini"}, "unsupported agent provider"),
        ({"access": "admin"}, "unsupported access mode"),
        ({"timeout_seconds": 0}, "timeout_seconds"),
        ({"reasoning_effort": " "}, "reasoning_effort"),
        ({"resume_session_id": ""}, "resume_session_id"),
    ],
)
def test_invalid_arguments_fail_before_any_process_starts(
    tmp_path: Path, options: dict[str, Any], message: str
) -> None:
    agent = ScriptedAgent(lambda _request: FakeRun())
    arguments: dict[str, Any] = {"provider": "codex", "access": "read-only", **options}
    with pytest.raises(ValueError, match=message):
        run_structured_turn(
            arguments.pop("provider"),
            "prompt",
            output_schema=_SCHEMA,
            cwd=tmp_path,
            executor=agent.executor,
            **arguments,
        )
    assert agent.requests == []


@pytest.mark.parametrize("provider", AGENT_PROVIDERS)
def test_each_turn_appends_its_usage_to_the_configured_log(
    provider: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = tmp_path / "usage" / "turns.jsonl"
    monkeypatch.setenv(TURN_USAGE_LOG_ENV, str(log))

    _run(provider, tmp_path, model="m-1")
    _run(provider, tmp_path, model="m-1")

    records = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 2
    assert records[0]["provider"] == provider
    assert records[0]["model"] == "m-1"
    assert records[0]["session_id"] == "s-1"
    assert set(records[0]["usage"]) >= {"llm_calls", "input_tokens", "output_tokens", "cached_input_tokens"}
    assert records[0]["duration_seconds"] >= 0


def test_turns_write_no_usage_log_unless_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(TURN_USAGE_LOG_ENV, raising=False)
    _run("codex", tmp_path)
    assert not list(tmp_path.rglob("*.jsonl"))


def _token_count(info: dict[str, Any] | None) -> str:
    return json.dumps({"type": "event_msg", "payload": {"type": "token_count", "info": info}}) + "\n"


def _append_rollout(home: Path, session_id: str, requests: int) -> Path:
    rollout = home / "sessions" / "2026" / "09" / "27" / f"rollout-2026-09-27T00-00-00-{session_id}.jsonl"
    rollout.parent.mkdir(parents=True, exist_ok=True)
    with rollout.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"type": "session_meta", "payload": {"id": session_id}}) + "\n")
        # Rate-limit-only updates carry no usage and are not model requests.
        handle.write(_token_count(None))
        for _ in range(requests):
            handle.write(_token_count({"last_token_usage": {"input_tokens": 10}}))
    return rollout


def test_codex_usage_log_counts_model_requests_added_by_this_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, codex_login: FakeCodexLogin
) -> None:
    log = tmp_path / "turns.jsonl"
    monkeypatch.setenv(TURN_USAGE_LOG_ENV, str(log))
    home = codex_login.auth.parent
    _append_rollout(home, "s-1", requests=3)

    def respond(_request: CommandRequest) -> FakeRun:
        _append_rollout(home, "s-1", requests=2)
        return reply("codex", {"answer": "ok"}, session_id="s-1", commands=["ls", "pwd"])

    agent = ScriptedAgent(respond)
    turn = run_structured_turn(
        "codex",
        "prompt",
        output_schema=_SCHEMA,
        cwd=tmp_path,
        access="danger-full-access",
        resume_session_id="s-1",
        executor=agent.executor,
    )

    assert turn.model_requests == 2
    assert turn_usage(turn)["model_requests"] == 2
    (record,) = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert record["resumed"] is True
    assert record["model_requests"] == 2
    assert record["model_requests_source"] == "codex-rollout-token-count"
    assert record["tool_calls"] == 2
    assert record["usage"]["model_requests"] == 2


def _usage(input_tokens: int, cached: int, output: int, reasoning: int) -> dict[str, int]:
    return {
        "input_tokens": input_tokens,
        "cached_input_tokens": cached,
        "output_tokens": output,
        "reasoning_output_tokens": reasoning,
        "total_tokens": input_tokens + output,
    }


def _append_cumulative_rollout(home: Path, session_id: str, per_request: list[dict[str, int]]) -> None:
    """Append model responses whose ``total_token_usage`` accumulates over the whole session."""
    rollout = home / "sessions" / "2026" / "09" / "27" / f"rollout-2026-09-27T00-00-00-{session_id}.jsonl"
    rollout.parent.mkdir(parents=True, exist_ok=True)
    total = _usage(0, 0, 0, 0)
    for line in rollout.read_text(encoding="utf-8").splitlines() if rollout.is_file() else []:
        info = json.loads(line).get("payload", {}).get("info")
        if info:
            total = info["total_token_usage"]
    with rollout.open("a", encoding="utf-8") as handle:
        handle.write(_token_count(None))
        for last in per_request:
            total = {key: total[key] + last[key] for key in total}
            handle.write(_token_count({"total_token_usage": total, "last_token_usage": last}))


def test_a_resumed_codex_turn_accounts_only_its_own_tokens(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, codex_login: FakeCodexLogin
) -> None:
    """``codex exec resume`` reports the session's cumulative usage; the turn must not inherit earlier turns."""
    log = tmp_path / "turns.jsonl"
    monkeypatch.setenv(TURN_USAGE_LOG_ENV, str(log))
    home = codex_login.auth.parent
    _append_cumulative_rollout(home, "s-1", [_usage(100, 0, 10, 2), _usage(150, 100, 20, 3)])

    def respond(_request: CommandRequest) -> FakeRun:
        _append_cumulative_rollout(home, "s-1", [_usage(300, 200, 40, 7)])
        # Codex's own turn-end usage for a resumed session is the thread total.
        return scripted_turn(
            "codex",
            session_id="s-1",
            structured_output={"answer": "ok"},
            usage=TokenUsage(input_tokens=550, cached_input_tokens=300, output_tokens=70),
        )

    turn = run_structured_turn(
        "codex",
        "prompt",
        output_schema=_SCHEMA,
        cwd=tmp_path,
        access="danger-full-access",
        resume_session_id="s-1",
        executor=ScriptedAgent(respond).executor,
    )

    usage = turn_usage(turn)
    assert (usage["input_tokens"], usage["cached_input_tokens"], usage["output_tokens"]) == (300, 200, 40)
    assert usage["reasoning_output_tokens"] == 7
    assert usage["llm_calls"] == 1
    assert usage["model_requests"] == 1
    (record,) = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert record["usage"]["input_tokens"] == 300


def test_a_fresh_codex_turn_takes_reasoning_tokens_from_its_rollout(
    tmp_path: Path, codex_login: FakeCodexLogin
) -> None:
    home = codex_login.auth.parent

    def respond(_request: CommandRequest) -> FakeRun:
        _append_cumulative_rollout(home, "s-1", [_usage(100, 0, 10, 2), _usage(150, 100, 20, 3)])
        return scripted_turn(
            "codex",
            session_id="s-1",
            structured_output={"answer": "ok"},
            usage=TokenUsage(input_tokens=250, cached_input_tokens=100, output_tokens=30),
        )

    turn = run_structured_turn(
        "codex",
        "prompt",
        output_schema=_SCHEMA,
        cwd=tmp_path,
        access="danger-full-access",
        executor=ScriptedAgent(respond).executor,
    )

    usage = turn_usage(turn)
    assert (usage["input_tokens"], usage["cached_input_tokens"], usage["output_tokens"]) == (250, 100, 30)
    assert usage["reasoning_output_tokens"] == 5


_BREAKDOWN_KEYS = {
    "input_tokens",
    "cache_read_input_tokens",
    "cache_write_input_tokens",
    "cache_write_1h_input_tokens",
    "uncached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
}


def test_a_resumed_codex_turn_records_its_own_cache_breakdown(tmp_path: Path, codex_login: FakeCodexLogin) -> None:
    home = codex_login.auth.parent
    _append_cumulative_rollout(home, "s-1", [_usage(100, 0, 10, 2)])

    def respond(_request: CommandRequest) -> FakeRun:
        _append_cumulative_rollout(home, "s-1", [_usage(300, 200, 40, 7)])
        return scripted_turn(
            "codex",
            session_id="s-1",
            structured_output={"answer": "ok"},
            usage=TokenUsage(input_tokens=400, cache_read_input_tokens=200, output_tokens=50),
        )

    turn = run_structured_turn(
        "codex",
        "prompt",
        output_schema=_SCHEMA,
        cwd=tmp_path,
        access="danger-full-access",
        resume_session_id="s-1",
        executor=ScriptedAgent(respond).executor,
    )

    usage = turn_usage(turn)
    assert usage["cache_read_input_tokens"] == usage["cached_input_tokens"] == 200
    assert usage["cache_write_input_tokens"] == 0
    assert usage["uncached_input_tokens"] == 100


def test_a_claude_turn_records_cache_reads_and_writes_apart(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log = tmp_path / "turns.jsonl"
    monkeypatch.setenv(TURN_USAGE_LOG_ENV, str(log))
    tokens = TokenUsage(
        input_tokens=1_000,
        cache_read_input_tokens=600,
        cache_write_input_tokens=300,
        cache_write_1h_input_tokens=100,
        output_tokens=80,
        turns=3,
    )

    def respond(_request: CommandRequest) -> FakeRun:
        return scripted_turn("claude", session_id="s-1", structured_output={"answer": "ok"}, usage=tokens)

    turn = run_structured_turn(
        "claude",
        "prompt",
        output_schema=_SCHEMA,
        cwd=tmp_path,
        access="danger-full-access",
        executor=ScriptedAgent(respond).executor,
    )

    usage = turn_usage(turn)
    assert {key: usage[key] for key in _BREAKDOWN_KEYS} == {
        "input_tokens": 1_000,
        "cache_read_input_tokens": 600,
        "cache_write_input_tokens": 300,
        "cache_write_1h_input_tokens": 100,
        "uncached_input_tokens": 100,
        "output_tokens": 80,
        "reasoning_output_tokens": 0,
    }
    # The deprecated key is cache reads only; before agentshim 0.7 it also counted writes.
    assert usage["cached_input_tokens"] == 600
    (record,) = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert set(record["usage"]) >= _BREAKDOWN_KEYS


def test_codex_usage_log_records_an_unknown_request_count_without_a_rollout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = tmp_path / "turns.jsonl"
    monkeypatch.setenv(TURN_USAGE_LOG_ENV, str(log))

    turn, _ = _run("codex", tmp_path, access="danger-full-access")

    assert turn.model_requests is None
    assert "model_requests" not in turn_usage(turn)
    (record,) = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert record["resumed"] is False
    assert record["model_requests"] is None
    assert record["model_requests_source"] is None


def test_claude_model_requests_are_its_reported_agentic_turns(tmp_path: Path) -> None:
    turn, _ = _run("claude", tmp_path)

    assert turn.model_requests == turn.usage.tokens.turns
    assert turn_usage(turn)["model_requests"] == turn.usage.tokens.turns


def test_usage_log_records_the_shell_commands_each_turn_ran(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log = tmp_path / "turns.jsonl"
    monkeypatch.setenv(TURN_USAGE_LOG_ENV, str(log))
    long_command = "echo " + "x" * 10_000
    agent = ScriptedAgent(
        lambda _request: reply("codex", {"answer": "ok"}, session_id="s-1", commands=["kubectl get pods", long_command])
    )

    run_structured_turn(
        "codex",
        "prompt",
        output_schema=_SCHEMA,
        cwd=tmp_path,
        access="danger-full-access",
        executor=agent.executor,
    )

    (record,) = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert record["shell_commands"] == 2
    lines = record["shell_command_lines"]
    assert lines[0] == "kubectl get pods"
    assert lines[1].startswith("echo xxx")
    assert len(lines[1]) < 3_000


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


def test_a_claude_turn_keeps_its_mcp_config_out_of_commits_made_during_the_turn(tmp_path: Path) -> None:
    """Claude writes ``<cwd>/.mcp.json`` for the turn; a responder commit must not capture it."""
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    _git(checkout, "init", "-q")
    _git(checkout, "config", "user.email", "t@example.com")
    _git(checkout, "config", "user.name", "t")
    (checkout / "base.txt").write_text("base\n", encoding="utf-8")
    committed: list[str] = []

    def respond(_request: CommandRequest) -> FakeRun:
        assert (checkout / ".mcp.json").exists()
        (checkout / "fix.txt").write_text("fix\n", encoding="utf-8")
        _git(checkout, "add", "--all")
        _git(checkout, "commit", "-q", "-m", "fix")
        committed.extend(_git(checkout, "show", "--name-only", "--format=", "HEAD").split())
        return reply("claude", {"answer": "ok"}, session_id="s-1")

    run_structured_turn(
        "claude",
        "prompt",
        output_schema=_SCHEMA,
        cwd=checkout,
        access="danger-full-access",
        executor=ScriptedAgent(respond).executor,
        mcp_servers=[StdioMcpServer(name="probe", command="probe")],
    )

    assert sorted(committed) == ["base.txt", "fix.txt"]
    exclude = _git(checkout, "rev-parse", "--git-path", "info/exclude").strip()
    assert (checkout / exclude).read_text(encoding="utf-8").split().count("/.mcp.json") == 1


def test_a_claude_turn_outside_a_git_checkout_still_runs(tmp_path: Path) -> None:
    turn, _ = _run(
        "claude", tmp_path, access="danger-full-access", mcp_servers=[StdioMcpServer(name="probe", command="probe")]
    )

    assert json.loads(turn.output_json) == {"answer": "ok"}


@pytest.mark.parametrize("provider", AGENT_PROVIDERS)
def test_provider_session_is_checkpointed_while_the_turn_is_running(provider: str, tmp_path: Path) -> None:
    sessions: list[str] = []
    turn, _ = _run(provider, tmp_path, on_session_started=sessions.append)
    assert sessions == [turn.session_id]


@pytest.mark.parametrize("provider", AGENT_PROVIDERS)
def test_failed_session_checkpoint_aborts_the_turn(provider: str, tmp_path: Path) -> None:
    def refuse(_session: str) -> None:
        raise RuntimeError("checkpoint persistence unavailable")

    with pytest.raises(RuntimeError, match="checkpoint persistence unavailable"):
        _run(provider, tmp_path, on_session_started=refuse)


def test_turn_usage_log_prices_setup_tokens_when_provider_omits_dollars(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from agentshim import cost_usd, default_pricing, price_for

    log = tmp_path / "setup" / "judge" / "setup-usage.jsonl"
    monkeypatch.setenv(TURN_USAGE_LOG_ENV, str(log))
    usage = TokenUsage(input_tokens=1000, cached_input_tokens=200, output_tokens=100, turns=2)
    run = scripted_turn("codex", session_id="judge", usage=usage, structured_output={"answer": "ok"})
    turn, _ = _run("codex", tmp_path, run=run, model="gpt-6-luna")
    record = json.loads(log.read_text())
    entry = price_for("codex", "gpt-6-luna", default_pricing())
    assert entry is not None
    assert record["cost_usd"] == cost_usd(turn.usage.tokens, entry)
    assert record["cost_usd"] > 0
    assert record["cost_source"] == "agentshim-pricing"
    assert record["model_requests"] == turn.model_requests
    assert record["usage"]["input_tokens"] == turn.usage.tokens.input_tokens
    assert record["usage"]["output_tokens"] == turn.usage.tokens.output_tokens


def test_rejected_setup_output_still_records_spent_tokens(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log = tmp_path / "setup-usage.jsonl"
    monkeypatch.setenv(TURN_USAGE_LOG_ENV, str(log))
    run = scripted_turn(
        "codex",
        text="no structured response",
        session_id="judge",
        usage=TokenUsage(input_tokens=1000, output_tokens=100, turns=1),
    )
    with pytest.raises(StructuredTurnError, match="no structured output"):
        _run("codex", tmp_path, run=run, model="gpt-6-luna")
    record = json.loads(log.read_text())
    assert record["usage"]["input_tokens"] == 1000
    assert record["usage"]["output_tokens"] == 100
    assert record["cost_usd"] > 0
