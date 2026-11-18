"""The vanilla arm: SREGym's stock Claude Code, never losing a transcript."""

from __future__ import annotations

import ast
import json
import os
import stat
import sys
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.claudecode_agent import (
    ALLOWED_TOOLS,
    AUTH_KEYS,
    ClaudeCodeBaselineAgent,
    ClaudeCodeSettings,
    claude_auth_env,
    claudecode_command,
    run_claudecode_cell,
)
from benchmarks.sregym.fastloop.codex_agent import SubmissionStub
from benchmarks.sregym.fastloop.loop import InjectionWindow

REPO_ROOT = Path(__file__).resolve().parents[5]
STOCK = REPO_ROOT / "third_party" / "sregym" / "clients"
T0 = datetime(2026, 11, 18, 12, 0, 0, tzinfo=timezone.utc)

# A stand-in ``claude``: writes a session transcript the way the CLI does, then behaves as the env says.
FAKE_CLAUDE = textwrap.dedent(
    f"""\
    #!{sys.executable}
    import json, os, sys, time
    from pathlib import Path
    if sys.argv[1:] == ["--version"]:
        print("9.9.9 (fake)"); sys.exit(0)
    mode = os.environ.get("FAKE_MODE", "ok")
    home = Path(os.environ["CLAUDE_CONFIG_DIR"])
    sid = "11111111-2222-3333-4444-555555555555"
    project = home / "projects" / "-neutral"
    project.mkdir(parents=True, exist_ok=True)
    def msg(i, out):
        return {{"type": "assistant", "uuid": f"u{{i}}", "timestamp": "2026-11-18T12:00:0%d.000Z" % i,
                 "message": {{"id": f"msg_{{i}}", "model": "claude-sonnet-5-5", "usage": {{
                     "input_tokens": 10, "cache_read_input_tokens": 100, "cache_creation_input_tokens": 50,
                     "output_tokens": out}}}}}}
    lines = [msg(1, 5), msg(1, 7), msg(2, 9)]  # message 1 is streamed twice: one request
    (project / f"{{sid}}.jsonl").write_text("".join(json.dumps(l) + chr(10) for l in lines))
    Path(os.environ["FAKE_PROBE"]).write_text(json.dumps({{
        "cwd": os.getcwd(), "config": os.environ["CLAUDE_CONFIG_DIR"], "kubeconfig": os.environ["KUBECONFIG"],
        "model": os.environ.get("ANTHROPIC_MODEL"), "argv": sys.argv[1:],
        "kubeconfig_text": Path(os.environ["KUBECONFIG"]).read_text()}}))
    print(json.dumps({{"type": "system", "subtype": "init", "session_id": sid}}), flush=True)
    if mode == "sidechain":
        print(json.dumps({{"type": "user", "session_id": sid,
              "tool_use_result": {{"agentId": "a0123456789abcdef"}}}}), flush=True)
    if mode == "hang":
        import subprocess
        subprocess.Popen(["sleep", "300"])
        sys.stdout.write('{{"type": "assistant", "partial'); sys.stdout.flush()
        time.sleep(300)
    if mode == "crash":
        sys.exit(3)
    print(json.dumps({{"type": "result", "session_id": sid, "total_cost_usd": 0.0001}}), flush=True)
    """
)


@pytest.fixture
def fake_claude(tmp_path: Path) -> Path:
    path = tmp_path / "bin" / "claude"
    path.parent.mkdir()
    path.write_text(FAKE_CLAUDE)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


@pytest.fixture
def profile(tmp_path: Path) -> Path:
    home = tmp_path / "profile"
    home.mkdir()
    env = {"CLAUDE_CODE_USE_VERTEX": "1", "ANTHROPIC_VERTEX_PROJECT_ID": "p", "CLOUD_ML_REGION": "r", "OTHER": "x"}
    (home / "settings.json").write_text(json.dumps({"env": env}))
    return home


def _settings(tmp_path: Path, fake: Path, profile: Path, mode: str = "ok", **overrides: object) -> ClaudeCodeSettings:
    kubeconfig = tmp_path / "agent.kubeconfig"
    kubeconfig.write_text("kubeconfig-body")
    environ = {
        "PATH": os.environ["PATH"],
        "CLAUDE_CONFIG_DIR": str(profile),
        "FAKE_MODE": mode,
        "FAKE_PROBE": str(tmp_path / "probe.json"),
    }
    return ClaudeCodeSettings(
        kubeconfig=kubeconfig,
        results_dir=tmp_path / "results",
        claude_binary=str(fake),
        environ=environ,
        timeout_seconds=float(overrides.pop("timeout_seconds", 30)),  # type: ignore[arg-type]
        **overrides,  # type: ignore[arg-type]
    )


def _run(settings: ClaudeCodeSettings, tmp_path: Path):
    return run_claudecode_cell(settings, cell_dir=tmp_path / "cells" / "secret-problem-id" / "vanilla", prompt="PROMPT")


def test_allowed_tools_and_auth_rule_are_the_stock_clients() -> None:
    agent = ast.parse((STOCK / "claudecode" / "claudecode_agent.py").read_text())
    tools = next(
        ast.literal_eval(node.value)
        for node in ast.walk(agent)
        if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == "ALLOWED_TOOLS" for t in node.targets)
    )
    assert list(ALLOWED_TOOLS) == tools
    auth = ast.parse((STOCK / "harness" / "claude_auth.py").read_text())
    keys = next(
        ast.literal_eval(node.value)
        for node in ast.walk(auth)
        if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == "AUTH_KEYS" for t in node.targets)
    )
    assert list(AUTH_KEYS) == list(keys)


def test_command_is_the_stock_invocation_plus_headless_denials() -> None:
    command = claudecode_command("claude", model="claude-sonnet-5-5", prompt="P", reasoning_effort="medium")
    assert command[:8] == [
        "claude",
        "--model",
        "claude-sonnet-5-5",
        "--verbose",
        "--output-format",
        "stream-json",
        "-p",
        "P",
    ]
    assert command[8:10] == ["--effort", "medium"]
    assert command[command.index("--allowedTools") + 1 : command.index("--disallowedTools")] == list(ALLOWED_TOOLS)
    assert "AskUserQuestion" in command[-1]
    assert "--effort" not in claudecode_command("claude", model="m", prompt="P")


def test_auth_comes_from_the_profile_settings_and_only_auth_keys(profile: Path) -> None:
    env = claude_auth_env({"CLAUDE_CONFIG_DIR": str(profile), "CLOUD_ML_REGION": "override"})
    assert env == {
        "CLAUDE_CODE_USE_VERTEX": "1",
        "ANTHROPIC_VERTEX_PROJECT_ID": "p",
        "CLOUD_ML_REGION": "override",
    }


def test_a_cell_keeps_its_own_config_home_and_both_streams(tmp_path: Path, fake_claude: Path, profile: Path) -> None:
    result = _run(_settings(tmp_path, fake_claude, profile), tmp_path)

    cell = result.cell_dir
    assert result.returncode == 0
    assert result.error is None
    assert result.transcripts_complete
    session = next((cell / "session-dir" / "projects").glob("**/*.jsonl"))
    assert session.name == "11111111-2222-3333-4444-555555555555.jsonl"
    assert (cell / "session-dir" / "settings.json").read_text() == '{"cleanupPeriodDays": 36500}'
    assert (cell / "transcript.jsonl").read_text().count("\n") == 2
    assert (cell / "claude-code.stderr").exists()
    assert (cell / "prompt.txt").read_text() == "PROMPT"
    clock = json.loads((cell / "clock.json").read_text())
    assert clock["claude_version"] == "9.9.9 (fake)"
    assert clock["model"] == "claude-sonnet-5-5"
    assert result.clock.notice_at <= result.clock.started_at <= result.clock.ended_at
    # the credentials never reach the evidence directory
    assert "ANTHROPIC_VERTEX_PROJECT_ID" not in (cell / "session-dir" / "settings.json").read_text()


def test_the_agent_sees_no_path_that_names_the_problem(tmp_path: Path, fake_claude: Path, profile: Path) -> None:
    _run(_settings(tmp_path, fake_claude, profile), tmp_path)

    probe = json.loads((tmp_path / "probe.json").read_text())
    for value in (probe["cwd"], probe["config"], probe["kubeconfig"]):
        assert "secret-problem-id" not in value
        assert "cells" not in value
    assert probe["kubeconfig_text"] == "kubeconfig-body"
    assert probe["model"] == "claude-sonnet-5-5"
    assert not Path(probe["cwd"]).exists()  # the alias scratch is gone; the real data stays in the cell


def test_accounting_counts_unique_message_ids_with_the_cache_split(
    tmp_path: Path, fake_claude: Path, profile: Path
) -> None:
    result = _run(_settings(tmp_path, fake_claude, profile), tmp_path)

    accounting = result.accounting
    assert accounting.requests == 2  # three records, two message ids
    tokens = accounting.tokens
    assert (tokens.cached_input_tokens, tokens.cache_write_input_tokens, tokens.output_tokens) == (200, 100, 7 + 9)
    assert tokens.input_tokens == 2 * (10 + 100 + 50)
    assert accounting.usd is not None
    assert accounting.usd > 0
    assert accounting.cli_total_cost_usd == pytest.approx(0.0001)
    assert json.loads((result.cell_dir / "accounting.json").read_text())["requests"] == 2


def test_a_timeout_kills_the_process_group_and_still_collects_the_transcripts(
    tmp_path: Path, fake_claude: Path, profile: Path
) -> None:
    started = time.monotonic()
    result = _run(_settings(tmp_path, fake_claude, profile, mode="hang", timeout_seconds=2), tmp_path)

    assert time.monotonic() - started < 60
    assert result.timed_out
    assert "timed out" in (result.error or "")
    assert list((result.cell_dir / "session-dir" / "projects").glob("**/*.jsonl"))
    assert result.accounting.requests == 2
    assert result.transcripts_complete, result.transcript_problems  # the cut stream line is expected after a kill
    assert json.loads((result.cell_dir / "clock.json").read_text())["timed_out"] is True


def test_a_crash_is_an_error_with_its_partial_transcripts(tmp_path: Path, fake_claude: Path, profile: Path) -> None:
    result = _run(_settings(tmp_path, fake_claude, profile, mode="crash"), tmp_path)

    assert result.returncode == 3
    assert result.error == "claudecode exited with code 3"
    assert result.accounting.requests == 2


def test_a_named_subagent_without_its_sidechain_file_makes_the_cell_incomplete(
    tmp_path: Path, fake_claude: Path, profile: Path
) -> None:
    result = _run(_settings(tmp_path, fake_claude, profile, mode="sidechain"), tmp_path)

    assert not result.transcripts_complete
    assert any("subagent a0123456" in item for item in result.transcript_problems)


def test_a_missing_binary_is_an_error_row_not_an_exception(tmp_path: Path, profile: Path) -> None:
    result = _run(_settings(tmp_path, tmp_path / "nope", profile), tmp_path)

    assert result.error is not None
    assert "FileNotFoundError" in result.error
    assert (result.cell_dir / "prompt.txt").exists()
    assert (result.cell_dir / "clock.json").exists()


def test_the_fast_loop_agent_maps_the_cell_to_an_outcome(tmp_path: Path, fake_claude: Path, profile: Path) -> None:
    settings = _settings(tmp_path, fake_claude, profile)
    window = InjectionWindow(T0, T0)
    with SubmissionStub(app_info={"app_name": "a", "namespace": "n"}) as stub:
        agent = ClaudeCodeBaselineAgent(settings, stub=stub, prompt_for=lambda _problem, _api: "PROMPT")
        outcome = agent.resolve(0, "problem-x", lambda: window)

    assert (agent.name, agent.provider, agent.model) == ("claudecode", "claude", "claude-sonnet-5-5")
    assert outcome.injection is window
    assert outcome.dispatched_at is not None
    assert outcome.responder_started_at is not None
    assert outcome.responder_tokens.cache_write_input_tokens == 100
    # the fake never submits to the stub, so the row says so instead of grading a cell with no mitigation
    assert outcome.error == "claudecode exited without submitting a mitigation"
    assert outcome.artifacts_dir is not None
    assert outcome.artifacts_dir.endswith("000_problem-x")


@pytest.mark.parametrize(
    ("argv", "model"),
    [
        (["--agent", "claudecode"], "claude-sonnet-5-5"),
        (["--agent", "claudecode", "--model", "claude-opus-5-5"], "claude-opus-5-5"),
        (["--agent", "codex"], "gpt-6-luna"),
    ],
)
def test_fastloop_run_takes_claudecode_with_the_agents_default_model(argv: list[str], model: str) -> None:
    from benchmarks.sregym.fastloop.cli import build_parser

    args = build_parser().parse_args(["run", "--run-dir", "x", *argv])

    assert args.model == model
