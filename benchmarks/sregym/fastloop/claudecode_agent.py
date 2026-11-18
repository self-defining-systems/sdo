"""SREGym's stock Claude Code agent as a fast-loop agent and a per-cell executor: the vanilla arm.

It mirrors ``third_party/sregym/clients/claudecode``: the same instruction (built by SREGym's own
``build_instruction`` in SREGym's venv), the same ``claude`` flags and allowed tools, auth from the
environment or the ``env`` block of the Claude settings (the ClaudeGCP profile, ``CLAUDE_CONFIG_DIR``), and
the same filtered Kubernetes API proxy. No SDO is involved.

Evidence (audit D3/F5): every cell has its own persistent CLI config home ``session-dir/`` under the cell
directory, the raw stream-json as ``transcript.jsonl`` and stderr separately. The process runs in its own
process group; a timeout or crash kills the group and still collects what was written. Nothing the agent can
read names the problem: its working directory, config home path and kubeconfig are neutral aliases in a
scratch directory that is removed afterwards (the symlinked config home keeps the real data).

``run_claudecode_cell`` is the executor the eval runner calls; ``ClaudeCodeBaselineAgent`` adapts it to
``fastloop --agent claudecode``.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.fuzzops.cellevidence import trial_problems
from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.claudecode_accounting import CellAccounting, account_cell
from benchmarks.sregym.fastloop.codex_agent import SubmissionStub
from benchmarks.sregym.fastloop.loop import AgentOutcome, InjectionWindow
from benchmarks.sregym.fastloop.records import AgentName, TokenCounts
from libs.agent_cli.claude_stream import disallowed_tools_args

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

DEFAULT_MODEL = "claude-sonnet-5-5"
#: ``clients/claudecode/claudecode_agent.py``'s ``ALLOWED_TOOLS`` (a test pins it to the file).
ALLOWED_TOOLS = (
    "Bash",
    "Edit",
    "Write",
    "Read",
    "Glob",
    "Grep",
    "LS",
    "WebFetch",
    "NotebookEdit",
    "NotebookRead",
    "TodoRead",
    "TodoWrite",
    "Agent",
    "Skill",
    "SlashCommand",
    "Task",
    "WebSearch",
)
#: ``clients/harness/claude_auth.py``'s auth variables (a test pins the rule to the file).
AUTH_KEYS = (
    "CLAUDE_CODE_USE_VERTEX",
    "ANTHROPIC_VERTEX_PROJECT_ID",
    "CLOUD_ML_REGION",
    "GOOGLE_CLOUD_PROJECT",
    "GOOGLE_CLOUD_LOCATION",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
)
AUTH_PREFIXES = ("VERTEX_REGION_",)
#: Keeps Claude Code from pruning the cell's transcripts at start-up (default 30 days).
CLEANUP_PERIOD_DAYS = 36500
STREAM_FILE = "transcript.jsonl"
STDERR_FILE = "claude-code.stderr"
SESSION_DIR = "session-dir"
KILL_GRACE_SECONDS = 15.0

_PROMPT_SCRIPT = """
import json, sys
from clients.claudecode.driver import build_instruction
print("PROMPT-JSON " + json.dumps(build_instruction(json.loads(sys.stdin.read()))))
"""


def _is_auth_key(key: str) -> bool:
    return key in AUTH_KEYS or key.startswith(AUTH_PREFIXES)


def claude_auth_env(environ: Mapping[str, str]) -> dict[str, str]:
    """The non-empty auth variables: *environ*'s over the ``env`` block of ``$CLAUDE_CONFIG_DIR/settings.json``."""
    found: dict[str, str] = {}
    raw = environ.get("SREGYM_CLAUDE_AUTH_CONFIG_DIR") or environ.get("CLAUDE_CONFIG_DIR")
    if raw and (path := Path(raw) / "settings.json").is_file():
        block = json.loads(path.read_text()).get("env", {})
        found.update({str(k): str(v) for k, v in block.items() if _is_auth_key(str(k)) and v})
    found.update({k: v for k, v in environ.items() if _is_auth_key(k) and v})
    return found


def claudecode_command(
    binary: str, *, model: str, prompt: str, reasoning_effort: str | None = None, max_turns: int | None = None
) -> list[str]:
    """``clients/claudecode/claudecode_agent.py``'s invocation, plus SDO's headless tool denials."""
    command = [binary, "--model", model, "--verbose", "--output-format", "stream-json", "-p", prompt]
    if reasoning_effort:
        command.extend(["--effort", reasoning_effort])
    if max_turns is not None:
        command.extend(["--max-turns", str(max_turns)])
    command.extend(["--allowedTools", *ALLOWED_TOOLS])
    command.extend(disallowed_tools_args())
    return command


def stock_prompt(sregym_dir: Path, app_info: dict[str, Any], api_base: str, *, timeout: float = 300) -> str:
    """SREGym's own claudecode instruction for *app_info*, pointed at *api_base* (no SDO memory appendix)."""
    from urllib.parse import urlsplit

    endpoint = urlsplit(api_base)
    env = {
        **os.environ,
        "API_HOSTNAME": endpoint.hostname or "127.0.0.1",
        "API_PORT": str(endpoint.port or 80),
    }
    env.pop("SREGYM_SUMMARY_FILE", None)
    done = subprocess.run(
        ["uv", "run", "--directory", str(sregym_dir), "python", "-c", _PROMPT_SCRIPT],
        input=json.dumps(app_info),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    for line in reversed(done.stdout.splitlines()):
        if line.startswith("PROMPT-JSON "):
            return str(json.loads(line.removeprefix("PROMPT-JSON ")))
    raise RuntimeError(f"SREGym claudecode build_instruction failed ({done.returncode}): {done.stderr[-500:]}")


@dataclass(frozen=True)
class ClaudeCodeSettings:
    kubeconfig: Path
    results_dir: Path
    model: str = DEFAULT_MODEL
    timeout_seconds: float = wait_for(WaitKind.RESPONSE, 2)
    reasoning_effort: str | None = None
    max_turns: int | None = None
    claude_binary: str = "claude"
    #: Where the auth comes from when the environment lacks it; default ``$CLAUDE_CONFIG_DIR`` (ClaudeGCP).
    environ: Mapping[str, str] = field(default_factory=lambda: dict(os.environ))

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not self.model:
            raise ValueError("model must not be empty")
        if self.max_turns is not None and self.max_turns < 1:
            raise ValueError("max_turns must be positive")


@dataclass(frozen=True)
class CellClock:
    #: The moment the notice (SREGym's instruction) is handed to Claude Code: the page, for this arm.
    notice_at: datetime
    started_at: datetime
    ended_at: datetime


@dataclass(frozen=True)
class CellResult:
    clock: CellClock
    returncode: int | None
    timed_out: bool
    error: str | None
    transcript_problems: tuple[str, ...]
    accounting: CellAccounting
    cell_dir: Path

    @property
    def transcripts_complete(self) -> bool:
        return not self.transcript_problems


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _kill_group(process: subprocess.Popen[bytes] | subprocess.Popen[str]) -> None:
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except (ProcessLookupError, PermissionError):
            return
        try:
            process.wait(timeout=KILL_GRACE_SECONDS if sig == signal.SIGTERM else 5)
            return
        except subprocess.TimeoutExpired:
            continue


def _fsync_tree(root: Path) -> None:
    for path in root.rglob("*"):
        if path.is_file():
            try:
                with path.open("rb") as handle:
                    os.fsync(handle.fileno())
            except OSError:
                continue


def transcript_problems(cell_dir: Path, *, killed: bool) -> tuple[str, ...]:
    """What is missing from the cell's transcripts; a stream the killed process cut mid-line is expected."""
    problems = trial_problems(cell_dir)
    if killed:
        problems = [item for item in problems if "truncated" not in item]
    stream = cell_dir / STREAM_FILE
    if stream.is_file() and '"session_id"' not in stream.read_text(errors="replace"):
        problems.append(f"{cell_dir.name}: no Claude session started (no session id in the stream)")
    return tuple(problems)


def run_claudecode_cell(
    settings: ClaudeCodeSettings,
    *,
    cell_dir: Path,
    prompt: str,
    on_notice: Callable[[], None] | None = None,
    now: Callable[[], datetime] = _now,
) -> CellResult:
    """Run stock Claude Code once on *prompt*; never lose a transcript, whatever the exit.

    ``on_notice`` runs right before the process starts (e.g. to inject the fault when the page is the instruction).
    """
    cell_dir.mkdir(parents=True, exist_ok=True)
    home = cell_dir / SESSION_DIR
    home.mkdir(exist_ok=True)
    (home / "settings.json").write_text(json.dumps({"cleanupPeriodDays": CLEANUP_PERIOD_DAYS}), encoding="utf-8")
    (cell_dir / "prompt.txt").write_text(prompt, encoding="utf-8")
    auth = claude_auth_env(settings.environ)
    if not (auth.get("ANTHROPIC_API_KEY") or auth.get("CLAUDE_CODE_OAUTH_TOKEN") or auth.get("CLAUDE_CODE_USE_VERTEX")):
        raise RuntimeError(
            "no Claude authentication: set CLAUDE_CONFIG_DIR to the ClaudeGCP profile or the auth variables"
        )

    scratch = Path(tempfile.mkdtemp(prefix="agent-"))
    workdir = scratch / "work"
    workdir.mkdir()
    alias = scratch / "claude"
    alias.symlink_to(home.resolve(), target_is_directory=True)
    kubeconfig = scratch / "kubeconfig"
    shutil.copyfile(settings.kubeconfig, kubeconfig)
    env = {
        **settings.environ,
        **auth,
        "CLAUDE_CONFIG_DIR": str(alias),
        "KUBECONFIG": str(kubeconfig),
        "ANTHROPIC_MODEL": settings.model,
        "FORCE_AUTO_BACKGROUND_TASKS": "1",
        "ENABLE_BACKGROUND_TASKS": "1",
    }
    env.pop("SREGYM_CLAUDE_AUTH_CONFIG_DIR", None)
    if auth.get("CLAUDE_CODE_OAUTH_TOKEN"):
        env.pop("ANTHROPIC_API_KEY", None)
    command = claudecode_command(
        settings.claude_binary,
        model=settings.model,
        prompt=prompt,
        reasoning_effort=settings.reasoning_effort,
        max_turns=settings.max_turns,
    )
    returncode: int | None = None
    timed_out = False
    error: str | None = None
    process: subprocess.Popen[bytes] | None = None
    started = ended = notice = now()
    try:
        if on_notice is not None:
            on_notice()
        notice = now()
        with (cell_dir / STREAM_FILE).open("wb") as stream, (cell_dir / STDERR_FILE).open("wb") as errors:
            try:
                process = subprocess.Popen(
                    command,
                    cwd=workdir,
                    env=env,
                    stdin=subprocess.DEVNULL,
                    stdout=stream,
                    stderr=errors,
                    start_new_session=True,
                )
                started = now()
                try:
                    returncode = process.wait(timeout=settings.timeout_seconds)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    error = f"claudecode timed out after {settings.timeout_seconds:.0f}s"
                    _kill_group(process)
                    returncode = process.returncode
            except BaseException as exc:
                error = error or f"claudecode run failed: {type(exc).__name__}: {exc}"
                if process is not None and process.poll() is None:
                    _kill_group(process)
                    returncode = process.returncode
                if isinstance(exc, KeyboardInterrupt):
                    raise
            finally:
                stream.flush()
                errors.flush()
    finally:
        ended = now()
        alias.unlink(missing_ok=True)
        if any(workdir.iterdir()):
            shutil.copytree(workdir, cell_dir / "workdir", dirs_exist_ok=True)
        shutil.rmtree(scratch, ignore_errors=True)
        _fsync_tree(cell_dir)
    if error is None and returncode != 0:
        error = f"claudecode exited with code {returncode}"
    clock = CellClock(notice, started, ended)
    problems = transcript_problems(cell_dir, killed=timed_out)
    accounting = account_cell(home, cell_dir / STREAM_FILE)
    result = CellResult(clock, returncode, timed_out, error, problems, accounting, cell_dir)
    try:
        version = subprocess.run(
            [settings.claude_binary, "--version"], capture_output=True, text=True, timeout=30, check=False
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        version = ""
    (cell_dir / "clock.json").write_text(
        json.dumps(
            {
                **{k: v.isoformat() for k, v in asdict(clock).items()},
                "returncode": returncode,
                "timed_out": timed_out,
                "error": error,
                "model": settings.model,
                "claude_version": version,
                "transcript_problems": list(problems),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (cell_dir / "accounting.json").write_text(json.dumps(accounting.to_json(), indent=2), encoding="utf-8")
    return result


class ClaudeCodeBaselineAgent:
    name: AgentName = "claudecode"

    def __init__(
        self,
        settings: ClaudeCodeSettings,
        *,
        stub: SubmissionStub,
        prompt_for: Callable[[str, str], str],
    ) -> None:
        self._settings = settings
        self._stub = stub
        self._prompt_for = prompt_for

    @property
    def model(self) -> str:
        return self._settings.model

    @property
    def provider(self) -> str:
        return "claude"

    @property
    def reasoning_effort(self) -> str | None:
        return self._settings.reasoning_effort

    def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
        self._stub.reset()
        prompt = self._prompt_for(problem_id, self._stub.url)
        cell_dir = self._settings.results_dir / f"{index:03d}_{problem_id}"
        windows: list[InjectionWindow] = []
        result = run_claudecode_cell(
            self._settings, cell_dir=cell_dir, prompt=prompt, on_notice=lambda: windows.append(inject())
        )
        submissions = self._stub.submissions()
        diagnosis = next((item.solution for item in submissions if item.stage == "diagnosis"), "")
        mitigation = next((item for item in submissions if item.stage == "mitigation"), None)
        error = result.error
        if error is None and not result.transcripts_complete:
            error = "claudecode transcripts incomplete: " + "; ".join(result.transcript_problems)
        elif error is None and mitigation is None:
            error = "claudecode exited without submitting a mitigation"
        accounting = result.accounting
        return AgentOutcome(
            injection=windows[0],
            dispatched_at=result.clock.notice_at,
            responder_started_at=result.clock.started_at,
            responder_finished_at=result.clock.ended_at,
            mitigation_applied_at=mitigation.received_at if mitigation is not None else result.clock.ended_at,
            resolved_at=result.clock.ended_at,
            diagnosis=diagnosis,
            mitigation=mitigation.solution if mitigation is not None else "",
            responder_tokens=accounting.tokens if accounting else TokenCounts(),
            setup_seconds=None,
            artifacts_dir=str(cell_dir),
            incident_id=f"claudecode-{index:03d}-{int(time.time())}",
            error=error,
        )

    def learn(self, outcome: AgentOutcome) -> AgentOutcome:
        """The baseline is memoryless."""

        return outcome

    def prepare_recovery(self, outcome: AgentOutcome | None) -> None:
        """The baseline has no long-running incident intake."""

    def close(self) -> None:
        """The stub belongs to the caller."""
