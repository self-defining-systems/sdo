"""Structured coding-agent turns for SDO, on top of agentshim.

SDO's deployer, health judge, responder and reflection each run one turn that
must return JSON matching a schema, under one of three access levels. This
module maps those levels onto each provider's own confinement and returns a
provider-neutral result, so callers do not branch on the provider.

Access levels:

- ``read-only``: the agent may read the repository but not write it.
- ``workspace-write``: it may write the repository only; SDO's detector
  gateway (``sdo detector check``) and complete memory check
  (``sdo-operator memory check``) provide compilation and
  run outside the sandbox, so direct ``go`` is refused where the provider can
  express that. Codex reads that exemption only from ``$CODEX_HOME/rules``, so
  each such Codex turn runs in a private Codex home that is deleted afterwards
  and cannot be resumed.
- ``danger-full-access``: no confinement, for sessions that must operate the
  cluster.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from agentshim import (
    AgentShimError,
    ClaudeHook,
    ClaudeProvider,
    CliAgent,
    CliExitError,
    CliTimeoutError,
    CodexProvider,
    CodexSandboxConfig,
    HostCommandExecutor,
    OutputSchema,
    SandboxConfig,
    SessionStarted,
    TokenUsage,
    ToolCall,
    TurnRequest,
    UsageReport,
    cost_usd,
    default_pricing,
    normalized_usage,
    price_for,
)
from agentshim.execution.host import ProcessCommandHandle
from agentshim.providers.codex import install_rules

from libs.agent_cli.session_ownership import (
    HANDOFF_TIMEOUT_SECONDS,
    SessionOwnership,
    WorkerIdentity,
    WorkerTermination,
)
from libs.agent_cli.startup import StartupTelemetry

if TYPE_CHECKING:
    from agentshim import (
        AgentEvent,
        CommandExecutor,
        CommandHandle,
        CommandRequest,
        CommandResult,
        CommandStreamSink,
        McpServer,
        Provider,
        ProviderUsage,
    )

AgentProvider = Literal["codex", "claude"]
logger = logging.getLogger(__name__)

AccessMode = Literal["read-only", "workspace-write", "danger-full-access"]

AGENT_PROVIDERS: tuple[AgentProvider, ...] = ("codex", "claude")
ACCESS_MODES: tuple[AccessMode, ...] = ("read-only", "workspace-write", "danger-full-access")

#: The command that stays outside the shell sandbox in workspace-write: it
#: starts the no-network validator container and needs the container runtime.
DETECTOR_GATEWAY_COMMAND = "sdo detector check"

#: The complete operational-memory acceptance command, shared with the broker.
MEMORY_CHECK_COMMAND = "sdo-operator memory check"

#: Overrides the directory per-turn Codex homes are created in. The default is
#: ``$XDG_CACHE_HOME/sdo/codex-homes`` (``~/.cache/sdo/codex-homes``). It must
#: be outside the workspace and the system temp dir.
CODEX_HOME_ROOT_ENV = "SDO_CODEX_HOME_ROOT"
# Optional JSONL file that receives one token-accounting record per turn.
TURN_USAGE_LOG_ENV = "SDO_TURN_USAGE_LOG"

#: How ``StructuredTurn.model_requests`` was measured. agentshim reports one
#: ``turn`` per ``codex exec`` run, so Codex requests are counted from the
#: session rollout's ``token_count`` events (one per model response); Claude
#: reports its agentic turns (``num_turns``), one per model request.
CODEX_ROLLOUT_REQUEST_SOURCE = "codex-rollout-token-count"
CLAUDE_TURNS_REQUEST_SOURCE = "claude-num-turns"

#: Environment variables that authenticate Codex without an ``auth.json``.
_CODEX_API_KEY_ENV = ("CODEX_API_KEY", "OPENAI_API_KEY")

#: Refuses direct execution of the executables named on its argv.
DENY_EXECUTABLES_HOOK = str(Path(__file__).absolute().parent / "hooks" / "deny_bash_executables.py")

#: A loaded host can take longer than agentshim's default to answer ``--help``.
_CLI_CHECK_TIMEOUT_S = 60.0

#: Tool names whose ``command`` argument is a shell command: Codex's
#: ``command_execution`` items and Claude's Bash tool.
_SHELL_TOOLS = frozenset({"execute", "Bash"})


class StructuredTurnError(RuntimeError):
    """A structured turn failed or returned something unusable."""


class StructuredTurnTimeout(StructuredTurnError):
    """A structured turn did not finish within its timeout."""

    def __init__(self, timeout_seconds: float) -> None:
        self.timeout_seconds = timeout_seconds
        super().__init__(f"agent turn timed out after {timeout_seconds:g}s")


class StructuredTurnInterrupted(StructuredTurnError):
    """The caller stopped a running turn; its session can be resumed.

    ``usage`` holds the accounting the stopped turn had already spent.
    """

    def __init__(self, session_id: str, usage: dict[str, int | float]) -> None:
        self.session_id = session_id
        self.usage = usage
        super().__init__(f"agent turn interrupted (session {session_id or 'unknown'})")


@dataclass(frozen=True)
class StructuredTurn:
    """What one structured turn produced.

    Attributes:
        output_json: The schema-conformant output, serialized.
        session_id: The provider session, for resuming this conversation.
        usage: Token and cost accounting reported by the provider.
        shell_commands: Every shell command the agent started, in order, for
            confinement audits.
        tool_calls: Tool calls of every kind the agent started.
        model_requests: Model requests this turn made, or ``None`` when the
            provider does not expose them (see ``model_requests_source``).
        model_requests_source: How ``model_requests`` was measured.
        resumed: Whether the turn resumed an existing session.
    """

    output_json: str
    session_id: str
    usage: ProviderUsage
    shell_commands: tuple[str, ...] = field(default=())
    tool_calls: int = 0
    model_requests: int | None = None
    model_requests_source: str | None = None
    resumed: bool = False
    skill_directory: Path | None = None


def turn_usage(turn: StructuredTurn) -> dict[str, int | float]:
    """Flatten one turn's provider accounting into the SDO usage record shape.

    The token breakdown is agentshim's normalized one, the same for Codex and
    Claude: ``input_tokens`` includes cache reads and writes, and
    ``uncached_input_tokens`` is the rest; ``output_tokens`` includes
    ``reasoning_output_tokens``. ``cached_input_tokens`` is the deprecated
    alias of ``cache_read_input_tokens`` (cache reads only).
    """

    tokens = turn.usage.tokens
    usage: dict[str, int | float] = {
        "llm_calls": tokens.turns,
        "input_tokens": tokens.input_tokens,
        "output_tokens": tokens.output_tokens,
        "cached_input_tokens": tokens.cached_input_tokens,
        "cache_read_input_tokens": tokens.cache_read_input_tokens,
        "cache_write_input_tokens": tokens.cache_write_input_tokens,
        "cache_write_1h_input_tokens": tokens.cache_write_1h_input_tokens,
        "uncached_input_tokens": tokens.uncached_input_tokens,
        "reasoning_output_tokens": tokens.reasoning_output_tokens,
    }
    if turn.usage.total_cost_usd is not None:
        usage["total_cost_usd"] = turn.usage.total_cost_usd
    if turn.model_requests is not None:
        usage["model_requests"] = turn.model_requests
    return usage


@dataclass
class _ShellCommandRecorder:
    commands: list[str] = field(default_factory=list[str])
    tool_calls: int = 0
    on_session_started: Callable[[str], None] | None = None
    on_usage: Callable[[dict[str, int | float], bool], None] | None = None
    session_id: str | None = None
    incremental: dict[str, int | float] = field(default_factory=dict[str, int | float])

    def on_event(self, event: AgentEvent) -> None:
        if isinstance(event, SessionStarted):
            self.session_id = event.session_id
            if self.on_session_started is not None:
                self.on_session_started(event.session_id)
        if isinstance(event, UsageReport) and self.on_usage is not None and event.usage.provider == "claude":
            observed = turn_usage(StructuredTurn("", self.session_id or "", event.usage))
            # Claude assistant reports are incremental; the terminal report has
            # num_turns and supersedes them. Codex reports only at turn end.
            if event.usage.provider == "claude" and event.usage.tokens.turns == 0:
                for name, value in observed.items():
                    self.incremental[name] = self.incremental.get(name, 0) + value
                self.incremental["llm_calls"] += 1
                self.incremental["model_requests"] = self.incremental["llm_calls"]
                self.on_usage(dict(self.incremental), False)
            else:
                self.on_usage(observed, False)
        if not isinstance(event, ToolCall):
            return
        self.tool_calls += 1
        if event.tool not in _SHELL_TOOLS:
            return
        if isinstance(event.args, Mapping):
            command = event.args.get("command")
            if isinstance(command, str):
                self.commands.append(command)


def run_structured_turn(
    provider: AgentProvider,
    prompt: str,
    *,
    output_schema: Mapping[str, object],
    cwd: str | Path,
    access: AccessMode,
    model: str | None = None,
    reasoning_effort: str | None = None,
    timeout_seconds: float | None = None,
    resume_session_id: str | None = None,
    executor: CommandExecutor | None = None,
    mcp_servers: Sequence[McpServer] = (),
    environment: Mapping[str, str] | None = None,
    on_session_started: Callable[[str], None] | None = None,
    on_usage: Callable[[dict[str, int | float], bool], None] | None = None,
    session_ownership_directory: Path | None = None,
    session_ownership_role: Literal["responder", "reflection"] = "responder",
    session_ownership_worker: WorkerIdentity | None = None,
    session_termination_observer: Callable[[WorkerIdentity, float], WorkerTermination | None] | None = None,
    interrupt_when: Callable[[], bool] | None = None,
) -> StructuredTurn:
    """Run one structured turn, fresh or resumed, and return its output.

    ``interrupt_when`` is polled while the turn runs; once it returns true the
    running CLI is stopped. A turn that still finished normally is returned.

    Raises:
        StructuredTurnInterrupted: ``interrupt_when`` stopped the turn.
        StructuredTurnTimeout: The turn exceeded ``timeout_seconds``.
        StructuredTurnError: The CLI failed, or reported no session id or no
            structured output.
        SessionHandoffError: Persisted writer ownership does not authorize a resume.
        ValueError: An argument is invalid before any process starts.
    """
    if provider not in AGENT_PROVIDERS:
        raise ValueError(f"unsupported agent provider {provider!r}")
    if access not in ACCESS_MODES:
        raise ValueError(f"unsupported access mode {access!r}")
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if reasoning_effort is not None and not reasoning_effort.strip():
        raise ValueError("reasoning_effort must not be empty")
    if resume_session_id is not None and not resume_session_id.strip():
        raise ValueError("resume_session_id must not be empty")

    if resume_session_id is not None and provider == "codex" and access == "workspace-write":
        raise ValueError("a Codex workspace-write turn cannot be resumed: its session lives in a per-turn CODEX_HOME")

    workspace = Path(cwd).resolve()
    ownership = (
        SessionOwnership(
            session_ownership_directory,
            provider,
            role=session_ownership_role,
            worker=session_ownership_worker,
            termination_observer=session_termination_observer,
            timeout_seconds=min(timeout_seconds, HANDOFF_TIMEOUT_SECONDS)
            if timeout_seconds is not None
            else HANDOFF_TIMEOUT_SECONDS,
        )
        if session_ownership_directory is not None
        else None
    )

    def session_started(session_id: str) -> None:
        # Persist ownership before exposing the ID to the controller checkpoint.
        if ownership is not None:
            ownership.claim(session_id, resume=resume_session_id is not None)
        if on_session_started is not None:
            on_session_started(session_id)

    recorder = _ShellCommandRecorder(on_session_started=session_started, on_usage=on_usage)
    started = time.monotonic()
    interrupted = threading.Event()
    codex_home: Path | None = None
    rollout_before: _RolloutTotals | None = None

    def spent_usage() -> dict[str, int | float]:
        """Accounting of a stopped turn: its rollout delta, or Claude's incremental reports."""
        if provider != "codex":
            return dict(recorder.incremental)
        latest = _codex_rollout_totals(codex_home, recorder.session_id or resume_session_id)
        delta = latest.since(rollout_before) if latest is not None else None
        if delta is None or delta.tokens is None or delta.requests == 0:
            return {}
        from agentshim import ProviderUsage

        return turn_usage(
            StructuredTurn(
                "", recorder.session_id or "", ProviderUsage(tokens=delta.tokens), model_requests=delta.requests
            )
        )

    def stopped() -> StructuredTurnInterrupted:
        usage = spent_usage()
        if usage and on_usage is not None:
            on_usage(usage, True)
        return StructuredTurnInterrupted(recorder.session_id or resume_session_id or "", usage)

    try:
        with ExitStack() as turn_scope:
            writer = _WriterExecutor(executor or HostCommandExecutor())
            if ownership is not None:
                turn_scope.callback(lambda: ownership.close(writer_closed=writer.closed))
                if resume_session_id is not None:
                    ownership.claim(resume_session_id, resume=True)
            executor = writer
            selected, extra_args, extra_env = _provider_for(provider, access, workspace, turn_scope)
            startup = turn_scope.enter_context(StartupTelemetry(provider))
            if provider == "codex":
                extra_args = [*extra_args, *startup.codex_args()]
            if startup.endpoint:
                executor = startup.executor(executor)
            effective_environment = {**os.environ, **(environment or {}), **extra_env}
            home_variable = "CODEX_HOME" if provider == "codex" else "CLAUDE_CONFIG_DIR"
            default_home = Path.home() / (".codex" if provider == "codex" else ".claude")
            skill_directory = Path(effective_environment.get(home_variable) or default_home).expanduser() / "skills"
            agent = CliAgent(
                selected,
                model=model,
                executor=executor,
                env=effective_environment,
                event_handler=recorder,
                check_timeout=_CLI_CHECK_TIMEOUT_S,
            )
            if provider == "claude" and mcp_servers:
                _exclude_claude_mcp_config(workspace)
            # Ownership waiting consumes the existing turn budget.
            remaining_timeout = timeout_seconds
            if ownership is not None and timeout_seconds is not None:
                remaining_timeout = timeout_seconds - (time.monotonic() - started)
                if remaining_timeout <= 0:
                    raise StructuredTurnTimeout(timeout_seconds)
            session = agent.start_session(cwd=str(workspace), timeout=remaining_timeout)
            if resume_session_id is not None and not session.adopt(resume_session_id):
                raise StructuredTurnError(f"{provider} cannot resume session {resume_session_id!r}")
            codex_home = _codex_session_home(effective_environment) if provider == "codex" else None
            rollout_before = _codex_rollout_totals(codex_home, resume_session_id)
            if (
                on_usage is not None
                and provider == "codex"
                and resume_session_id is not None
                and (rollout_before is None or (rollout_before.requests > 0 and rollout_before.totals is None))
            ):
                raise StructuredTurnError(
                    "cannot checkpoint resumed usage without a readable cumulative token baseline"
                )
            schema_dir = turn_scope.enter_context(tempfile.TemporaryDirectory(prefix="sdo-structured-turn-"))
            # Codex exposes response accounting in its rollout before its CLI
            # emits a terminal usage event. Poll only in the worker, never Go.
            stop_usage = threading.Event()
            usage_errors: list[Exception] = []

            def sample_usage() -> None:
                if provider != "codex" or on_usage is None:
                    return
                latest = _codex_rollout_totals(codex_home, recorder.session_id or resume_session_id)
                delta = latest.since(rollout_before) if latest is not None else None
                if delta is not None and delta.tokens is not None and delta.requests > 0:
                    from agentshim import ProviderUsage

                    snapshot = StructuredTurn(
                        "", recorder.session_id or "", ProviderUsage(tokens=delta.tokens), model_requests=delta.requests
                    )
                    on_usage(turn_usage(snapshot), False)

            def monitor_usage() -> None:
                try:
                    while not stop_usage.wait(0.1):
                        sample_usage()
                except Exception as exc:
                    usage_errors.append(exc)

            monitor = threading.Thread(target=monitor_usage, name="sdo-usage-checkpoint", daemon=True)
            if on_usage is not None:
                monitor.start()

            def watch_interrupt() -> None:
                assert interrupt_when is not None
                while not stop_usage.wait(0.2):
                    if interrupt_when():
                        interrupted.set()
                        session.cancel()
                        return

            watcher = threading.Thread(target=watch_interrupt, name="sdo-turn-interrupt", daemon=True)
            if interrupt_when is not None:
                watcher.start()
            try:
                result = session.turn(
                    TurnRequest(
                        prompt=prompt,
                        output_schema=OutputSchema(schema=dict(output_schema), host_dir=Path(schema_dir)),
                        reasoning_effort=reasoning_effort,
                        extra_args=extra_args,
                        mcp_servers=mcp_servers,
                    )
                )
            finally:
                stop_usage.set()
                if interrupt_when is not None:
                    watcher.join()
                if on_usage is not None:
                    monitor.join()
                    sample_usage()
                    if usage_errors:
                        raise StructuredTurnError(f"usage checkpoint failed: {usage_errors[0]}") from usage_errors[0]
            # Read the rollout now: a workspace-write Codex home is deleted
            # when the turn scope closes.
            rollout_after = _codex_rollout_totals(codex_home, result.session_id or resume_session_id)
    except CliTimeoutError as exc:
        if interrupted.is_set():
            raise stopped() from exc
        raise StructuredTurnTimeout(exc.timeout) from exc
    except CliExitError as exc:
        if interrupted.is_set():
            raise stopped() from exc
        # Both streams, stderr first: a provider may report the real cause on
        # either, and callers hand this text back to the next attempt.
        details = "\n".join(part.strip() for part in (exc.stderr, exc.stdout) if part.strip())
        raise StructuredTurnError(f"{provider} exited with code {exc.returncode}: {details or 'no output'}") from exc
    except AgentShimError as exc:
        if interrupted.is_set():
            raise stopped() from exc
        raise StructuredTurnError(f"{provider} turn failed: {exc}") from exc

    if on_usage is not None and provider == "codex" and resume_session_id is not None:
        delta = rollout_after.since(rollout_before) if rollout_after is not None else None
        if delta is None or delta.tokens is None:
            # The CLI's whole-session total cannot safely become a turn delta.
            # Keep any observed partial checkpoint rather than billing it twice.
            raise StructuredTurnError("resumed cumulative token accounting unavailable after turn")

    structured_output: object = result.structured_output
    session_id = result.session_id or resume_session_id or ""
    model_requests: int | None = None
    model_requests_source: str | None = None
    usage = result.usage
    if provider == "claude" and result.usage.tokens.turns > 0:
        model_requests, model_requests_source = result.usage.tokens.turns, CLAUDE_TURNS_REQUEST_SOURCE
    elif rollout_after is not None:
        turn_totals = rollout_after.since(rollout_before)
        if turn_totals is not None:
            model_requests, model_requests_source = turn_totals.requests, CODEX_ROLLOUT_REQUEST_SOURCE
            # ``codex exec resume`` reports the whole session's usage at turn
            # end, so a resumed turn would inherit every earlier turn; the
            # rollout's cumulative counters give this turn's own delta (and
            # the reasoning tokens the stream omits).
            if turn_totals.tokens is not None:
                usage = replace(result.usage, tokens=turn_totals.tokens)
    turn = StructuredTurn(
        output_json=json.dumps(structured_output),
        session_id=session_id,
        usage=usage,
        shell_commands=tuple(recorder.commands),
        tool_calls=recorder.tool_calls,
        model_requests=model_requests,
        model_requests_source=model_requests_source,
        resumed=resume_session_id is not None,
        skill_directory=skill_directory,
    )
    # A model can spend tokens and then fail the structured-response contract.
    # Persist its observed accounting before rejecting that response.
    if on_usage is not None:
        on_usage(turn_usage(turn), True)
    _append_turn_usage(provider, model, workspace, turn, time.monotonic() - started)
    if not isinstance(structured_output, dict):
        raise StructuredTurnError(f"{provider} turn returned no structured output")
    if not session_id:
        raise StructuredTurnError(f"{provider} turn did not report a session id")
    return turn


class _WriterExecutor:
    """Track the executor's terminal process evidence, independently of parsing.

    Local closure requires a reaped process; remote closure requires a terminal
    result/exit. Unknown transport failures leave the durable owner active, including
    after restart; an OS lock disappearing does not manufacture a release.
    """

    def __init__(self, live: CommandExecutor) -> None:
        self.live = live
        self.closed = True

    def find_binary(self, name: str, env: Mapping[str, str]) -> str:
        return self.live.find_binary(name, env)

    def check_binary(self, path: str, env: Mapping[str, str], *, timeout: float) -> None:
        self.live.check_binary(path, env, timeout=timeout)

    def run(self, request: CommandRequest, sink: CommandStreamSink) -> CommandResult:
        self.closed = False
        process: subprocess.Popen[str] | None = None
        terminal = False

        class Sink:
            def started(self, handle: CommandHandle) -> None:
                nonlocal process
                if isinstance(handle, ProcessCommandHandle):
                    process = handle.process
                sink.started(handle)

            def stdout(self, line: str) -> None:
                sink.stdout(line)

            def stderr(self, line: str) -> None:
                sink.stderr(line)

        try:
            result = self.live.run(request, Sink())
            terminal = True
            return result
        except CliExitError:
            terminal = True
            raise
        finally:
            # For local processes, inspect actual reaping even on cancellation
            # or callback failure. A timeout alone never proves writer closure.
            # Remote executors must return a terminal result/exit to release.
            self.closed = process.poll() is not None if process is not None else terminal


def _exclude_claude_mcp_config(workspace: Path) -> None:
    """Keep the ``.mcp.json`` Claude gets for the turn out of commits the agent makes in the checkout.

    A responder may commit source during its turn, and the broker stages the checkout with ``add --all``.
    """

    probe = subprocess.run(
        ["git", "rev-parse", "--git-path", "info/exclude"], cwd=workspace, capture_output=True, text=True, check=False
    )
    if probe.returncode != 0:
        return
    exclude = workspace / probe.stdout.strip()
    entry = "/.mcp.json"
    existing = exclude.read_text(encoding="utf-8") if exclude.exists() else ""
    if entry in existing.split():
        return
    exclude.parent.mkdir(parents=True, exist_ok=True)
    separator = "" if not existing or existing.endswith("\n") else "\n"
    exclude.write_text(f"{existing}{separator}{entry}\n", encoding="utf-8")


def _append_turn_usage(
    provider: AgentProvider, model: str | None, workspace: Path, turn: StructuredTurn, duration_seconds: float
) -> None:
    """Append one accounting record when ``SDO_TURN_USAGE_LOG`` names a file."""

    path = os.environ.get(TURN_USAGE_LOG_ENV, "").strip()
    if not path:
        return
    record = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "provider": provider,
        "model": model,
        "cwd": str(workspace),
        "session_id": turn.session_id,
        "resumed": turn.resumed,
        "duration_seconds": duration_seconds,
        "model_requests": turn.model_requests,
        "model_requests_source": turn.model_requests_source,
        "tool_calls": turn.tool_calls,
        "shell_commands": len(turn.shell_commands),
        "shell_command_lines": [_bounded_command(command) for command in turn.shell_commands],
        "usage": turn_usage(turn),
    }
    if turn.usage.total_cost_usd is not None:
        record["cost_usd"] = turn.usage.total_cost_usd
        record["cost_source"] = "provider"
    elif (pricing := price_for(provider, model, default_pricing())) is not None:
        record["cost_usd"] = cost_usd(turn.usage.tokens, pricing)
        record["cost_source"] = "agentshim-pricing"
    else:
        record["cost_usd"] = None
        record["cost_source"] = "pricing-unavailable"
    log = Path(path).expanduser()
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")


#: Upper bound on one shell command recorded in the usage log.
_MAX_LOGGED_COMMAND_CHARS = 2_000


def _bounded_command(command: str) -> str:
    if len(command) <= _MAX_LOGGED_COMMAND_CHARS:
        return command
    return f"{command[:_MAX_LOGGED_COMMAND_CHARS]}... [{len(command) - _MAX_LOGGED_COMMAND_CHARS} chars omitted]"


def _codex_session_home(extra_env: Mapping[str, str]) -> Path:
    """The ``CODEX_HOME`` a Codex turn records its session rollout under."""

    home = extra_env.get("CODEX_HOME") or os.environ.get("CODEX_HOME") or str(Path.home() / ".codex")
    return Path(home).expanduser()


_ROLLOUT_TOKEN_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "cache_write_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
)


@dataclass(frozen=True)
class _RolloutTotals:
    """Model responses and cumulative token usage recorded in one Codex session rollout.

    ``totals`` is the last ``total_token_usage`` Codex wrote, a running total
    over the whole session; ``None`` when the rollout carries no totals.
    """

    requests: int
    totals: Mapping[str, int] | None

    def since(self, earlier: _RolloutTotals | None) -> _TurnTotals | None:
        """What this session recorded after *earlier* (a fresh session when ``None``)."""

        before_requests = earlier.requests if earlier is not None else 0
        if self.requests < before_requests:
            return None
        before = earlier.totals if earlier is not None else None
        known_before = before is not None or before_requests == 0
        tokens: TokenUsage | None = None
        if self.totals is not None and known_before:
            delta = {key: self.totals[key] - (before or {}).get(key, 0) for key in _ROLLOUT_TOKEN_FIELDS}
            if all(value >= 0 for value in delta.values()):
                tokens = normalized_usage(
                    input_tokens=delta["input_tokens"],
                    output_tokens=delta["output_tokens"],
                    cache_read_input_tokens=delta["cached_input_tokens"],
                    cache_write_input_tokens=delta["cache_write_input_tokens"],
                    reasoning_output_tokens=delta["reasoning_output_tokens"],
                    turns=1,
                )
        return _TurnTotals(requests=self.requests - before_requests, tokens=tokens)


@dataclass(frozen=True)
class _TurnTotals:
    requests: int
    tokens: TokenUsage | None


def _codex_rollout_totals(home: Path | None, session_id: str | None) -> _RolloutTotals | None:
    """Count model responses and read cumulative usage from a Codex session's rollout files.

    Codex writes one ``token_count`` event carrying usage after every model
    response; events without usage (rate-limit updates) are not requests.
    Each carries ``total_token_usage``, the session's running total, and
    ``last_token_usage``, that one response's usage. Returns ``None`` when
    the rollout cannot be found or read.
    """

    if home is None or not session_id:
        return None
    sessions = home / "sessions"
    try:
        rollouts = sorted(sessions.rglob(f"rollout-*{session_id}.jsonl")) if sessions.is_dir() else []
        if not rollouts:
            return None
        count = 0
        totals: Mapping[str, int] | None = None
        for rollout in rollouts:
            with rollout.open(encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    if '"token_count"' not in line:
                        continue
                    info = _model_response_usage(line)
                    if info is None:
                        continue
                    count += 1
                    total = info.get("total_token_usage")
                    if isinstance(total, dict):
                        counters = cast("dict[str, int | None]", total)
                        totals = {key: int(counters.get(key) or 0) for key in _ROLLOUT_TOKEN_FIELDS}
    except OSError:
        return None
    return _RolloutTotals(requests=count, totals=totals)


def _model_response_usage(line: str) -> dict[str, object] | None:
    """The usage ``info`` of a ``token_count`` event, or ``None`` for other lines."""

    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(record, dict):
        return None
    payload = cast("dict[str, object]", record).get("payload")
    if not isinstance(payload, dict):
        return None
    fields = cast("dict[str, object]", payload)
    info = fields.get("info")
    if fields.get("type") != "token_count" or not isinstance(info, dict):
        return None
    return cast("dict[str, object]", info)


def _provider_for(
    provider: AgentProvider, access: AccessMode, workspace: Path, turn_scope: ExitStack
) -> tuple[Provider, Sequence[str], dict[str, str]]:
    """Build the agentshim provider, extra CLI arguments and env for *access*.

    Anything the turn needs on disk is registered on *turn_scope*, which
    removes it when the turn ends, however it ends.
    """
    if provider == "codex":
        if access != "workspace-write":
            return CodexProvider(sandbox=CodexSandboxConfig(mode=access)), (), {}
        sandbox = CodexSandboxConfig(
            mode="workspace-write", excluded_commands=[DETECTOR_GATEWAY_COMMAND, MEMORY_CHECK_COMMAND]
        )
        home = turn_scope.enter_context(_codex_turn_home(sandbox))
        return CodexProvider(sandbox=sandbox), (), {"CODEX_HOME": str(home)}
    if access == "danger-full-access":
        return ClaudeProvider(), (), {}
    root = str(workspace)
    # The Skill tool loads SKILL.md itself, but a skill's references are read
    # with the native Read tool, which the confinement would otherwise deny.
    readable = [root, str(_claude_skills_dir())]
    if access == "read-only":
        claude = ClaudeProvider(sandbox=SandboxConfig(deny_write=[root], confine_native_reads_to=readable))
        return claude, ("--disallowedTools", "Edit,Write,NotebookEdit"), claude.sandbox_env
    claude = ClaudeProvider(
        sandbox=SandboxConfig(
            confine_native_reads_to=readable,
            excluded_commands=[DETECTOR_GATEWAY_COMMAND, MEMORY_CHECK_COMMAND],
        ),
        hooks=[
            ClaudeHook(
                event="PreToolUse",
                matcher="Bash",
                command=[sys.executable, DENY_EXECUTABLES_HOOK, "go"],
            )
        ],
    )
    return claude, (), claude.sandbox_env


@contextmanager
def _codex_turn_home(sandbox: CodexSandboxConfig) -> Iterator[Path]:
    """Yield a private ``CODEX_HOME`` holding *sandbox*'s exemptions, then delete it.

    Codex reads ``excluded_commands`` from ``$CODEX_HOME/rules``, so the home
    must lie outside everything the sandbox lets commands write, or a command
    could exempt itself; agentshim refuses the turn otherwise. It must also
    avoid the system temp dir, where Codex will not start its sandbox helper.
    Only ``auth.json`` and the SDO skills are copied in: the user's ``config.toml`` and rules
    could widen the sandbox or trust the workspace's own rules.
    """
    auth = _codex_auth_file()
    root = _codex_home_root()
    try:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        home = Path(tempfile.mkdtemp(prefix="turn-", dir=root))
    except OSError as exc:
        raise StructuredTurnError(
            f"cannot create a Codex home under {root} (set {CODEX_HOME_ROOT_ENV}): {exc}"
        ) from exc
    try:
        if auth is not None:
            _copy_private(auth, home / "auth.json")
        _copy_sdo_skills(Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex").expanduser(), home)
        install_rules(home, sandbox)
    except OSError as exc:
        shutil.rmtree(home, ignore_errors=True)
        raise StructuredTurnError(f"cannot prepare the Codex home {home}: {exc}") from exc
    try:
        yield home
    finally:
        if auth is not None:
            _keep_refreshed_login(home / "auth.json", auth)
        shutil.rmtree(home, ignore_errors=True)


def _claude_skills_dir() -> Path:
    """Where Claude Code discovers user skills."""
    return Path(os.environ.get("CLAUDE_CONFIG_DIR") or Path.home() / ".claude").expanduser() / "skills"


def _copy_sdo_skills(source_home: Path, home: Path) -> None:
    """Copy the SDO skills (``sdo-*``) from the launching Codex home into a per-turn home.

    Codex discovers user skills under ``$CODEX_HOME/skills``. Other skills in the
    user's home are not copied: the turn home is deliberately minimal.
    """
    source = source_home / "skills"
    if not source.is_dir():
        return
    for skill in sorted(source.glob("sdo-*")):
        if skill.is_dir():
            shutil.copytree(skill, home / "skills" / skill.name)


def _codex_home_root() -> Path:
    configured = os.environ.get(CODEX_HOME_ROOT_ENV)
    if configured:
        return Path(configured).expanduser().resolve()
    cache = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return (Path(cache).expanduser() / "sdo" / "codex-homes").resolve()


def _codex_auth_file() -> Path | None:
    """The user's Codex login, or ``None`` when an API key in the env replaces it."""
    source_home = os.environ.get("CODEX_HOME") or str(Path.home() / ".codex")
    auth = Path(source_home).expanduser() / "auth.json"
    if auth.is_file():
        return auth
    if any(os.environ.get(name) for name in _CODEX_API_KEY_ENV):
        return None
    raise StructuredTurnError(
        f"Codex workspace-write turns need credentials in a private CODEX_HOME, but {auth} does not exist "
        f"and none of {', '.join(_CODEX_API_KEY_ENV)} is set; run `codex login` or export an API key"
    )


def _keep_refreshed_login(copy: Path, original: Path) -> None:
    """Write a login Codex refreshed during the turn back to the user's home.

    Codex rotates ChatGPT refresh tokens, so discarding the refreshed copy
    would leave the original holding a revoked token.
    """
    try:
        refreshed = copy.read_bytes()
        if refreshed == original.read_bytes():
            return
        staged = original.with_name(f".{original.name}.{os.getpid()}.sdo")
        staged.unlink(missing_ok=True)
        _write_private(staged, refreshed)
        staged.replace(original)
    except OSError:
        logger.warning("could not keep the Codex login refreshed during a turn", exc_info=True)


def _copy_private(source: Path, target: Path) -> None:
    _write_private(target, source.read_bytes())


def _write_private(target: Path, content: bytes) -> None:
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(content)
