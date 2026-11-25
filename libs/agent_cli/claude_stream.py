"""One Claude Code conversation over a long-lived ``stream-json`` process.

SDO's agents are one-shot requests: they decide for themselves when they are done. A
responder that starts background subagents and has nothing else to do must end its turn, and a
required output schema makes that turn end with a structured result. Claude Code then starts a
turn of its own when a background task finishes, and that later turn holds the answer that
counts. A request that is read only up to its first result would hand the early result back
while the agent's own tasks still run.

``ClaudeStreamSession`` reads on: after the turn ends it keeps reading the process until the CLI
itself reports ``session_state_changed: idle`` with no registered background task left (subagents
and background Bash alike: the CLI reports idle after a result while a background Bash runs, and
starts a turn of its own when it finishes; see ``SessionWatch.working``), and returns the last
structured result. SDO injects no turn and no context; it only does not stop reading. A CLI that
reports no session state gives no such signal, and then the first result stands.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, cast

from agentshim import (
    AgentEvent,
    ApprovalPolicy,
    ClaudeStreamTransport,
    CliTimeoutError,
    Conversation,
    ConversationSpec,
    FrameDirection,
    NativeMode,
    NativePermissions,
    RawFrame,
    TurnCancelledError,
    TurnRequest,
    TurnResult,
    TurnTimeoutError,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from agentshim import (
        CommandExecutor,
        CommandRequest,
        CommandResult,
        CommandStreamSink,
        IdleConversation,
        Process,
        SpawnRequest,
    )

#: Makes the CLI report ``system/session_state_changed`` (``running`` while it works, ``idle`` once
#: nothing is queued and no background task will start another turn). It is the CLI's own signal that
#: the agent is done, so no timer decides.
SESSION_STATE_ENV = "CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS"

#: Claude Code tools that cannot work in a headless one-shot session, denied for every SDO agent.
#: The first group starts a later turn of the session (a wakeup, a cron job, a remote trigger) that
#: never begins: the CLI does not count them as background tasks, so the session goes idle and
#: SDO closes it. The second needs a person (a question, plan approval, a push notification) or moves
#: the session out of its checkout (worktrees). ``Monitor`` and ``Bash`` ``run_in_background`` do
#: register as background tasks the CLI keeps the session alive for, and subagents (``Agent``) too,
#: so they stay available.
HEADLESS_DISALLOWED_TOOLS = (
    "ScheduleWakeup",
    "CronCreate",
    "CronDelete",
    "CronList",
    "RemoteTrigger",
    "AskUserQuestion",
    "EnterPlanMode",
    "ExitPlanMode",
    "PushNotification",
    "EnterWorktree",
    "ExitWorktree",
)


def disallowed_tools_args(extra: Sequence[str] = ()) -> tuple[str, ...]:
    """The one ``--disallowedTools`` argument: the headless-unusable tools plus *extra*."""
    return ("--disallowedTools", ",".join((*HEADLESS_DISALLOWED_TOOLS, *extra)))


#: How often the drain looks at the process again; it decides nothing.
POLL_SECONDS = 0.2


@dataclass
class SessionWatch:
    """The CLI's own session state and result frames, read from its raw output."""

    state: str | None = None
    #: Background tasks the CLI lists as registered (``background_tasks_changed``): subagents and
    #: background Bash alike. The CLI reports ``idle`` after a turn's result even while one runs.
    tasks: int = 0
    #: A task finished while the CLI was idle, so it will start a turn of its own; not yet running.
    wake_pending: bool = False
    results: list[dict[str, Any]] = field(default_factory=lambda: cast("list[dict[str, Any]]", []))
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def observe(self, line: str) -> None:
        """Record one stdout line of the process."""
        try:
            frame = json.loads(line)
        except ValueError:
            return
        if not isinstance(frame, dict):
            return
        record = cast("dict[str, Any]", frame)
        with self._lock:
            if record.get("type") == "system" and record.get("subtype") == "session_state_changed":
                state = record.get("state")
                self.state = state if isinstance(state, str) else None
                if self.state == "running":
                    self.wake_pending = False
            elif record.get("type") == "system" and record.get("subtype") == "background_tasks_changed":
                tasks = record.get("tasks")
                self.tasks = len(cast("list[Any]", tasks)) if isinstance(tasks, list) else 0
            elif record.get("type") == "system" and record.get("subtype") == "task_notification":
                if self.state != "running":
                    self.wake_pending = True
            elif record.get("type") == "result":
                self.results.append(record)

    @property
    def working(self) -> bool:
        """The CLI is running, or registered background work remains or is about to wake it.

        False when it never reports state (an old CLI): there is no signal to wait for.
        """
        with self._lock:
            if self.state is None:
                return False
            return self.state == "running" or self.tasks > 0 or self.wake_pending

    def final_result(self) -> dict[str, Any] | None:
        """The last result frame that carried structured output, if any."""
        with self._lock:
            for frame in reversed(self.results):
                if frame.get("structured_output") is not None:
                    return frame
        return None


class LaunchArgs:
    """An executor that starts the CLI with the turn's own arguments.

    agentshim opens a conversation before it knows the turn, so it starts one process for the conversation, stops
    it unused, and starts a second one with the turn's schema and arguments. SDO runs exactly one turn per
    conversation and knows them up front: they go on the process that opens it, which then serves the turn.
    """

    def __init__(self, live: CommandExecutor) -> None:
        self.live = live
        self.args: tuple[str, ...] = ()

    def find_binary(self, name: str, env: Mapping[str, str]) -> str:
        return self.live.find_binary(name, env)

    def check_binary(self, path: str, env: Mapping[str, str], *, timeout: float) -> None:
        self.live.check_binary(path, env, timeout=timeout)

    def spawn(self, request: SpawnRequest) -> Process:
        return self.live.spawn(replace(request, argv=[*request.argv, *self.args]))

    def run(self, request: CommandRequest, sink: CommandStreamSink) -> CommandResult:
        return self.live.run(request, sink)


def structured_answer(frame: Mapping[str, Any] | None, text: str) -> object | None:
    """The schema's answer: the result frame's payload, or the JSON text of a CLI that has no such field."""
    if frame is not None:
        return frame["structured_output"]
    try:
        return json.loads(text)
    except ValueError:
        return None


class ClaudeStreamSession:
    """One Claude conversation: open (or resume), one turn, wait out the agent's own tasks, close."""

    def __init__(
        self,
        launch: LaunchArgs,
        transport: ClaudeStreamTransport,
        *,
        cwd: str,
        model: str | None,
        resume_id: str | None,
        timeout: float | None,
        on_event: Callable[[AgentEvent], None],
        on_stdout: Callable[[str], None] | None = None,
    ) -> None:
        self._launch = launch
        self._transport = transport
        self._cwd = cwd
        self._model = model
        self._resume_id = resume_id
        self._timeout = timeout
        self._on_event = on_event
        self._on_stdout = on_stdout
        self._conversation: Conversation | None = None
        self._cancelled = threading.Event()
        self.watch = SessionWatch()

    def _raw(self, frame: RawFrame) -> None:
        if frame.direction is not FrameDirection.FROM_PROVIDER:
            return
        self.watch.observe(frame.text)
        if self._on_stdout is not None:
            self._on_stdout(frame.text)

    def cancel(self, grace_s: float = 5.0) -> None:
        """Stop the running turn and the wait for background tasks; the conversation survives."""
        del grace_s
        self._cancelled.set()
        conversation = self._conversation
        if conversation is not None:
            conversation.interrupt()

    def close(self) -> None:
        """End the process. Idempotent."""
        conversation, self._conversation = self._conversation, None
        if conversation is not None:
            conversation.close()

    def turn(self, request: TurnRequest) -> TurnResult:
        """Run *request*; return the answer that stands once the agent's background tasks are done."""
        started = time.monotonic()
        schema = request.output_schema
        if schema is not None:
            self._launch.args = ("--json-schema", json.dumps(schema.schema, separators=(",", ":")))
        self._launch.args += tuple(request.extra_args)
        spec = ConversationSpec(
            cwd=self._cwd,
            model=self._model,
            permissions=NativePermissions(NativeMode.BYPASS),
            approvals=ApprovalPolicy.DENY,
            mcp_servers=tuple(request.mcp_servers),
            reasoning_effort=request.reasoning_effort,
            resume_id=self._resume_id,
            raw_sink=self._raw,
        )
        self._conversation = self._transport.open(spec)
        result = self._conversation.turn(
            replace(request, timeout=self._timeout, output_schema=None, extra_args=()), self._on_event
        )
        if result.interrupted or self._cancelled.is_set():
            raise TurnCancelledError("the turn was interrupted")
        if self.watch.working:
            self._drain(started)
        final = self.watch.final_result()
        text = final.get("result") if final is not None else None
        result = replace(result, text=text if isinstance(text, str) else result.text)
        return result if schema is None else replace(result, structured_output=structured_answer(final, result.text))

    def _drain(self, started: float) -> None:
        """Read the process until the CLI has nothing left to say about the agent's tasks."""
        conversation: IdleConversation = self._conversation  # type: ignore[assignment]
        deadline = None if self._timeout is None else started + self._timeout
        while not self._cancelled.is_set():
            alive = conversation.poll_idle(self._on_event)
            if not self.watch.working or not alive:
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise CliTimeoutError([], self._timeout or 0.0)
            time.sleep(POLL_SECONDS)
        raise TurnCancelledError("the turn was interrupted while background tasks ran")


def stream_transport(executor: LaunchArgs, env: Mapping[str, str], *, check_timeout: float) -> ClaudeStreamTransport:
    """The transport for a host or remote ``claude``."""
    return ClaudeStreamTransport(executor=executor, env=env, check_timeout=check_timeout)


def settings_args(settings: Mapping[str, object], extra: Sequence[str]) -> tuple[str, ...]:
    """The CLI arguments that carry SDO's sandbox settings, then *extra*."""
    return (*(("--settings", json.dumps(settings)) if settings else ()), *extra)


__all__ = [
    "HEADLESS_DISALLOWED_TOOLS",
    "LaunchArgs",
    "SessionWatch",
    "disallowed_tools_args",
    "ClaudeStreamSession",
    "TurnTimeoutError",
    "settings_args",
    "stream_transport",
]
