"""One Claude Code conversation over a long-lived ``stream-json`` process.

SDO's agents are one-shot requests: they decide for themselves when they are done. A
responder that starts background subagents and has nothing else to do must end its turn, and a
required output schema makes that turn end with a structured result. Claude Code then starts a
turn of its own when a background task finishes, and that later turn holds the answer that
counts. A request that is read only up to its first result would hand the early result back
while the agent's own tasks still run.

``ClaudeStreamSession`` reads on: after the turn ends it keeps reading the process while the
CLI reports background tasks, and returns the last result once the tasks are gone and the CLI
has gone quiet. SDO injects no turn and no context; it only does not stop reading.
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

    from agentshim import CommandExecutor, IdleConversation

#: How long the CLI must stay quiet, with no background task left, before the last result is final.
#: A finished task makes the CLI start its follow-on turn within moments; this covers that gap.
SETTLE_SECONDS = 3.0

#: How often the drain looks at the process and the clock.
POLL_SECONDS = 0.2

#: System frames that carry no work: they neither start nor end a turn.
_BOOKKEEPING = frozenset(
    {"background_tasks_changed", "thinking_tokens", "commands_changed", "task_progress", "task_updated"}
)


@dataclass
class BackgroundWatch:
    """What the CLI's own frames say about its background tasks and results.

    Fed every raw frame of the process. ``settled`` is true when the agent has no background
    task, the last substantive frame was a result and nothing has been written for
    ``SETTLE_SECONDS``.
    """

    clock: Callable[[], float] = time.monotonic
    tasks: int = 0
    results: list[dict[str, Any]] = field(default_factory=lambda: cast("list[dict[str, Any]]", []))
    _idle_since: float | None = None
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
        kind, subtype = record.get("type"), record.get("subtype")
        with self._lock:
            if kind == "system" and subtype == "background_tasks_changed":
                tasks = record.get("tasks")
                self.tasks = len(cast("list[object]", tasks)) if isinstance(tasks, list) else 0
            if kind == "result":
                self.results.append(record)
                self._idle_since = self.clock()
            elif not (kind == "system" and subtype in _BOOKKEEPING) and kind != "rate_limit_event":
                self._idle_since = None

    @property
    def pending(self) -> bool:
        """The agent has a background task the CLI still reports."""
        with self._lock:
            return self.tasks > 0

    def settled(self, quiet_for: float | None = None) -> bool:
        """No background task, the last substantive frame was a result, and it is old enough."""
        quiet = SETTLE_SECONDS if quiet_for is None else quiet_for
        with self._lock:
            return self.tasks == 0 and self._idle_since is not None and self.clock() - self._idle_since >= quiet

    def final_result(self) -> dict[str, Any] | None:
        """The last result frame that carried structured output, if any."""
        with self._lock:
            for frame in reversed(self.results):
                if frame.get("structured_output") is not None:
                    return frame
        return None


class ClaudeStreamSession:
    """One Claude conversation: open (or resume), one turn, wait out the agent's own tasks, close."""

    def __init__(
        self,
        transport: ClaudeStreamTransport,
        *,
        cwd: str,
        model: str | None,
        resume_id: str | None,
        timeout: float | None,
        on_event: Callable[[AgentEvent], None],
        on_stdout: Callable[[str], None] | None = None,
        settle_seconds: float | None = None,
    ) -> None:
        self._transport = transport
        self._cwd = cwd
        self._model = model
        self._resume_id = resume_id
        self._timeout = timeout
        self._on_event = on_event
        self._on_stdout = on_stdout
        self._settle_seconds = settle_seconds
        self._conversation: Conversation | None = None
        self._cancelled = threading.Event()
        self.watch = BackgroundWatch()

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
        spec = ConversationSpec(
            cwd=self._cwd,
            model=self._model,
            permissions=NativePermissions(NativeMode.BYPASS),
            approvals=ApprovalPolicy.DENY,
            reasoning_effort=request.reasoning_effort,
            resume_id=self._resume_id,
            raw_sink=self._raw,
        )
        self._conversation = self._transport.open(spec)
        result = self._conversation.turn(replace(request, timeout=self._timeout), self._on_event)
        if result.interrupted or self._cancelled.is_set():
            raise TurnCancelledError("the turn was interrupted")
        if not self.watch.pending:
            return result
        self._drain(started)
        final = self.watch.final_result()
        if final is None:
            return result
        text = final.get("result")
        return replace(
            result,
            text=text if isinstance(text, str) else result.text,
            structured_output=final["structured_output"],
        )

    def _drain(self, started: float) -> None:
        """Read the process until the CLI has nothing left to say about the agent's tasks."""
        conversation: IdleConversation = self._conversation  # type: ignore[assignment]
        deadline = None if self._timeout is None else started + self._timeout
        while not self._cancelled.is_set():
            alive = conversation.poll_idle(self._on_event)
            if self.watch.settled(self._settle_seconds) or not alive:
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise CliTimeoutError([], self._timeout or 0.0)
            time.sleep(POLL_SECONDS)
        raise TurnCancelledError("the turn was interrupted while background tasks ran")


def stream_transport(
    executor: CommandExecutor, env: Mapping[str, str], *, check_timeout: float
) -> ClaudeStreamTransport:
    """The transport for a host or remote ``claude``."""
    return ClaudeStreamTransport(executor=executor, env=env, check_timeout=check_timeout)


def settings_args(settings: Mapping[str, object], extra: Sequence[str]) -> tuple[str, ...]:
    """The CLI arguments that carry SDO's sandbox settings, then *extra*."""
    return (*(("--settings", json.dumps(settings)) if settings else ()), *extra)


__all__ = [
    "BackgroundWatch",
    "ClaudeStreamSession",
    "TurnTimeoutError",
    "settings_args",
    "stream_transport",
]
