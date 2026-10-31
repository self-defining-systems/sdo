"""Codex CLI wire formats: the ``--json`` event stream and the session rollout file.

The scripted CLI speaks exactly what ``codex exec --json`` speaks, so agentshim
parses it with its real Codex parser, and it writes the session rollout that
SDO's usage accounting (``libs.agent_cli.structured``) and the benchmark cost
analysis read. Two behaviours of the real CLI are reproduced on purpose,
because SDO's accounting depends on them:

- ``turn.completed`` reports the whole session's cumulative usage, so a
  resumed turn reports its predecessors' tokens too;
- the rollout gets one ``token_count`` event per model response, carrying the
  response's own ``last_token_usage`` and the session's running
  ``total_token_usage``.

Stdlib-only: this runs inside the controller and responder images.
"""

from __future__ import annotations

import json
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import IO, TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

CLI_VERSION = "0.0.0-scripted"
TOKEN_FIELDS = ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens")


class ResumeError(RuntimeError):
    """A resumed session has no rollout; the real CLI fails the same way."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class RequestUsage:
    """One model response's usage, in Codex's raw shape (input includes cache reads, output reasoning)."""

    input_tokens: int
    cached_input_tokens: int
    output_tokens: int
    reasoning_output_tokens: int

    def __post_init__(self) -> None:
        if min(self.input_tokens, self.cached_input_tokens, self.output_tokens, self.reasoning_output_tokens) < 0:
            raise ValueError("token counts must not be negative")
        if self.cached_input_tokens > self.input_tokens:
            raise ValueError("cached input cannot exceed input")
        if self.reasoning_output_tokens > self.output_tokens:
            raise ValueError("reasoning output cannot exceed output")

    def as_dict(self) -> dict[str, int]:
        payload = {name: int(getattr(self, name)) for name in TOKEN_FIELDS}
        payload["total_tokens"] = self.input_tokens + self.output_tokens
        return payload


def add_usage(left: dict[str, int], right: RequestUsage) -> dict[str, int]:
    total = {name: int(left.get(name, 0)) + int(getattr(right, name)) for name in TOKEN_FIELDS}
    total["total_tokens"] = total["input_tokens"] + total["output_tokens"]
    return total


@dataclass
class Rollout:
    """A Codex session rollout under ``$CODEX_HOME/sessions``."""

    path: Path
    session_id: str
    totals: dict[str, int] = field(default_factory=dict)
    requests: int = 0

    @classmethod
    def create(cls, home: Path, *, cwd: str, session_id: str | None = None) -> Rollout:
        session = session_id or str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        directory = home / "sessions" / f"{now:%Y}" / f"{now:%m}" / f"{now:%d}"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"rollout-{now:%Y-%m-%dT%H-%M-%S}-{session}.jsonl"
        rollout = cls(path=path, session_id=session)
        rollout._write(
            "session_meta",
            {
                "id": session,
                "timestamp": _now(),
                "cwd": cwd,
                "originator": "codex_exec",
                "cli_version": CLI_VERSION,
                "instructions": None,
            },
        )
        return rollout

    @classmethod
    def resume(cls, home: Path, session_id: str) -> Rollout:
        sessions = home / "sessions"
        paths = sorted(sessions.rglob(f"rollout-*{session_id}.jsonl")) if sessions.is_dir() else []
        if not paths:
            raise ResumeError(f"thread/resume failed: no rollout found for thread {session_id}")
        rollout = cls(path=paths[-1], session_id=session_id)
        for line in paths[-1].read_text(encoding="utf-8").splitlines():
            record = json.loads(line)
            payload = record.get("payload") if isinstance(record, dict) else None
            if isinstance(payload, dict) and payload.get("type") == "token_count":
                info = payload.get("info")
                if isinstance(info, dict) and isinstance(info.get("total_token_usage"), dict):
                    rollout.totals = {key: int(value) for key, value in info["total_token_usage"].items()}
                    rollout.requests += 1
        return rollout

    def _write(self, kind: str, payload: dict[str, object]) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"timestamp": _now(), "type": kind, "payload": payload}) + "\n")

    def task_started(self) -> None:
        self._write("event_msg", {"type": "task_started", "model_context_window": 272000})

    def user_message(self, text: str) -> None:
        self._write(
            "response_item", {"type": "message", "role": "user", "content": [{"type": "input_text", "text": text}]}
        )

    def function_call(self, call_id: str, command: str) -> None:
        self._write(
            "response_item",
            {
                "type": "function_call",
                "name": "exec_command",
                "arguments": json.dumps({"cmd": command}),
                "call_id": call_id,
            },
        )

    def function_call_output(self, call_id: str, output: str, exit_code: int) -> None:
        self._write(
            "response_item",
            {"type": "function_call_output", "call_id": call_id, "output": f"Exit code: {exit_code}\n{output}"},
        )

    def token_count(self, usage: RequestUsage) -> None:
        self.totals = add_usage(self.totals, usage)
        self.requests += 1
        self._write(
            "event_msg",
            {
                "type": "token_count",
                "info": {
                    "total_token_usage": dict(self.totals),
                    "last_token_usage": usage.as_dict(),
                    "model_context_window": 272000,
                },
                "rate_limits": None,
            },
        )

    def assistant_message(self, text: str) -> None:
        self._write(
            "response_item",
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": text}]},
        )


class EventStream:
    """``codex exec --json`` events on stdout."""

    def __init__(self, out: IO[str] | None = None) -> None:
        self._out = out or sys.stdout
        self._items = 0

    def _emit(self, payload: dict[str, object]) -> None:
        self._out.write(json.dumps(payload) + "\n")
        self._out.flush()

    def thread_started(self, session_id: str) -> None:
        self._emit({"type": "thread.started", "thread_id": session_id})

    def turn_started(self) -> None:
        self._emit({"type": "turn.started"})

    def command_started(self, command: str) -> str:
        item_id = f"item_{self._items}"
        self._items += 1
        self._emit(
            {
                "type": "item.started",
                "item": {"id": item_id, "type": "command_execution", "command": command, "status": "in_progress"},
            }
        )
        return item_id

    def command_completed(self, item_id: str, command: str, output: str, exit_code: int) -> None:
        self._emit(
            {
                "type": "item.completed",
                "item": {
                    "id": item_id,
                    "type": "command_execution",
                    "command": command,
                    "aggregated_output": output,
                    "exit_code": exit_code,
                    "status": "completed" if exit_code == 0 else "failed",
                },
            }
        )

    def agent_message(self, text: str) -> None:
        item_id = f"item_{self._items}"
        self._items += 1
        self._emit({"type": "item.completed", "item": {"id": item_id, "type": "agent_message", "text": text}})

    def turn_completed(self, session_totals: dict[str, int]) -> None:
        usage = {name: int(session_totals.get(name, 0)) for name in TOKEN_FIELDS}
        usage["cache_write_input_tokens"] = 0
        self._emit({"type": "turn.completed", "usage": usage})
