"""One scripted Codex turn: run real shell commands and account every model response.

Each command stands for one model response that decided to run it, and the
final answer for one more, as in a real Codex turn. Every response writes a
``token_count`` event with deterministic synthetic usage.

Stdlib-only: this runs inside the controller and responder images.
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from benchmarks.sregym.agent_script.wire import TOKEN_FIELDS, EventStream, RequestUsage, Rollout

from .usage import request_usage

if TYPE_CHECKING:
    from pathlib import Path

MAX_OUTPUT_CHARS = 20_000


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class CommandRecord:
    command: str
    exit_code: int
    seconds: float


@dataclass
class Turn:
    kind: str
    seed: int
    cwd: Path
    rollout: Rollout
    stream: EventStream
    commands: list[CommandRecord] = field(default_factory=list)
    usage: dict[str, int] = field(default_factory=dict)
    requests: int = 0
    started_at: str = field(default_factory=utc_now)

    def _model_response(self) -> RequestUsage:
        usage = request_usage(self.seed, self.kind, self.requests)
        self.requests += 1
        for name in TOKEN_FIELDS:
            self.usage[name] = self.usage.get(name, 0) + int(getattr(usage, name))
        self.rollout.token_count(usage)
        return usage

    def run(self, command: str, *, timeout: float = 300.0) -> tuple[int, str]:
        """Run ``command`` with ``/bin/sh -c`` in the turn's working directory."""

        self._model_response()
        call_id = f"call_{self.requests}"
        self.rollout.function_call(call_id, command)
        item = self.stream.command_started(command)
        started = time.monotonic()
        try:
            completed = subprocess.run(
                ["/bin/sh", "-c", command],
                cwd=self.cwd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            code, output = completed.returncode, (completed.stdout + completed.stderr)
        except subprocess.TimeoutExpired as exc:
            code = 124
            output = f"{exc.stdout or ''}{exc.stderr or ''}\ncommand timed out after {timeout:.0f}s"
        if len(output) > MAX_OUTPUT_CHARS:
            output = output[:MAX_OUTPUT_CHARS] + "\n[... output truncated ...]"
        self.commands.append(CommandRecord(command=command, exit_code=code, seconds=time.monotonic() - started))
        self.rollout.function_call_output(call_id, output, code)
        self.stream.command_completed(item, command, output, code)
        return code, output

    def finish(self, text: str) -> None:
        """The final model response carrying the answer."""

        self._model_response()
        self.rollout.assistant_message(text)
        self.stream.agent_message(text)
        self.stream.turn_completed(self.rollout.totals)

    def record(self, **extra: object) -> dict[str, object]:
        return {
            "kind": self.kind,
            "session_id": self.rollout.session_id,
            "requests": self.requests,
            "usage": dict(self.usage),
            "session_totals": dict(self.rollout.totals),
            "commands": [
                {"command": item.command, "exit_code": item.exit_code, "seconds": round(item.seconds, 3)}
                for item in self.commands
            ],
            "started_at": self.started_at,
            "finished_at": utc_now(),
            **extra,
        }
