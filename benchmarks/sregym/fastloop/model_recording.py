"""Codex CLI I/O recording at the agentshim command executor boundary."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from typing import TYPE_CHECKING

from agentshim import CommandResult, HostCommandExecutor

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from agentshim import CommandExecutor, CommandHandle, CommandRequest, CommandStreamSink

# One turn's complete Codex JSONL, which carries every tool result the turn read.
MAX_MODEL_IO_BYTES = 64 << 20


class ModelRecorder:
    """Write-only live capture; replay is owned by agent scripts."""

    def __init__(self, directory: Path, *, request_json: str, live: CommandExecutor | None = None) -> None:
        self.directory = directory
        self.request_digest = hashlib.sha256(request_json.encode()).hexdigest()
        self.live = live or HostCommandExecutor()
        self.occurrences: dict[str, int] = {}
        self.turn_index = 0

    def find_binary(self, name: str, env: Mapping[str, str]) -> str:
        return self.live.find_binary(name, env)

    def check_binary(self, path: str, env: Mapping[str, str], *, timeout: float) -> None:
        self.live.check_binary(path, env, timeout=timeout)

    def run(self, request: CommandRequest, sink: CommandStreamSink) -> CommandResult:
        prompt = request.stdin if request.stdin is not None else request.argv[-1]
        prompt_digest = hashlib.sha256(prompt.encode()).hexdigest()
        turn_index = self.turn_index
        self.turn_index += 1
        occurrence = self.occurrences.get(prompt_digest, 0)
        self.occurrences[prompt_digest] = occurrence + 1
        suffix = f"-{occurrence:03d}" if occurrence else ""
        path = self.directory / self.request_digest / f"{prompt_digest}{suffix}.json"
        events: list[tuple[str, str]] = []

        class Tee:
            def started(self, handle: CommandHandle) -> None:
                sink.started(handle)

            def stdout(self, line: str) -> None:
                events.append(("stdout", line))
                sink.stdout(line)

            def stderr(self, line: str) -> None:
                events.append(("stderr", line))
                sink.stderr(line)

        result = self.live.run(request, Tee())
        # The streamed lines are the turn's whole output; replay joins them, so store them once.
        payload = {
            "turn_index": turn_index,
            "request_digest": self.request_digest,
            "prompt_digest": prompt_digest,
            "events": events,
            "returncode": result.returncode,
        }
        wire = json.dumps(payload, separators=(",", ":")).encode()
        if len(wire) > MAX_MODEL_IO_BYTES:
            # Benchmark evidence never discards the live turn; replay names the gap instead.
            del payload["events"], payload["returncode"]
            payload["unrecorded"] = f"model I/O exceeds {MAX_MODEL_IO_BYTES} bytes"
            wire = json.dumps(payload, separators=(",", ":")).encode()
        try:
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            temporary = path.with_suffix(".tmp")
            with os.fdopen(os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600), "wb") as stream:
                stream.write(wire)
            temporary.replace(path)
        except OSError as exc:
            print(f"model turn not recorded: {exc}", file=sys.stderr)
        return result
