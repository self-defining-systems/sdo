"""Codex CLI I/O recording at the agentshim command executor boundary."""

from __future__ import annotations

import hashlib
import json
import os
from typing import TYPE_CHECKING

from agentshim import CommandResult, HostCommandExecutor

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from agentshim import CommandExecutor, CommandHandle, CommandRequest, CommandStreamSink

MAX_MODEL_IO_BYTES = 4 << 20


class ModelCacheMiss(RuntimeError):
    """A model turn was not recorded for the request and prompt digests."""


class RecordingExecutor:
    def __init__(
        self,
        directory: Path,
        *,
        request_json: str,
        live: CommandExecutor | None = None,
        record: bool = False,
        live_on_miss: bool = False,
        prompt_replacements: Mapping[str, str] | None = None,
        ordered: bool = False,
    ) -> None:
        self.directory = directory
        self.request_digest = hashlib.sha256(request_json.encode()).hexdigest()
        self.live = live or (HostCommandExecutor() if record or live_on_miss else None)
        self.record = record
        self.live_on_miss = live_on_miss
        self.prompt_replacements = prompt_replacements or {}
        self.occurrences: dict[str, int] = {}
        self.ordered = ordered
        self.turn_index = 0
        self.prompt_changes: list[dict[str, str]] = []

    def find_binary(self, name: str, env: Mapping[str, str]) -> str:
        if self.live is None:
            return name
        return self.live.find_binary(name, env)

    def check_binary(self, path: str, env: Mapping[str, str], *, timeout: float) -> None:
        if self.live is not None and (self.record or self.live_on_miss):
            self.live.check_binary(path, env, timeout=timeout)

    def run(self, request: CommandRequest, sink: CommandStreamSink) -> CommandResult:
        prompt = request.stdin if request.stdin is not None else request.argv[-1]
        for current, recorded in sorted(self.prompt_replacements.items(), key=lambda item: len(item[0]), reverse=True):
            prompt = prompt.replace(current, recorded)
        prompt_digest = hashlib.sha256(prompt.encode()).hexdigest()
        turn_index = self.turn_index
        self.turn_index += 1
        occurrence = self.occurrences.get(prompt_digest, 0)
        self.occurrences[prompt_digest] = occurrence + 1
        suffix = f"-{occurrence:03d}" if occurrence else ""
        path = self.directory / self.request_digest / f"{prompt_digest}{suffix}.json"
        if self.ordered and not self.record:
            turns = {}
            for candidate in path.parent.glob("*.json"):
                payload = json.loads(candidate.read_text(encoding="utf-8"))
                if "turn_index" in payload:
                    turns[payload["turn_index"]] = candidate
            if turns:
                if turn_index in turns:
                    path = turns[turn_index]
                elif self.live_on_miss:
                    path = path.with_name(f"{prompt_digest}-turn-{turn_index:03d}.json")
                else:
                    raise ModelCacheMiss(
                        f"model cache exhausted at turn {turn_index} for request {self.request_digest} "
                        f"prompt {prompt_digest}"
                    )
        if not self.record and path.is_file():
            payload = json.loads(path.read_text(encoding="utf-8"))
            changed = payload.get("prompt_digest") != prompt_digest
            if payload.get("request_digest") != self.request_digest or changed and not self.ordered:
                raise ModelCacheMiss(
                    f"model cache digest mismatch for request {self.request_digest} prompt {prompt_digest}"
                )
            if changed:
                self.prompt_changes.append({"recorded": payload["prompt_digest"], "current": prompt_digest})
            sink.started(_ReplayHandle())
            for stream, line in payload["events"]:
                if stream == "stdout":
                    sink.stdout(line)
                else:
                    sink.stderr(line)
            return CommandResult(payload["returncode"], payload["stdout"], payload["stderr"])
        if self.live is None or not (self.record or self.live_on_miss):
            raise ModelCacheMiss(f"model cache miss for request {self.request_digest} prompt {prompt_digest}")
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
        payload = {
            "turn_index": turn_index,
            "request_digest": self.request_digest,
            "prompt_digest": prompt_digest,
            "events": events,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
        wire = json.dumps(payload, separators=(",", ":")).encode()
        if len(wire) > MAX_MODEL_IO_BYTES:
            raise ValueError(f"model I/O exceeds {MAX_MODEL_IO_BYTES} bytes")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = path.with_suffix(".tmp")
        with os.fdopen(os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600), "wb") as stream:
            stream.write(wire)
        temporary.replace(path)
        return result


class _ReplayHandle:
    def terminate(self) -> None:
        pass

    def kill(self) -> None:
        pass
