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
<<<<<<< HEAD
=======
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
            if "unrecorded" in payload:
                raise ModelCacheMiss(f"model turn {payload.get('turn_index')} not recorded: {payload['unrecorded']}")
            changed = payload.get("prompt_digest") != prompt_digest
            if payload.get("request_digest") != self.request_digest or changed and not self.ordered:
                raise ModelCacheMiss(
                    f"model cache digest mismatch for request {self.request_digest} prompt {prompt_digest}"
                )
            if changed:
                self.prompt_changes.append({"recorded": payload["prompt_digest"], "current": prompt_digest})
            sink.started(_ReplayHandle())
            streams: dict[str, list[str]] = {"stdout": [], "stderr": []}
            for stream, line in payload["events"]:
                streams[stream].append(line)
                if stream == "stdout":
                    sink.stdout(line)
                else:
                    sink.stderr(line)
            return CommandResult(payload["returncode"], "".join(streams["stdout"]), "".join(streams["stderr"]))
        if self.live is None or not (self.record or self.live_on_miss):
            raise ModelCacheMiss(f"model cache miss for request {self.request_digest} prompt {prompt_digest}")
>>>>>>> origin/vic-lsh/dev/sdo-improve
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
