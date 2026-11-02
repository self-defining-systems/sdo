"""Opt-in startup timestamps; only timing fields survive the local OTLP edge."""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from agentshim import HostCommandExecutor

if TYPE_CHECKING:
    from agentshim import CommandExecutor, CommandHandle, CommandRequest, CommandResult, CommandStreamSink

_LOCK = threading.Lock()


def mark_startup(name: str, timestamp: str | None = None, **fields: str) -> None:
    target = os.environ.get("SDO_STARTUP_TIMING_PATH")
    if not target:
        return
    with _LOCK:
        path = Path(target)
        try:
            record: dict[str, str] = cast("dict[str, str]", json.loads(path.read_text())) if path.exists() else {}
            record.setdefault(name, timestamp or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
            record.update(fields)
            temporary = path.with_suffix(".tmp")
            with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as stream:
                json.dump(record, stream)
            temporary.replace(path)
        except (OSError, ValueError):
            # Observability must not stop incident response.
            return


class StartupTelemetry:
    """Receive Codex's own HTTP/WebSocket request timing on loopback.

    Request-start is reconstructed from the provider's event timestamp and
    duration, never from a process spawn or first response timestamp. Bodies,
    prompts, credentials and tool arguments are discarded without persistence.
    """

    def __init__(self, provider: str = "codex") -> None:
        self.provider = provider

    endpoint = ""
    server: ThreadingHTTPServer | None = None
    thread: threading.Thread | None = None

    def __enter__(self) -> StartupTelemetry:
        if not os.environ.get("SDO_STARTUP_TIMING_PATH"):
            return self
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 0 or length > 4 << 20:
                    self.send_error(413)
                    return
                try:
                    owner.consume(json.loads(self.rfile.read(length)))
                except (ValueError, TypeError, KeyError):
                    self.send_error(400)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, format: str, *args: object) -> None:  # noqa: A002
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}/v1/logs"
        self.thread = threading.Thread(
            target=lambda: self.server and self.server.serve_forever(poll_interval=0.02), daemon=True
        )
        self.thread.start()
        return self

    def consume(self, payload: dict[str, Any]) -> None:
        for resource in payload.get("resourceLogs", []):
            for scope in resource.get("scopeLogs", []):
                for record in scope.get("logRecords", []):
                    attributes = {
                        item["key"]: next(iter(item["value"].values()), None) for item in record.get("attributes", [])
                    }
                    event = attributes.get("event.name") or record.get("body", {}).get("stringValue")
                    if event not in {"codex.api_request", "codex.websocket_request"}:
                        continue
                    timestamp = int(record["timeUnixNano"]) / 1e9 - float(str(attributes["duration_ms"])) / 1000
                    mark_startup(
                        "first_model_request_sent",
                        datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace("+00:00", "Z"),
                        first_model_request_source=f"codex.otel.{event.removeprefix('codex.')}.timestamp-minus-duration",
                    )

    def process_started(self) -> None:
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        mark_startup("agent_process_started", timestamp)
        mark_startup(f"{self.provider}_process_started", timestamp)

    def executor(self, live: CommandExecutor | None) -> CommandExecutor:
        return _TimingExecutor(live or HostCommandExecutor(), self)

    def codex_args(self) -> list[str]:
        if not self.endpoint:
            return []
        return [
            "-c",
            f'otel.exporter={{otlp-http={{endpoint="{self.endpoint}",protocol="json"}}}}',
            "-c",
            "otel.log_user_prompt=false",
        ]

    def __exit__(self, *args: object) -> None:
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
        if self.thread is not None:
            self.thread.join(timeout=1)


class _TimingExecutor:
    def __init__(self, live: CommandExecutor, telemetry: StartupTelemetry) -> None:
        self.live = live
        self.telemetry = telemetry

    def find_binary(self, name: str, env: Any) -> str:
        return self.live.find_binary(name, env)

    def check_binary(self, path: str, env: Any, *, timeout: float) -> None:
        self.live.check_binary(path, env, timeout=timeout)

    def run(self, request: CommandRequest, sink: CommandStreamSink) -> CommandResult:
        owner = self

        class Sink:
            def started(self, handle: CommandHandle) -> None:
                owner.telemetry.process_started()
                sink.started(handle)

            def stdout(self, line: str) -> None:
                sink.stdout(line)

            def stderr(self, line: str) -> None:
                sink.stderr(line)

        return self.live.run(request, Sink())
