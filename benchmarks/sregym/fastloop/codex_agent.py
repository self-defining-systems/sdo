"""The raw Codex baseline as a fast-loop agent, for A/B comparisons with SDO.

It mirrors SREGym's ``clients/codex`` agent: the same instruction (built by the
benchmark client inside the SREGym worker), the same ``codex exec`` flags, the
same filtered Kubernetes API proxy, a ``CODEX_HOME`` holding only ``auth.json``,
and token usage taken from the last ``usage`` event. A local stub stands in for
the conductor's ``/status``, ``/submit`` and ``/get_app`` so the instruction's
submission protocol works unchanged; it records submissions with timestamps
and grades nothing.

Differences from the benchmark: Codex runs on the host (not in the
``sregym-agent-base`` container), with the host's Codex CLI version.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.loop import AgentOutcome, InjectionWindow
from benchmarks.sregym.fastloop.records import AgentName, TokenCounts

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

_STAGES = ("diagnosis", "mitigation", "done")


@dataclass(frozen=True)
class Submission:
    stage: str
    solution: str
    received_at: datetime


class SubmissionStub:
    """A grading-free stand-in for the conductor endpoints the Codex instruction uses."""

    def __init__(self, *, app_info: dict[str, Any], host: str = "127.0.0.1", port: int = 0) -> None:
        self._app_info = dict(app_info)
        self._lock = threading.Lock()
        self._stage_index = 0
        self._submissions: list[Submission] = []
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - stdlib signature
                return

            def _reply(self, status: int, payload: dict[str, Any]) -> None:
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:
                if self.path.rstrip("/") == "/status":
                    self._reply(200, {"stage": stub.stage})
                elif self.path.rstrip("/") == "/get_app":
                    self._reply(200, stub._app_info)
                else:
                    self._reply(404, {"detail": "not found"})

            def do_POST(self) -> None:
                if self.path.rstrip("/") != "/submit":
                    self._reply(404, {"detail": "not found"})
                    return
                length = int(self.headers.get("Content-Length") or 0)
                try:
                    solution = json.loads(self.rfile.read(length) or b"{}")["solution"]
                except (ValueError, KeyError, TypeError):
                    self._reply(400, {"detail": "expected JSON {'solution': ...}"})
                    return
                status, payload = stub._submit(solution)
                self._reply(status, payload)

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, name="codex-submission-stub", daemon=True)

    def __enter__(self) -> SubmissionStub:
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._server.shutdown()
        self._server.server_close()

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host!s}:{port}"

    @property
    def stage(self) -> str:
        with self._lock:
            return _STAGES[self._stage_index]

    def reset(self) -> None:
        with self._lock:
            self._stage_index = 0
            self._submissions = []

    def submissions(self) -> list[Submission]:
        with self._lock:
            return list(self._submissions)

    def _submit(self, solution: object) -> tuple[int, dict[str, Any]]:
        with self._lock:
            stage = _STAGES[self._stage_index]
            if stage == "done":
                return 200, {"status": "done", "message": "All stages have been completed."}
            text = solution if isinstance(solution, str) else json.dumps(solution)
            self._submissions.append(Submission(stage=stage, solution=text, received_at=datetime.now(timezone.utc)))
            self._stage_index += 1
            return 200, {"status": "200", "message": "Submission received"}


def codex_command(binary: str, *, model: str, reasoning_effort: str | None, prompt: str) -> list[str]:
    """``clients/codex/codex_agent.py``'s invocation."""

    command = [
        binary,
        "exec",
        "--dangerously-bypass-approvals-and-sandbox",
        "--skip-git-repo-check",
        "--model",
        model,
        "--json",
        "--enable",
        "unified_exec",
    ]
    if reasoning_effort:
        command.extend(["-c", f"model_reasoning_effort={reasoning_effort}"])
    command.extend(["--", prompt])
    return command


def codex_usage(stream: str) -> TokenCounts:
    """The last JSON event carrying ``usage``, as the benchmark client reads it."""

    usage: object = None
    for line in stream.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict) and isinstance(event.get("usage"), dict):
            usage = event["usage"]
    return TokenCounts.from_usage(usage)


@dataclass(frozen=True)
class CodexSettings:
    model: str
    #: Copied into a fresh ``CODEX_HOME`` for every incident, so the baseline never carries Codex memories.
    auth_file: Path
    kubeconfig: Path
    results_dir: Path
    timeout_seconds: float = wait_for(WaitKind.RESPONSE, 2)
    reasoning_effort: str | None = None
    codex_binary: str = "codex"

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not self.model:
            raise ValueError("model must not be empty")


class CodexBaselineAgent:
    name: AgentName = "codex"

    def __init__(
        self,
        settings: CodexSettings,
        *,
        stub: SubmissionStub,
        prompt_for: Callable[[str, str], str],
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._settings = settings
        self._stub = stub
        self._prompt_for = prompt_for
        self._now = now

    @property
    def model(self) -> str:
        return self._settings.model

    def resolve(self, index: int, problem_id: str, inject: Callable[[], InjectionWindow]) -> AgentOutcome:
        settings = self._settings
        incident_dir = settings.results_dir / f"{index:03d}_{problem_id}"
        incident_dir.mkdir(parents=True, exist_ok=True)
        # Codex sees its working directory and home; neither may name the injected problem.
        with tempfile.TemporaryDirectory(prefix="codex-incident-") as scratch:
            workdir = Path(scratch) / "workdir"
            workdir.mkdir()
            codex_home = Path(scratch) / "codex_home"
            codex_home.mkdir()
            shutil.copyfile(settings.auth_file, codex_home / "auth.json")
            return self._run(settings, index, incident_dir, problem_id, inject, workdir=workdir, codex_home=codex_home)

    def _run(
        self,
        settings: CodexSettings,
        index: int,
        incident_dir: Path,
        problem_id: str,
        inject: Callable[[], InjectionWindow],
        *,
        workdir: Path,
        codex_home: Path,
    ) -> AgentOutcome:
        self._stub.reset()
        prompt = self._prompt_for(problem_id, self._stub.url)
        (incident_dir / "prompt.txt").write_text(prompt, encoding="utf-8")
        env = {
            **os.environ,
            "KUBECONFIG": str(settings.kubeconfig),
            "CODEX_HOME": str(codex_home),
        }
        env.pop("OPENAI_API_KEY", None)
        command = codex_command(
            settings.codex_binary, model=settings.model, reasoning_effort=settings.reasoning_effort, prompt=prompt
        )
        window = inject()
        started = time.monotonic()
        log_path = incident_dir / "codex.jsonl"
        with log_path.open("w", encoding="utf-8") as log, (incident_dir / "codex.stderr").open("w") as errors:
            completed = subprocess.run(
                command,
                cwd=workdir,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=errors,
                timeout=settings.timeout_seconds,
                check=False,
            )
        finished = self._now()
        submissions = self._stub.submissions()
        diagnosis = next((item.solution for item in submissions if item.stage == "diagnosis"), "")
        mitigation = next((item for item in submissions if item.stage == "mitigation"), None)
        error = None
        if completed.returncode != 0:
            error = f"codex exited with code {completed.returncode}"
        elif mitigation is None:
            error = "codex exited without submitting a mitigation"
        return AgentOutcome(
            injection=window,
            mitigation_applied_at=mitigation.received_at if mitigation is not None else finished,
            resolved_at=finished,
            diagnosis=diagnosis,
            mitigation=mitigation.solution if mitigation is not None else "",
            responder_tokens=codex_usage(log_path.read_text(encoding="utf-8")),
            setup_seconds=None,
            artifacts_dir=str(incident_dir),
            incident_id=f"codex-{index:03d}-{int(started)}",
            error=error,
        )

    def learn(self, outcome: AgentOutcome) -> AgentOutcome:
        """The baseline is memoryless."""

        return outcome

    def prepare_recovery(self, outcome: AgentOutcome | None) -> None:
        """The baseline has no long-running incident intake."""

    def close(self) -> None:
        """The stub belongs to the caller."""
