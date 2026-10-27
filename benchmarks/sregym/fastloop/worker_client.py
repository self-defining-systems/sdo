"""Drive the SREGym fast-loop worker (``sregym.fastloop.worker``) as a JSON-lines subprocess.

SREGym runs in its own virtual environment, so first-party code talks to it
through a process boundary, exactly as the benchmark runner does. The worker
keeps the problem instance of the incident in flight between ``inject``,
``oracle`` and ``recover``.

SREGym fault injectors back up resources to fixed ``/tmp/<service>_*.yaml``
paths. Another SREGym run on the same host uses the same paths, so the worker
runs under ``bwrap`` with a private ``/tmp`` unless the caller opts out.
"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
from typing import IO, TYPE_CHECKING, Any

from benchmarks.sregym.adapter import Deadline, WaitKind, wait_for
from benchmarks.sregym.fastloop.host_limits import HostLimits

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType

DEFAULT_TIMEOUT_SECONDS = wait_for(WaitKind.RESPONSE)


class SregymWorkerError(RuntimeError):
    """Raised when the SREGym worker fails a request, times out, or exits."""


def worker_argv(sregym_dir: Path, *, private_tmp: Path | None) -> list[str]:
    command = ["uv", "run", "--directory", str(sregym_dir), "python", "-m", "benchmarks.sregym.fastloop.worker_bridge"]
    if private_tmp is None:
        return command
    return ["bwrap", "--dev-bind", "/", "/", "--bind", str(private_tmp), "/tmp", *command]


class SregymWorker:
    """One long-lived worker process; requests are served strictly in order."""

    def __init__(self, argv: list[str], *, env: dict[str, str], log_path: Path, cwd: Path | None = None) -> None:
        if not argv:
            raise ValueError("argv must not be empty")
        self._argv = argv
        self._env = env
        self._log_path = log_path
        self._cwd = cwd
        self._process: subprocess.Popen[str] | None = None
        self._log: IO[str] | None = None
        self._replies: queue.Queue[str | None] = queue.Queue()
        self._next_id = 0
        # A timed-out request leaves the worker busy; it is killed instead of asked to stop.
        self._abandoned = False

    def __enter__(self) -> SregymWorker:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def start(self) -> None:
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log = self._log_path.open("a", encoding="utf-8")
        self._process = subprocess.Popen(
            self._argv,
            cwd=self._cwd,
            env={**os.environ, **self._env},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._log,
            text=True,
            bufsize=1,
        )
        threading.Thread(target=self._read_replies, name="sregym-worker-replies", daemon=True).start()

    def _read_replies(self) -> None:
        assert self._process is not None
        assert self._process.stdout is not None
        for line in self._process.stdout:
            self._replies.put(line)
        self._replies.put(None)

    def request(self, op: str, *, timeout: float = DEFAULT_TIMEOUT_SECONDS, **args: Any) -> dict[str, Any]:
        if op == "deploy":
            with HostLimits.from_env(self._env).admit("redeploy"):
                return self._request(op, timeout=timeout, **args)
        return self._request(op, timeout=timeout, **args)

    def _request(self, op: str, *, timeout: float, **args: Any) -> dict[str, Any]:
        process = self._process
        if process is None or process.stdin is None:
            raise SregymWorkerError("worker is not running")
        self._next_id += 1
        request_id = self._next_id
        try:
            process.stdin.write(json.dumps({"id": request_id, "op": op, **args}) + "\n")
            process.stdin.flush()
        except BrokenPipeError as exc:
            raise SregymWorkerError(f"worker exited before {op!r} (see {self._log_path})") from exc
        budget = Deadline.after(timeout, f"worker request {op!r}")
        while True:
            try:
                if budget.left() <= 0:
                    raise queue.Empty
                line = self._replies.get(timeout=budget.left())
            except queue.Empty as exc:
                self._abandoned = True
                raise SregymWorkerError(f"worker request {op!r} timed out after {timeout:.0f}s") from exc
            if line is None:
                code = process.wait()
                raise SregymWorkerError(f"worker exited with code {code} during {op!r} (see {self._log_path})")
            reply = json.loads(line)
            if reply.get("id") != request_id:
                continue  # a late reply to a request that already timed out
            if not reply.get("ok"):
                raise SregymWorkerError(f"worker {op!r} failed: {reply.get('error')}")
            result = reply.get("result")
            return result if isinstance(result, dict) else {}

    def close(self) -> None:
        process = self._process
        if process is not None:
            if self._abandoned and process.poll() is None:
                process.kill()
            if process.poll() is None and process.stdin is not None:
                try:
                    process.stdin.write(json.dumps({"id": None, "op": "shutdown"}) + "\n")
                    process.stdin.close()
                except (BrokenPipeError, ValueError):
                    pass
                try:
                    process.wait(timeout=wait_for(WaitKind.COMMAND))
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            self._process = None
        if self._log is not None:
            self._log.close()
            self._log = None
