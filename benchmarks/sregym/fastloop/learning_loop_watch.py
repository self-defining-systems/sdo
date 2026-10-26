"""Record real validator Job watch events before the broker deletes its Jobs."""

from __future__ import annotations

import json
import subprocess
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path


class ValidatorWatch:
    def __init__(self, kubeconfig: Path, namespace: str, path: Path) -> None:
        self.path = path
        self.command = [
            "kubectl",
            "--kubeconfig",
            str(kubeconfig),
            "-n",
            namespace,
            "get",
            "jobs",
            "--watch",
            "--output-watch-events",
            "-o",
            "json",
        ]
        self.errors: list[str] = []
        self.lock = threading.Lock()
        self.process: subprocess.Popen[str] | None = None
        self.thread: threading.Thread | None = None

    def __enter__(self) -> ValidatorWatch:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.process = subprocess.Popen(self.command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.thread = threading.Thread(target=self._record, daemon=True)
        self.thread.start()
        return self

    def _record(self) -> None:
        assert self.process is not None
        assert self.process.stdout is not None
        buffer = ""
        try:
            with self.path.open("w") as destination:
                for line in self.process.stdout:
                    buffer += line
                    try:
                        event = json.loads(buffer)
                    except json.JSONDecodeError:
                        continue
                    with self.lock:
                        destination.write(json.dumps(event) + "\n")
                        destination.flush()
                    buffer = ""
        except (OSError, ValueError) as exc:
            self.errors.append(str(exc))

    def jobs(self) -> dict[str, Any]:
        if self.errors or self.process is None or self.process.poll() is not None:
            raise RuntimeError(f"learning-loop: validator watch unavailable: {self.errors}")
        latest = {}
        with self.lock:
            lines = self.path.read_text().splitlines()
        for line in lines:
            event = json.loads(line)
            job = event.get("object") or {}
            if job.get("kind") == "Job":
                latest[job["metadata"]["uid"]] = job
        return {"items": list(latest.values())}

    def __exit__(self, *_args: object) -> None:
        assert self.process is not None
        assert self.thread is not None
        self.process.terminate()
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=3)
        self.thread.join(timeout=3)
        if self.process.stdout:
            self.process.stdout.close()
        if self.process.stderr:
            self.process.stderr.close()
