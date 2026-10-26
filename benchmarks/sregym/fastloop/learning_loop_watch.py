"""Record real validator Job watch events before the broker deletes its Jobs."""

from __future__ import annotations

import json
import subprocess
import threading
from typing import TYPE_CHECKING, IO, Any

if TYPE_CHECKING:
    from pathlib import Path


class ValidatorWatch:
    def __init__(self, kubeconfig: Path, namespace: str, path: Path) -> None:
        self.path = path
        self.prefix = ["kubectl", "--kubeconfig", str(kubeconfig), "-n", namespace]
        self.logs: dict[str, tuple[subprocess.Popen[str], IO[str]]] = {}
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
                    self._capture_logs(event)
                    buffer = ""
        except (OSError, ValueError) as exc:
            self.errors.append(str(exc))

    def _capture_logs(self, event: dict[str, Any]) -> None:
        job = event.get("object") or {}
        containers = job.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
        if job.get("kind") != "Job" or not any(c.get("name") == "validator" for c in containers):
            return
        uid = job["metadata"]["uid"]
        if uid in self.logs or event.get("type") == "DELETED":
            return
        directory = self.path.parent / "validator-logs"
        directory.mkdir(exist_ok=True)
        destination = (directory / f"{uid}.log").open("w")
        try:
            process = subprocess.Popen(
                [
                    *self.prefix,
                    "logs",
                    "job/" + job["metadata"]["name"],
                    "--all-containers=true",
                    "--prefix=true",
                    "--follow",
                    "--pod-running-timeout=60s",
                ],
                stdout=destination,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except OSError:
            destination.close()
            raise
        self.logs[uid] = process, destination

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
        for process, destination in self.logs.values():
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
            destination.close()
        if self.process.stdout:
            self.process.stdout.close()
        if self.process.stderr:
            self.process.stderr.close()
