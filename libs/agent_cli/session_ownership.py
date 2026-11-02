"""Persisted, exclusive ownership of a provider's session writer.

The stable flock file serializes processes sharing a session volume. The JSON
record survives process death: a dropped OS lock alone never proves that the
provider writer closed. Executor process reaping or an exact Kubernetes worker termination observation
releases ownership. Waiting is bounded and never invokes the provider to probe/retry.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import tempfile
import time
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Callable

HANDOFF_TIMEOUT_SECONDS = 30.0


class SessionHandoffError(RuntimeError):
    """Persisted session ownership cannot authorize a resume."""


class SessionHandoffTimeout(SessionHandoffError):
    """The previous provider writer did not confirm release within the bound."""


@dataclass(frozen=True)
class WorkerIdentity:
    namespace: str
    pod_name: str
    pod_uid: str
    container: str

    def __post_init__(self) -> None:
        for value in (self.namespace, self.pod_name, self.pod_uid, self.container):
            if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,252}", value):
                raise ValueError("invalid worker Pod/container identity")


@dataclass(frozen=True)
class WorkerTermination:
    worker: WorkerIdentity
    kind: str
    reason: str
    observed_at: str
    finished_at: str = ""
    exit_code: int | None = None
    resource_version: str = ""

    def __post_init__(self) -> None:
        if type(self.worker) is not WorkerIdentity:
            raise TypeError("termination confirmation requires a worker identity")
        if type(self.finished_at) is not str or type(self.resource_version) is not str:
            raise TypeError("termination timestamps and resource version must be strings")
        if self.exit_code is not None and type(self.exit_code) is not int:
            raise TypeError("termination exit code must be an integer")
        if self.kind not in {"container_terminated", "pod_terminated", "pod_gone"}:
            raise ValueError("invalid worker termination observation")
        if type(self.reason) is not str or not self.reason or type(self.observed_at) is not str or not self.observed_at:
            raise ValueError("termination confirmation requires reason and observation time")


@dataclass(frozen=True)
class _OwnershipRecord:
    version: int
    provider: str
    session_id: str
    state: str
    role: str
    worker: WorkerIdentity | None = None
    termination_confirmation: WorkerTermination | None = None


class SessionOwnership:
    def __init__(
        self,
        directory: Path,
        provider: str,
        *,
        timeout_seconds: float,
        role: str = "responder",
        worker: WorkerIdentity | None = None,
        termination_observer: Callable[[WorkerIdentity, float], WorkerTermination | None] | None = None,
    ) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("session handoff timeout must be finite and positive")
        if role not in {"responder", "reflection"}:
            raise ValueError("session owner role must be responder or reflection")
        self.worker = worker
        self.termination_observer = termination_observer
        self._next_observation = 0.0
        self._termination_confirmation: WorkerTermination | None = None
        self.role = role
        self.directory = directory
        self.provider = provider
        self.deadline = time.monotonic() + min(HANDOFF_TIMEOUT_SECONDS, timeout_seconds)
        self.session_id: str | None = None
        self._lock: IO[str] | None = None
        self._record_path: Path | None = None

    def claim(self, session_id: str, *, resume: bool) -> None:
        if not session_id:
            raise SessionHandoffError("provider reported an empty session ID")
        if self.session_id is not None:
            if self.session_id != session_id:
                raise SessionHandoffError("provider changed the owned session ID")
            return
        key = hashlib.sha256(f"{self.provider}:{session_id}".encode()).hexdigest()
        self.directory.mkdir(parents=True, exist_ok=True)
        record_path = self.directory / f"{key}.json"
        lock = (self.directory / f"{key}.lock").open("a+", encoding="utf-8")
        prior_confirmation: WorkerTermination | None = None
        try:
            while True:
                try:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    self._wait(session_id)
                    continue
                if resume:
                    record = self._read(record_path, session_id)
                    if self.role == "responder" and record.role == "reflection":
                        raise SessionHandoffError(f"session {session_id!r} ownership already transferred to reflection")
                    if (
                        record.state == "active"
                        and record.worker is not None
                        and self.termination_observer is not None
                        and time.monotonic() >= self._next_observation
                    ):
                        remaining = self.deadline - time.monotonic()
                        if remaining <= 0:
                            self._wait(session_id)
                        confirmation = self.termination_observer(record.worker, remaining)
                        if time.monotonic() >= self.deadline:
                            self._wait(session_id)
                        self._next_observation = time.monotonic() + 0.5
                        if confirmation is not None:
                            if confirmation.worker != record.worker:
                                raise SessionHandoffError("termination observation names another writer")
                            record = replace(record, state="released", termination_confirmation=confirmation)
                            self._write(record_path, record)
                    prior_confirmation = record.termination_confirmation
                    if record.state != "released":
                        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                        self._wait(session_id)
                        continue
                elif record_path.exists():
                    raise SessionHandoffError(f"new provider session {session_id!r} already has an ownership record")
                self._write(
                    record_path,
                    _OwnershipRecord(
                        1, self.provider, session_id, "active", self.role, self.worker, prior_confirmation
                    ),
                )
                self._termination_confirmation = prior_confirmation
                self._lock, self._record_path, self.session_id = lock, record_path, session_id
                return
        except BaseException:
            lock.close()
            raise

    def _wait(self, session_id: str) -> None:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise SessionHandoffTimeout(
                f"session {session_id!r} writer did not confirm release before handoff deadline"
            )
        time.sleep(min(0.025, remaining))

    def _read(self, path: Path, session_id: str) -> _OwnershipRecord:
        try:
            raw: object = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("ownership record must be an object")
            document = cast("dict[str, Any]", raw)
            if document.get("worker") is not None:
                document["worker"] = WorkerIdentity(**document["worker"])
            if document.get("termination_confirmation") is not None:
                evidence = cast("dict[str, Any]", document["termination_confirmation"])
                evidence["worker"] = WorkerIdentity(**evidence["worker"])
                document["termination_confirmation"] = WorkerTermination(**evidence)
            record = _OwnershipRecord(**document)
            if (
                type(record.version) is not int
                or record.version != 1
                or record.provider != self.provider
                or record.session_id != session_id
                or record.state not in {"active", "released"}
                or record.role not in {"responder", "reflection"}
            ):
                raise ValueError("invalid ownership identity or state")
            return record
        except (OSError, TypeError, ValueError, KeyError) as exc:
            raise SessionHandoffError(f"session {session_id!r} has no valid persisted writer ownership: {exc}") from exc

    def _write(self, path: Path, record: _OwnershipRecord) -> None:
        fd, temporary = tempfile.mkstemp(prefix=".ownership-", dir=self.directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(asdict(record), stream, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            directory_fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def close(self, *, writer_closed: bool) -> None:
        if self._lock is None:
            return
        try:
            if writer_closed:
                assert self._record_path is not None
                assert self.session_id is not None
                self._write(
                    self._record_path,
                    _OwnershipRecord(
                        1,
                        self.provider,
                        self.session_id,
                        "released",
                        self.role,
                        self.worker,
                        self._termination_confirmation,
                    ),
                )
        finally:
            self._lock.close()
            self._lock = None
