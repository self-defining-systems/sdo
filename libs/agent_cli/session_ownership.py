"""Persisted, exclusive ownership of a provider's session writer.

The stable flock file serializes processes sharing a session volume. The JSON
record survives process death: a dropped OS lock alone never proves that the
provider writer closed. Only the executor's terminal process evidence releases
ownership. Waiting is bounded and never invokes the provider to probe/retry.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import IO

HANDOFF_TIMEOUT_SECONDS = 30.0


class SessionHandoffError(RuntimeError):
    """Persisted session ownership cannot authorize a resume."""


class SessionHandoffTimeout(SessionHandoffError):
    """The previous provider writer did not confirm release within the bound."""


@dataclass(frozen=True)
class _OwnershipRecord:
    version: int
    provider: str
    session_id: str
    state: str
    role: str


class SessionOwnership:
    def __init__(self, directory: Path, provider: str, *, timeout_seconds: float, role: str = "responder") -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("session handoff timeout must be finite and positive")
        if role not in {"responder", "reflection"}:
            raise ValueError("session owner role must be responder or reflection")
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
                    if record.state != "released":
                        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                        self._wait(session_id)
                        continue
                elif record_path.exists():
                    raise SessionHandoffError(f"new provider session {session_id!r} already has an ownership record")
                self._write(record_path, _OwnershipRecord(1, self.provider, session_id, "active", self.role))
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
            record = _OwnershipRecord(**json.loads(path.read_text(encoding="utf-8")))
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
        except (OSError, TypeError, ValueError) as exc:
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
                    self._record_path, _OwnershipRecord(1, self.provider, self.session_id, "released", self.role)
                )
        finally:
            self._lock.close()
            self._lock = None
