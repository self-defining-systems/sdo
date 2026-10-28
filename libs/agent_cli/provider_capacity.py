"""Bounded provider backoff and crash-releasing shared or static host Opus pools."""

from __future__ import annotations

import fcntl
import json
import math
import os
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
from uuid import uuid4

from libs.agent_cli.errors import ProviderRateLimited

DEFAULT_OPUS_SESSIONS = 8
DEFAULT_OPUS_HOST_SHARE = 4
MAX_PROVIDER_WAIT_SECONDS = 1800.0
OPUS_POOL_ENVIRONMENT = (
    "SDO_OPUS_POOL_DIR",
    "SDO_OPUS_MAX_SESSIONS",
    "SDO_OPUS_POOL_NFS_SERVER",
    "SDO_OPUS_POOL_NFS_PATH",
    "SDO_OPUS_POOL_MODE",
    "SDO_OPUS_POOL_HOST_ID",
    "SDO_OPUS_POOL_HOST_PATH",
)
_LIMIT = re.compile(
    r"rate_limit_error|overloaded_error|API Error:\s*(?:429|529)\b|"
    r"you['’]ve hit your limit|you have hit your limit|usage limit reached|"
    r"(?:rate limit|overloaded)\s*(?:reached|exceeded|error)",
    re.IGNORECASE,
)


def is_claude_rate_limit(text: str) -> bool:
    """Recognize Claude's CLI/API diagnostics, not arbitrary HTTP tool errors."""
    for line in text.splitlines():
        try:
            frame = json.loads(line)
        except json.JSONDecodeError:
            if _LIMIT.search(line):
                return True
            continue
        if not isinstance(frame, dict):
            continue
        frame = cast("dict[str, object]", frame)
        if (
            frame.get("type") == "result"
            and frame.get("is_error") is True
            and (_LIMIT.search(str(frame.get("result", ""))) or _LIMIT.search(str(frame.get("errors", ""))))
        ):
            return True
        if frame.get("type") == "error" and _LIMIT.search(str(frame.get("error", ""))):
            return True
    return False


@dataclass(frozen=True)
class RateLimitPolicy:
    initial_delay_seconds: float = 30.0
    max_delay_seconds: float = 600.0
    max_wait_seconds: float = MAX_PROVIDER_WAIT_SECONDS

    def __post_init__(self) -> None:
        for value in (self.initial_delay_seconds, self.max_delay_seconds, self.max_wait_seconds):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("provider retry delays and wait capacity must be positive and finite")
        if self.initial_delay_seconds > self.max_delay_seconds:
            raise ValueError("initial delay cannot exceed maximum delay")


@dataclass(frozen=True)
class OpusPoolAllocation:
    """Run-owned static allocation; host shares must sum to the account cap."""

    mode: Literal["shared", "per_host"]
    capacity: int
    host_id: str = ""

    def __post_init__(self) -> None:
        if self.mode not in ("shared", "per_host"):
            raise ValueError("Opus pool mode must be shared or per_host")
        if type(self.capacity) is not int or self.capacity <= 0:
            raise ValueError("Opus pool capacity must be a positive integer")
        if self.mode == "per_host" and not self.host_id.strip():
            raise ValueError("per_host Opus pool requires SDO_OPUS_POOL_HOST_ID")

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> OpusPoolAllocation:
        mode = environment.get("SDO_OPUS_POOL_MODE", "shared")
        if mode not in ("shared", "per_host"):
            raise ValueError("Opus pool mode must be shared or per_host")
        default = DEFAULT_OPUS_HOST_SHARE if mode == "per_host" else DEFAULT_OPUS_SESSIONS
        return cls(
            mode,
            int(environment.get("SDO_OPUS_MAX_SESSIONS", str(default))),
            environment.get("SDO_OPUS_POOL_HOST_ID", ""),
        )


@dataclass(frozen=True)
class OpusPool:
    directory: Path
    capacity: int = DEFAULT_OPUS_SESSIONS
    create: bool = True

    def __post_init__(self) -> None:
        if type(self.capacity) is not int or self.capacity <= 0:
            raise ValueError("Opus pool capacity must be a positive integer")

    def try_acquire(self) -> IO[str] | None:
        if self.create:
            self.directory.mkdir(parents=True, exist_ok=True)
        else:
            # Host pools must be provisioned once, including aliases for each
            # existing kind extraMount. Never silently create a private pool.
            for name in ("capacity", *(f"slot-{slot}.lock" for slot in range(self.capacity))):
                if not (self.directory / name).is_file():
                    raise FileNotFoundError(f"host Opus pool is not provisioned: {self.directory / name}")
        # Never unlink lock files, including during release. Replacing an inode
        # would let a second host acquire a different lock for the same token.
        with (self.directory / "capacity").open("a+" if self.create else "r+", encoding="utf-8") as metadata:
            fcntl.flock(metadata, fcntl.LOCK_EX)
            metadata.seek(0)
            recorded = metadata.read().strip()
            if recorded and recorded != str(self.capacity):
                raise ValueError("shared Opus pool capacity differs between callers")
            if not recorded and not self.create:
                raise ValueError("host Opus pool capacity is not provisioned")
            if not recorded:
                metadata.write(f"{self.capacity}\n")
                metadata.flush()
                os.fsync(metadata.fileno())
        for slot in range(self.capacity):
            handle = (self.directory / f"slot-{slot}.lock").open("a" if self.create else "r+", encoding="utf-8")
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                handle.close()
                continue
            return handle
        return None


def prepare_host_pool(directory: Path, aliases: list[Path], *, capacity: int = DEFAULT_OPUS_HOST_SHARE) -> None:
    """Provision one host's inodes into existing app-specific extraMount roots.

    Hard links preserve flock exclusion across different node-visible paths.
    Existing independent files and cross-filesystem aliases fail closed; live
    files are never replaced. Use a shared writable umask for the worker UID.
    """
    pool = OpusPool(directory, capacity)
    handle = pool.try_acquire()  # Also validates immutable capacity metadata.
    if handle is not None:
        handle.close()
    names = ["capacity", *(f"slot-{slot}.lock" for slot in range(capacity))]
    for name in names[1:]:
        (directory / name).touch(exist_ok=True)
    for alias in aliases:
        alias.mkdir(parents=True, exist_ok=True)
        for name in names:
            source, destination = directory / name, alias / name
            try:
                os.link(source, destination)
            except FileExistsError:
                if not source.samefile(destination):
                    raise ValueError(f"Opus pool alias uses a different inode: {destination}") from None


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ProviderCapacity:
    environment: Mapping[str, str] = field(default_factory=lambda: dict(os.environ))
    policy: RateLimitPolicy = field(default_factory=RateLimitPolicy)
    monotonic: Callable[[], float] = time.monotonic
    now: Callable[[], datetime] = _now
    sleep: Callable[[float], None] = time.sleep
    turn_id: str = field(default_factory=lambda: uuid4().hex)
    rate_limit_wait_seconds: float = 0.0
    concurrency_wait_seconds: float = 0.0
    wait_deadline: float | None = field(default=None, init=False)

    @property
    def wait_seconds(self) -> float:
        return self.rate_limit_wait_seconds + self.concurrency_wait_seconds

    @property
    def remaining_seconds(self) -> float:
        remaining = self.policy.max_wait_seconds - self.wait_seconds
        if self.wait_deadline is not None:
            remaining = min(remaining, self.wait_deadline - self.monotonic())
        return max(0.0, remaining)

    @property
    def log_path(self) -> Path | None:
        if value := self.environment.get("SDO_PROVIDER_WAIT_LOG"):
            return Path(value)
        if value := self.environment.get("SDO_SESSION_DIR"):
            return Path(value) / "provider-waits.jsonl"
        if value := self.environment.get("SDO_TURN_USAGE_LOG"):
            return Path(value).parent / "provider-waits.jsonl"
        return None

    def record(self, kind: str, state: str, **fields: object) -> None:
        path = self.log_path
        if path is None:
            return
        event = {
            "schema_version": "sdo.provider-wait/v1",
            "turn_id": self.turn_id,
            "incident_id": self.environment.get("SDO_INCIDENT_ID", ""),
            "phase": self.environment.get("SDO_PHASE", "responder"),
            "kind": kind,
            "state": state,
            "recorded_at": self.now().isoformat(),
            **fields,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = (json.dumps(event, sort_keys=True) + "\n").encode()
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            os.write(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def exhausted(self, reason: str, *, kind: str = "rate_limit", session_id: str = "") -> ProviderRateLimited:
        self.record(kind, "exhausted", reason="rate_limited")
        return ProviderRateLimited(reason, session_id=session_id, retryable=False)

    @contextmanager
    def measured_wait(self, kind: str) -> Iterator[None]:
        wait_id = uuid4().hex
        started = self.monotonic()
        if self.wait_deadline is None:
            self.wait_deadline = started + self.policy.max_wait_seconds
        started_at = self.now().isoformat()
        self.record(kind, "started", wait_id=wait_id, started_at=started_at)
        try:
            yield
        finally:
            duration = max(0.0, self.monotonic() - started)
            if kind == "rate_limit":
                self.rate_limit_wait_seconds += duration
            else:
                self.concurrency_wait_seconds += duration
            self.record(
                kind,
                "finished",
                wait_id=wait_id,
                started_at=started_at,
                finished_at=self.now().isoformat(),
                duration_seconds=duration,
            )

    def backoff(self, attempt: int, *, session_id: str = "") -> None:
        left = self.remaining_seconds
        if left <= 0:
            raise self.exhausted("provider retry budget exhausted", session_id=session_id)
        delay = min(self.policy.initial_delay_seconds * 2 ** min(attempt, 32), self.policy.max_delay_seconds, left)
        with self.measured_wait("rate_limit"):
            self.sleep(delay)
        if self.remaining_seconds <= 0:
            raise self.exhausted("provider retry budget exhausted", session_id=session_id)

    @contextmanager
    def opus_session(self, model: str | None, *, workspace: Path | None = None) -> Iterator[None]:
        if model is not None and "opus" not in model.lower():
            yield
            return
        directory = self.environment.get("SDO_OPUS_POOL_DIR", "").strip()
        if not directory:
            raise self.exhausted("Opus requires a shared filesystem pool", kind="concurrency")
        root = Path(directory).resolve()
        if workspace is not None and root.is_relative_to(workspace.resolve()):
            raise self.exhausted("Opus pool must be outside the agent workspace", kind="concurrency")
        allocation = OpusPoolAllocation.from_environment(self.environment)
        pool = OpusPool(root, allocation.capacity, create=allocation.mode != "per_host")
        try:
            handle = pool.try_acquire()
            if handle is None:
                started = self.monotonic()
                left = self.remaining_seconds
                with self.measured_wait("concurrency"):
                    while handle is None and self.monotonic() - started < left:
                        self.sleep(min(1.0, left - (self.monotonic() - started)))
                        handle = pool.try_acquire()
                if handle is None:
                    raise self.exhausted("Opus admission budget exhausted", kind="concurrency")
        except OSError as exc:
            raise self.exhausted("shared Opus pool unavailable", kind="concurrency") from exc
        try:
            yield
        finally:
            handle.close()
