"""One curated failure line for the kubelet's termination message.

The controller reads a failed worker's `/dev/termination-log`, never its logs. The line carries the exception
class and a short single-line message: no traceback, no tool output, no environment.
"""

from __future__ import annotations

import functools
import re
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

TERMINATION_LOG = Path("/dev/termination-log")
MAX_MESSAGE_CHARS = 200

# Defense in depth: even a short exception message can carry a credential from a failed call.
_SECRET_SHAPES = re.compile(
    r"(?i)(bearer\s+\S+|sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9]{16,}|(?:api[_-]?key|token|secret|password)\s*[=:]\s*\S+|[A-Za-z0-9+/=_-]{32,})"
)


def failure_line(exc: BaseException) -> str:
    """Return `ClassName: first line of the message`, redacted and bounded."""
    lines = str(exc).strip().splitlines()
    message = _SECRET_SHAPES.sub("[redacted]", lines[0] if lines else "")
    if len(message) > MAX_MESSAGE_CHARS:
        message = message[:MAX_MESSAGE_CHARS] + "…"
    return f"{type(exc).__name__}: {message}" if message else type(exc).__name__


def record_worker_failure(exc: BaseException, *, path: Path | None = None) -> None:
    """Write the one failure line; never raise, a failed write must not mask the failure."""
    try:
        (path or TERMINATION_LOG).write_text(failure_line(exc) + "\n", encoding="utf-8")
    except OSError:
        pass


def records_worker_failure(main: Callable[[], int]) -> Callable[[], int]:
    """Make an uncaught exception or a nonzero exit leave a termination line behind."""

    @functools.wraps(main)
    def wrapped() -> int:
        try:
            code = main()
        except Exception as exc:
            record_worker_failure(exc)
            raise
        if code != 0 and not _recorded():
            record_worker_failure(RuntimeError(f"worker exited with code {code}"))
        return code

    return wrapped


def _recorded() -> bool:
    try:
        return bool(TERMINATION_LOG.read_text(encoding="utf-8").strip())
    except OSError:
        return False
