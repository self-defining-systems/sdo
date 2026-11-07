"""Read and write GitHub's REST API through the `gh` CLI, retrying transient failures.

A 5xx, a rate limit or a dropped connection is retried a bounded number of times with
exponential backoff; when every attempt fails the caller gets `GithubApiUnavailable`, a
typed result it reports instead of crashing. A request the server rejected for good
(404, 410, 401, 422) is not retried and keeps raising `subprocess.CalledProcessError`.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")
ATTEMPTS = 4
BACKOFF_SECONDS = 2.0
PERMANENT = ("HTTP 404", "HTTP 410", "HTTP 401", "HTTP 422")


class GithubApiUnavailable(Exception):
    """GitHub's API kept failing after every retry; the data this run wanted is unknown."""

    def __init__(self, path: str, attempts: int, last_error: str) -> None:
        super().__init__(f"GitHub API unavailable for {path} after {attempts} attempts: {last_error}")
        self.path = path
        self.attempts = attempts
        self.last_error = last_error


def is_permanent(error: subprocess.CalledProcessError) -> bool:
    return any(marker in (error.stderr or "") for marker in PERMANENT)


def with_retries(
    path: str,
    call: Callable[[], T],
    sleep: Callable[[float], None] = time.sleep,
    attempts: int = ATTEMPTS,
    backoff: float = BACKOFF_SECONDS,
) -> T:
    if attempts < 1 or backoff < 0:
        raise ValueError("attempts must be at least 1 and backoff non-negative")
    last = ""
    for attempt in range(1, attempts + 1):
        try:
            return call()
        except subprocess.CalledProcessError as error:
            if is_permanent(error):
                raise
            lines = (error.stderr or "").strip().splitlines()
            last = lines[-1] if lines else str(error)
        if attempt < attempts:
            sleep(backoff * 2 ** (attempt - 1))
    raise GithubApiUnavailable(path, attempts, last)


def _run(path: str, extra: list[str]) -> str:
    repo = os.environ["GITHUB_REPOSITORY"]
    command = ["gh", "api", *extra, f"repos/{repo}/{path}"]
    return with_retries(path, lambda: subprocess.run(command, check=True, capture_output=True, text=True).stdout)


def gh_json(path: str, method: str = "GET") -> Any:
    out = _run(path, ["-X", method])
    return json.loads(out) if out.strip() else None


def gh_text(path: str) -> str:
    return _run(path, ["--allow-escape-sequences"])
