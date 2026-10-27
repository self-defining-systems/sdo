"""One-shot `uv run` calls into SREGym's own environment: uv warnings are warnings, failures are typed."""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

FailureCause = Literal["launch", "timeout", "exit", "no_result"]
_TAIL_CHARS = 2000


class SregymSubprocessError(RuntimeError):
    """A one-shot SREGym call failed; `cause` says how, the message carries its real output."""

    def __init__(self, what: str, cause: FailureCause, detail: str) -> None:
        super().__init__(f"SREGym {what} failed ({cause}): {detail}" if detail else f"SREGym {what} failed ({cause})")
        self.what = what
        self.cause: FailureCause = cause
        self.detail = detail


def sregym_env(base: Mapping[str, str], **extra: str) -> dict[str, str]:
    """The environment for a call into SREGym's project environment.

    An inherited VIRTUAL_ENV names the caller's venv, not SREGym's, so uv warns about it on every call.
    """

    env = {key: value for key, value in base.items() if key != "VIRTUAL_ENV"}
    env.update(extra)
    return env


def stderr_without_warnings(text: str) -> str:
    """Keep what a failure printed; drop uv's own `warning:` lines, which never explain one."""

    return "\n".join(line for line in text.splitlines() if not line.startswith("warning:")).strip()


def _result_json(stdout: str) -> Any | None:
    """The structured result: the whole output, else its last JSON line (tools may log before it)."""

    for candidate in (stdout, *reversed([line for line in stdout.splitlines() if line.strip()])):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    return None


def run_sregym_json(
    what: str,
    argv: Sequence[str],
    *,
    input_text: str,
    env: Mapping[str, str],
    timeout: float,
    error: type[SregymSubprocessError] = SregymSubprocessError,
) -> Any:
    """Run one SREGym worker and return its JSON result; a warning on stderr never decides success."""

    try:
        completed = subprocess.run(
            list(argv),
            input=input_text,
            capture_output=True,
            text=True,
            env=dict(env),
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise error(what, "timeout", f"no result within {timeout:g}s") from exc
    except OSError as exc:
        raise error(what, "launch", f"{type(exc).__name__}: {exc}") from exc
    stderr = stderr_without_warnings(completed.stderr or "")
    if completed.returncode != 0:
        output = stderr or (completed.stdout or "").strip() or "no output"
        raise error(what, "exit", f"exit {completed.returncode}: {output[-_TAIL_CHARS:]}")
    result = _result_json(completed.stdout or "")
    if result is None:
        raise error(what, "no_result", (stderr or "no output")[-_TAIL_CHARS:])
    return result
