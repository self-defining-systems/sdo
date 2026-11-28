"""The responder session's finish: Go, not the agent, decides when the incident work is done.

Before Recovered holds, a finish is answered with Go's facts and one fixed reflective ask. From
inside a Claude turn the ``StructuredOutput`` call is refused with that text through a
``PreToolUse`` hook (``python -m sdo.agents.responder.finish``), so the turn goes on. After a turn
ends (any provider), the worker asks again and SDO answers ``wait``, ``resume`` (the same provider
session, with the message) or ``end`` (Recovered, a page, or a fresh session is next).
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TextIO

    from agentshim import ClaudeHook

#: The worker asks again at most this often while SDO waits.
MIN_POLL_SECONDS = 1.0
#: How many times a finish is asked before an unavailable controller ends it.
ASK_ATTEMPTS = 3
FinishAction = Literal["end", "deny", "wait", "resume"]


@dataclass(frozen=True)
class FinishAnswer:
    """SDO's answer to a finish."""

    action: FinishAction
    message: str = ""
    wait_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.action not in ("end", "deny", "wait", "resume"):
            raise ValueError(f"unknown finish action {self.action!r}")
        if self.action in ("deny", "resume") and not self.message.strip():
            raise ValueError(f"finish action {self.action!r} needs a message")


def _tools_directory() -> Path:
    return Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools"))


def ask(*, in_turn: bool, status: str, resumable: bool = True, reason: str = "") -> FinishAnswer:
    """Tell SDO the session is finishing; an unavailable controller ends the finish."""
    from sdo.agents.responder.tools_cli import _exchange

    finish: dict[str, object] = {"in_turn": in_turn, "status": status, "resumable": resumable}
    if reason:
        finish["reason"] = reason
    response: dict[str, object] = {}
    for attempt in range(ASK_ATTEMPTS):
        try:
            response = _exchange({"command": "finish", "finish": finish}, _tools_directory())
            break
        except (OSError, ValueError, TimeoutError):
            # One transient tools error must not end a resumable session.
            if attempt == ASK_ATTEMPTS - 1:
                return FinishAnswer("end")
            time.sleep(MIN_POLL_SECONDS)
    answer = response.get("finish")
    if not isinstance(answer, dict):
        return FinishAnswer("end")
    try:
        return FinishAnswer(
            str(answer.get("action", "end")),  # type: ignore[arg-type]
            str(answer.get("message", "")),
            float(answer.get("wait_seconds", 0) or 0),
        )
    except (TypeError, ValueError):
        return FinishAnswer("end")


def enabled() -> bool:
    """A finish is asked only in a responder session with SDO's tools."""
    return os.environ.get("SDO_RESPONDER_SESSION") == "1"


def claude_hooks() -> tuple[ClaudeHook, ...]:
    """The in-turn finish reply: a ``PreToolUse`` hook on Claude's ``StructuredOutput`` call."""
    from agentshim import ClaudeHook

    return (ClaudeHook(event="PreToolUse", matcher="StructuredOutput", command=[sys.executable, "-m", __name__]),)


def await_decision(
    status: str, *, resumable: bool, reason: str = "", sleep: Callable[[float], None] = time.sleep
) -> FinishAnswer:
    """After a turn ended: ask until SDO resumes or ends the session."""
    while True:
        answer = ask(in_turn=False, status=status, resumable=resumable, reason=reason)
        if answer.action != "wait":
            return answer
        sleep(max(MIN_POLL_SECONDS, answer.wait_seconds))


def _proposed_status(tool_input: object) -> str:
    """The status of the incident result a StructuredOutput call proposes."""
    if not isinstance(tool_input, dict):
        return ""
    for document in (tool_input, tool_input.get("value"), tool_input.get("incident_result")):
        if isinstance(document, dict):
            branch = document.get("incident_result", document)
            if isinstance(branch, dict) and isinstance(branch.get("status"), str):
                return branch["status"]
    return ""


def hook_main(stdin: TextIO | None = None, stdout: TextIO | None = None) -> int:
    """Claude Code ``PreToolUse`` hook on ``StructuredOutput``: refuse a finish before Recovered."""
    source: TextIO = stdin or sys.stdin
    sink: TextIO = stdout or sys.stdout
    try:
        event = json.load(source)
    except ValueError:
        return 0
    tool_input = event.get("tool_input") if isinstance(event, dict) else None
    if isinstance(tool_input, dict) and "incident_result" in tool_input and tool_input["incident_result"] is None:
        return 0  # the other branch (a closing turn) is never judged here
    answer = ask(in_turn=True, status=_proposed_status(tool_input))
    if answer.action != "deny":
        return 0
    json.dump(
        {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": answer.message,
            }
        },
        sink,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(hook_main())
