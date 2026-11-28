"""Tool calls of a Claude Code ``stream-json`` onboarding transcript, in order, with their results."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path  # noqa: TC003
from typing import Any

# The SDO operator subcommands that judge the agent's files: each call is one validator call.
VALIDATOR_PATTERN = re.compile(r"sdo-operator\s+(?:memory\s+(?:check|test)|onboarding\s+(?:journeys|check))\b")
SDO_CALL_PATTERN = re.compile(r"sdo-operator\s+(\w+)(?:\s+(\w+))?")


@dataclass(frozen=True)
class ToolCall:
    """One tool call of the main agent and the text of its result."""

    index: int
    tool_use_id: str
    name: str
    input: dict[str, Any]
    result: str
    is_error: bool

    @property
    def command(self) -> str:
        return str(self.input.get("command", "")) if self.name == "Bash" else ""

    @property
    def file_path(self) -> str:
        return str(self.input.get("file_path", "")) if self.name in ("Write", "Edit", "MultiEdit") else ""

    def sdo_calls(self) -> list[str]:
        """The ``sdo-operator`` subcommands a Bash call runs, such as ``onboarding journeys``."""
        return [" ".join(part for part in match if part) for match in SDO_CALL_PATTERN.findall(self.command)]

    def validates(self) -> bool:
        return bool(VALIDATOR_PATTERN.search(self.command))


def _result_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(str(part.get("text", "")) for part in content if isinstance(part, dict))
    return ""


def read_tool_calls(path: Path) -> list[ToolCall]:
    """Return the main agent's tool calls in call order; subagent (sidechain) calls are skipped."""
    pending: dict[str, tuple[str, dict[str, Any]]] = {}
    order: list[str] = []
    results: dict[str, tuple[str, bool]] = {}
    for line in path.read_text().splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if not isinstance(event, dict) or event.get("parent_tool_use_id") or event.get("isSidechain"):
            continue
        message = event.get("message")
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            if event.get("type") == "assistant" and part.get("type") == "tool_use":
                pending[part["id"]] = (part["name"], dict(part.get("input") or {}))
                order.append(part["id"])
            elif event.get("type") == "user" and part.get("type") == "tool_result":
                results[part["tool_use_id"]] = (_result_text(part.get("content")), bool(part.get("is_error")))
    calls = []
    for index, tool_use_id in enumerate(order):
        name, arguments = pending[tool_use_id]
        text, is_error = results.get(tool_use_id, ("", False))
        calls.append(ToolCall(index, tool_use_id, name, arguments, text, is_error))
    return calls
