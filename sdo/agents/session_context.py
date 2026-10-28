"""Measure canonical provider session files, including tools outside SDO's CLI."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

from sdo.agents.context_output import budget_for


def text_content(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(text_content(item) for item in value)
    if isinstance(value, dict):
        if "text" in value:
            return text_content(value["text"])
        if "content" in value:
            return text_content(value["content"])
    raise ValueError("Unsupported provider session text shape")


@dataclass(frozen=True)
class SessionContext:
    first_message_chars: int
    largest_message_chars: int
    total_tool_output_chars: int
    tool_outputs: int

    def check(self, role: str) -> None:
        limit = budget_for("session_context." + role + "_first")
        if self.largest_message_chars > limit:
            raise ValueError(f"{role}: provider message {self.largest_message_chars} > {limit} chars")
        tool_limit = budget_for("session_context.tool_total")
        if self.total_tool_output_chars > tool_limit:
            raise ValueError(f"provider tool output {self.total_tool_output_chars} > {tool_limit} chars")

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


def measure_session(path: Path) -> SessionContext:
    """Read Codex response items or Claude user/tool_result messages, once each."""
    messages: list[int] = []
    tools: list[int] = []
    for line in path.read_text().splitlines():
        item = json.loads(line)
        if item.get("type") == "response_item":
            payload = item["payload"]
            kind = payload["type"]
            if kind in {"function_call_output", "custom_tool_call_output"}:
                tools.append(len(text_content(payload["output"])))
            elif kind == "message" and payload.get("role") == "user":
                text = text_content(payload["content"])
                if not text.startswith("<environment_context>"):
                    messages.append(len(text))
        elif item.get("type") == "user" and "message" in item:
            content = item["message"]["content"]
            if isinstance(content, str):
                messages.append(len(content))
            else:
                for block in content:
                    if block.get("type") == "tool_result":
                        tools.append(len(text_content(block["content"])))
                    elif block.get("type") == "text":
                        messages.append(len(block["text"]))
    if not messages:
        raise ValueError(f"{path}: no provider user messages found")
    return SessionContext(messages[0], max(messages), sum(tools), len(tools))
