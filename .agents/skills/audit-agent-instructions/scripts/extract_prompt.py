#!/usr/bin/env python3
"""Split a Codex rollout without repeating mirrored event_msg records.

Default output is a size/provenance report; --include-text is for private local
inspection only. Categories describe transport placement, not instruction trust.
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
from pathlib import Path
from typing import Any

DATA_START = re.compile(
    r"(?m)^(?:Incident request(?: summary)?|Request(?: data)?|Task data|Outcome(?: data)?|"
    r"Operational memory snapshot|Accepted memory snapshot|Per-cause work|Current outcome|"
    r"Outcome commit|Architecture summary \(trusted memory snapshot\)|"
    r"Health objective \(trusted memory snapshot\)|Latest detector evidence \(compacted from detector_history\))"
    r"(?: JSON)?:[ \t]*|"
    r"(?m:^<(?:request|incident_request|request_data)>\s*)"
)
SKILL_PATH = re.compile(r"[^\s\"'`;|<>]*skills/[^\s\"'`;|<>]+(?:SKILL\.md|references/[^\s\"'`;|<>]+\.md)")
READ_COMMAND = re.compile(r"(?:^|[\n;&|]\s*|\s)(?:cat|sed|head|tail|read_file)\s")


def message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(part.get("text", "") for part in content if isinstance(part, dict))
    if isinstance(content, dict):
        return str(content.get("text", ""))
    return ""


def extract(path: Path, *, include_text: bool = False) -> dict[str, Any]:
    """Keep every text segment's original line and Unicode character count."""
    categories: dict[str, list[dict[str, Any]]] = {
        name: [] for name in ("developer_harness", "our_instructions", "request_data")
    }
    calls: list[dict[str, Any]] = []
    skills: set[str] = set()
    help_count = 0

    def add(category: str, text: str, line: int, source: str) -> None:
        if not text:
            return
        item: dict[str, Any] = {"line": line, "source": source, "chars": len(text)}
        if include_text:
            item["text"] = text
        categories[category].append(item)

    for line, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid rollout JSON on line {line}: {exc.msg}") from exc
        payload = record.get("payload", {})
        if record.get("type") == "session_meta":
            add(
                "developer_harness",
                message_text(payload.get("base_instructions", "")),
                line,
                "session_meta.base_instructions",
            )
        if record.get("type") != "response_item":
            continue
        kind = payload.get("type")
        if kind == "message":
            role = payload.get("role")
            text = message_text(payload.get("content", []))
            if role in {"developer", "system"}:
                add("developer_harness", text, line, str(role))
            elif role == "user":
                if text.lstrip().startswith(("<environment_context>", "<permissions")):
                    add("developer_harness", text, line, "user.environment")
                else:
                    marker = DATA_START.search(text)
                    split = marker.start() if marker else len(text)
                    add("our_instructions", text[:split], line, "user")
                    add("request_data", text[split:], line, "user")
        elif kind in {"function_call", "custom_tool_call"}:
            arguments = payload.get("arguments", payload.get("input", ""))
            if isinstance(arguments, str):
                try:
                    arguments = json.loads(arguments)
                except json.JSONDecodeError:
                    pass
            name = str(payload.get("name", ""))
            call: dict[str, Any] = {"line": line, "name": name, "chars": len(json.dumps(arguments, ensure_ascii=False))}
            if include_text:
                call["arguments"] = arguments
            calls.append(call)
            command = arguments.get("cmd", arguments.get("command", "")) if isinstance(arguments, dict) else arguments
            if not isinstance(command, str):
                continue
            if READ_COMMAND.search(command) or "read_file" in name:
                skills.update(match.group().rstrip(").,") for match in SKILL_PATH.finditer(command))
            try:
                help_count += shlex.split(command).count("--help")
            except ValueError:
                pass
    return {
        "segments": categories,
        "sizes": {name: sum(item["chars"] for item in items) for name, items in categories.items()},
        "tool_calls": calls,
        "skill_files_read": sorted(skills),
        "help_count": help_count,
        "classification_note": (
            "User text before a recognized request marker is instructions; inspect mixed payloads manually. "
            "Tool calls show attempted reads; confirm success in tool results."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rollout", type=Path)
    parser.add_argument("--include-text", action="store_true")
    args = parser.parse_args()
    print(json.dumps(extract(args.rollout, include_text=args.include_text), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
