"""List oversized recorded tool responses without printing their contents.

Usage: uv run python -m benchmarks.sregym.analysis.tool_output_audit rollout.jsonl --threshold 3000
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

_COMMAND = re.compile(
    r"\bsdo\s+(status|menu|diagnose|mitigate|act|detect|verify|source|check|plan|apply|rollback|report)\b"
)


@dataclass(frozen=True)
class OversizedOutput:
    ordinal: int
    tool: str
    commands: tuple[str, ...]
    chars: int
    utf8_bytes: int


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "\n".join(item["text"] for item in value if isinstance(item, dict) and isinstance(item.get("text"), str))
    return ""


def audit_outputs(records: Iterable[dict[str, Any]], *, threshold: int = 3000) -> Iterator[OversizedOutput]:
    """Join Codex function/custom outputs to their calls; count Unicode and UTF-8 separately."""
    if threshold < 0:
        raise ValueError("threshold must be nonnegative")
    calls: dict[str, tuple[str, tuple[str, ...]]] = {}
    for index, record in enumerate(records):
        if record.get("type") != "response_item":
            continue
        payload = record.get("payload", {})
        identifier = payload.get("call_id", "")
        kind = payload.get("type")
        if kind in ("function_call", "custom_tool_call"):
            arguments = payload.get("input", payload.get("arguments", ""))
            calls[identifier] = (payload.get("name", "unknown"), tuple(dict.fromkeys(_COMMAND.findall(str(arguments)))))
        elif kind in ("function_call_output", "custom_tool_call_output"):
            output = _text(payload.get("output"))
            if len(output) > threshold:
                tool, commands = calls.get(identifier, ("unknown", ()))
                yield OversizedOutput(
                    int(record.get("ordinal", index)), tool, commands, len(output), len(output.encode("utf-8"))
                )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rollout", type=Path)
    parser.add_argument(
        "--threshold", type=int, default=3000, help="Unicode characters, excluding the threshold itself"
    )
    args = parser.parse_args()
    if args.threshold < 0:
        parser.error("threshold must be nonnegative")
    with args.rollout.open(encoding="utf-8") as source:
        for item in audit_outputs((json.loads(line) for line in source if line.strip()), threshold=args.threshold):
            print(json.dumps(asdict(item), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
