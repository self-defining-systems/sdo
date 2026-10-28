"""One lossless context boundary for prompts and tool replies."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from functools import lru_cache
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


@lru_cache(maxsize=1)
def budgets() -> dict[str, int]:
    document = tomllib.loads(Path(__file__).with_name("prompt_budgets.toml").read_text())
    return {f"{role}.{part}": entry["max_chars"] for role, parts in document.items() for part, entry in parts.items()}


def budget_for(name: str) -> int:
    try:
        return budgets()[name]
    except KeyError as exc:
        raise ValueError(f"Missing context budget: {name}") from exc


def save_context(text: str) -> tuple[Path, str]:
    """Content-addressed private evidence survives all turns in the session."""
    scratch = os.environ.get("SDO_SESSION_SCRATCH")
    directory = Path(scratch) / "context" if scratch else Path("/tmp/sdo-context")
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    digest = hashlib.sha256(text.encode()).hexdigest()[:24]
    path = directory / f"{digest}.txt"
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.read_text() != text:
            raise ValueError("context digest collision") from None
    else:
        with os.fdopen(descriptor, "w") as stream:
            stream.write(text)
    reference = f"$SDO_SESSION_SCRATCH/context/{path.name}" if scratch else str(path)
    return path, reference


def cap_text(text: str, limit: int, *, json_output: bool = False, bytes_limit: bool = False) -> str:
    """Bound the complete emitted representation, including its file pointer and framing."""
    def size(value: str) -> int:
        return len(value.encode()) if bytes_limit else len(value)

    if size(text) <= limit:
        return text
    path, reference = save_context(text)
    if json_output:
        # JSON paths are literal readable paths, never shell expressions.
        result = json.dumps({"summarized": True, "characters": len(text), "details_file": str(path)}, ensure_ascii=False)
    else:
        result = f"Full: {reference}\n"
    if size(result) > limit:
        raise ValueError(f"context budget {limit} cannot hold the full-data reference")
    if not json_output:
        remaining = limit - size(result)
        label = f"Summary ({len(text)} chars; partial):\n"
        if remaining > size(label) + 24:
            head = text[:remaining - size(label)]
            while size(head) > remaining - size(label):
                head = head[:-1]
            result += label + head
    return result
