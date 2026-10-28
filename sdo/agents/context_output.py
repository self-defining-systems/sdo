"""One lossless context boundary for prompts and tool replies."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from functools import lru_cache
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


# A spilled section's inline summary must carry at least this much real content.
MIN_SUMMARY_CHARS = 256


class ContextText(str):
    """Bounded text with the exact private files its producer created.

    ``degraded`` marks a placeholder: the inline part is a field skeleton, a
    fragment or nothing, so a reader learns almost nothing without the file.
    """

    context_files: tuple[Path, ...]
    degraded: bool

    def __new__(cls, text: str, path: Path, *, degraded: bool = False) -> ContextText:
        value = super().__new__(cls, text)
        value.context_files = (path,)
        value.degraded = degraded
        return value


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
    scratch_variable = "SDO_SESSION_SCRATCH" if os.environ.get("SDO_SESSION_SCRATCH") else "SDO_SESSION_DIR"
    scratch = os.environ.get(scratch_variable)
    directory = Path(scratch) / "context" if scratch else Path("/tmp/sdo-context")
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    digest = hashlib.sha256(text.encode()).hexdigest()[:24]
    path = directory / f"{digest}.txt"
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=directory, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(text)
            stream.close()
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_text() != text:
                    raise ValueError("context digest collision") from None
        finally:
            temporary.unlink(missing_ok=True)
    reference = f"${scratch_variable}/context/{path.name}" if scratch else str(path)
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
        envelope: dict[str, Any] = {"summarized": True, "characters": len(text), "details_file": str(path)}
        try:
            document = json.loads(text)
        except ValueError:
            document = None
        if isinstance(document, dict):
            # Keep decisive scalar facts without treating absent/omitted fields as success.
            for key in ("command", "healthy", "targeted_healthy", "success", "exit_code", "error", "summary"):
                value = document.get(key)
                if isinstance(value, str):
                    value = " ".join(value.split())
                    value = value if len(value) <= 128 else value[:127] + "…"
                elif value is not None and not isinstance(value, (bool, int, float)):
                    continue
                if key in document and size(json.dumps({**envelope, key: value}, ensure_ascii=False)) <= limit:
                    envelope[key] = value
            counts = {key: len(value) for key, value in document.items() if isinstance(value, list)}
            if counts and size(json.dumps({**envelope, "counts": counts}, ensure_ascii=False)) <= limit:
                envelope["counts"] = counts
        result = json.dumps(envelope, ensure_ascii=False)
    else:
        result = f"Full: {reference}\n"
    if size(result) > limit:
        raise ValueError(f"context budget {limit} cannot hold the full-data reference")
    if not json_output:
        remaining = limit - size(result)
        # Summarize structured payloads uniformly, including fenced prompt data.
        start, end = text.find("{"), text.rfind("}")
        try:
            # A JSON fragment inside prose is not the section; its head is a better summary.
            document = (
                json.loads(text[start : end + 1]) if 0 <= start < end and (end + 1 - start) * 2 >= len(text) else None
            )
        except ValueError:
            document = None

        def project(value: Any, width: int, count: int, depth: int = 0) -> Any:
            if isinstance(value, str):
                if value.startswith(("/", "$SDO_SESSION_SCRATCH/", "$SDO_SESSION_DIR/")):
                    return value  # a shortened file reference must never look usable
                return value if len(value) <= width else value[:width] + "…"
            if isinstance(value, list):
                return [project(child, width, count, depth + 1) for child in value[:count]]
            if isinstance(value, dict):
                if depth >= 8:
                    return {"keys": list(value)}
                result = {key: project(child, width, count, depth + 1) for key, child in value.items()}
                for key, child in value.items():
                    if isinstance(child, list) and len(child) > count:
                        result[key + "_omitted"] = len(child) - count
                return result
            return value

        if isinstance(document, dict):
            for width, count in ((160, 4), (80, 2), (32, 1), (0, 0)):
                summary = "Summary (partial):\n" + json.dumps(project(document, width, count), ensure_ascii=False)
                if size(summary) <= remaining:
                    return ContextText(result + summary, path, degraded=width < 80)
        label = f"Summary ({len(text)} chars; partial):\n"
        if remaining > size(label) + 24:
            head = text[: min(1024, remaining - size(label))]
            while size(head) > remaining - size(label):
                head = head[:-1]
            result += label + head
            return ContextText(result, path, degraded=len(head) < min(len(text), MIN_SUMMARY_CHARS))
        return ContextText(result, path, degraded=True)
    return ContextText(result, path)
