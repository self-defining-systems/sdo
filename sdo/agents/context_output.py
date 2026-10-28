"""One lossless context boundary for prompts and tool replies."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from functools import lru_cache
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


# A spilled section's inline summary must carry at least this much real content.
MIN_SUMMARY_CHARS = 256

#: Lists a reader acts on first: reflection's per-cause questions, then what was
#: repaired and how. Other lists follow in document order.
PRIORITY_LISTS = (
    "causes",
    "unassigned",
    "repair_actions",
    "plan_applications",
    "confirmed_root_causes",
    "undetected_causes",
    "findings",
    "matches",
)

#: Item detail levels, richest first: (string width, nested list items, nested record depth).
_ITEM_FORMS = ((160, 4, 8), (120, 2, 3), (80, 1, 1), (32, 1, 0))

#: Width of top-level strings, widest first, while the list items still fit.
_SKELETON_WIDTHS = (160, 80, 32)

_SUMMARY_LABEL = "Summary (partial):\n"


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
            # When the prose around a JSON block overflows on its own, the block is a
            # fragment of a prose section and the head is the better summary.
            prose = len(text) - (end + 1 - start)
            document = json.loads(text[start : end + 1]) if 0 <= start < end and prose <= remaining else None
        except ValueError:
            document = None

        if isinstance(document, dict):
            fitted = _fit_summary(document, remaining, size)
            if fitted is not None:
                summary, degraded = fitted
                if size(result + summary) > limit:
                    raise AssertionError(f"summary exceeds its context budget {limit}")
                return ContextText(result + summary, path, degraded=degraded)
        label = f"Summary ({len(text)} chars; partial):\n"
        if remaining > size(label) + 24:
            head = text[: min(1024, remaining - size(label))]
            while size(head) > remaining - size(label):
                head = head[:-1]
            result += label + head
            return ContextText(result, path, degraded=len(head) < min(len(text), MIN_SUMMARY_CHARS))
        return ContextText(result, path, degraded=True)
    return ContextText(result, path)


def _project(value: Any, width: int, count: int, depth: int) -> Any:
    """Shorten strings, keep ``count`` items per nested list, and drop records below ``depth``."""
    if isinstance(value, str):
        if value.startswith(("/", "$SDO_SESSION_SCRATCH/", "$SDO_SESSION_DIR/")):
            return value  # a shortened file reference must never look usable
        return value if len(value) <= width else value[:width] + "…"
    if isinstance(value, list):
        return [_project(child, width, count, depth) for child in value[:count]]
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        dropped = 0
        for key, child in value.items():
            nested = isinstance(child, dict) or (isinstance(child, list) and any(isinstance(c, dict) for c in child))
            if nested and depth <= 0:
                dropped += 1
                continue
            result[key] = _project(child, width, count, depth - 1 if nested else depth)
            if isinstance(child, list) and len(child) > count:
                result[key + "_omitted"] = len(child) - count
        if dropped:
            result["fields_omitted"] = dropped
        return result
    return value


def _count_omitted(target: dict[str, Any], key: str, total: int) -> None:
    omitted = total - len(target[key])
    if omitted:
        target[key + "_omitted"] = omitted
    else:
        target.pop(key + "_omitted", None)


def _fit_summary(
    document: dict[str, Any], limit: int, size: Callable[[str], int]
) -> tuple[str, bool] | None:
    """The richest JSON summary of ``document`` within ``limit``, and whether it is a placeholder.

    A placeholder leaves a nonempty list with no items. The first top-level
    string width that avoids one wins; otherwise the fullest attempt does.
    """
    best: tuple[int, str, bool] | None = None
    for width in _SKELETON_WIDTHS:
        attempt = _fill(document, width, limit, size)
        if attempt is None:
            continue
        if not attempt[2]:
            return attempt[1], False
        if best is None or attempt[0] > best[0]:
            best = attempt
    return (best[1], best[2]) if best is not None else None


def _fill(
    document: dict[str, Any], width: int, limit: int, size: Callable[[str], int]
) -> tuple[int, str, bool] | None:
    """Fill list slots round-robin, priority lists first, each item at the richest form that fits.

    Every list reachable through records is a slot. Slots start empty with their
    full length counted as omitted, so every list shows its first item before
    any list shows a second, and only the items that do not fit are counted.
    Returns the items shown, the rendered summary and whether it is a placeholder.
    """
    skeleton = _project(document, width, 0, 8)
    slots: list[tuple[dict[str, Any], str, list[Any]]] = []
    pending: list[tuple[dict[str, Any], dict[str, Any]]] = [(document, skeleton)]
    while pending:
        source, target = pending.pop(0)
        for key, child in source.items():
            if isinstance(child, list) and child:
                slots.append((target, key, child))
            elif isinstance(child, dict) and isinstance(target.get(key), dict):
                pending.append((child, target[key]))

    def rendered() -> str:
        return _SUMMARY_LABEL + json.dumps(skeleton, ensure_ascii=False)

    if size(rendered()) > limit:
        return None
    rank = {key: index for index, key in enumerate(PRIORITY_LISTS)}
    slots.sort(key=lambda slot: rank.get(slot[1], len(rank)))

    def place(target: dict[str, Any], key: str, items: list[Any], index: int, forms: Sequence[tuple[int, int, int]]) -> bool:
        """Put ``items[index]`` at the first of ``forms`` that fits, keeping any earlier form otherwise."""
        visible = target[key]
        previous = visible[index] if index < len(visible) else None
        for item_width, count, depth in forms:
            candidate = _project(items[index], item_width, count, depth)
            if previous is None:
                visible.append(candidate)
            else:
                visible[index] = candidate
            _count_omitted(target, key, len(items))
            if size(rendered()) <= limit:
                return True
            if previous is None:
                visible.pop()
            else:
                visible[index] = previous
            _count_omitted(target, key, len(items))
        return False

    # Every list first shows one lean item, then those items get richer, then lists grow.
    for target, key, items in slots:
        place(target, key, items, 0, _ITEM_FORMS[-1:])
    for target, key, items in slots:
        if target[key]:
            place(target, key, items, 0, _ITEM_FORMS[:-1])
    growing = [slot for slot in slots if 0 < len(slot[0][slot[1]]) < len(slot[2])]
    while growing:
        growing = [
            (target, key, items)
            for target, key, items in growing
            if place(target, key, items, len(target[key]), _ITEM_FORMS) and len(target[key]) < len(items)
        ]
    shown = sum(len(target[key]) for target, key, _ in slots)
    return shown, rendered(), any(not target[key] for target, key, _ in slots)
