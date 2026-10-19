"""Render Go's typed broker rejection diagnostics for an agent prompt."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sdo.contracts import MemoryDiagnostic


def render_validation_feedback(validation_attempt: int, diagnostics: Iterable[MemoryDiagnostic]) -> str:
    """Return the retry section of a prompt, or "" for a first dispatch."""

    rows = list(diagnostics)
    if not rows:
        return ""
    lines = [
        f"This is validation attempt {validation_attempt}. The controller's broker rejected your previous "
        "proposal and rolled it back; the checkout is clean. Fix every diagnostic below:"
    ]
    for row in rows:
        location = row.path if not row.line else f"{row.path}:{row.line}"
        text = f"- [{row.rule}] {location + ': ' if location else ''}{row.message}"
        if row.hint:
            text += f" (hint: {row.hint})"
        lines.append(text)
    return "\n".join(lines) + "\n"
