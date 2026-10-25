"""Render Go's typed broker rejection diagnostics for an agent prompt."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sdo.agents.prompt_templates import render_prompt

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sdo.contracts import MemoryDiagnostic


def render_validation_feedback(validation_attempt: int, diagnostics: Iterable[MemoryDiagnostic]) -> str:
    """Return the retry section of a prompt, or "" for a first dispatch."""

    rows = list(diagnostics)
    if not rows:
        return ""
    lines = [
        render_prompt(
            "lifecycle",
            "render_validation_feedback_01",
            part="validation.instructions",
            validation_attempt=validation_attempt,
        )
    ]
    for row in rows:
        location = row.path if not row.line else f"{row.path}:{row.line}"
        text = render_prompt(
            "lifecycle",
            "render_validation_feedback_02",
            part="validation.instructions",
            rule=row.rule,
            value=location + ": " if location else "",
            message=row.message,
        )
        if row.hint:
            text += render_prompt(
                "lifecycle", "render_validation_feedback_03", part="validation.instructions", hint=row.hint
            )
        lines.append(text)
    return "\n".join(lines) + "\n"
