"""Render Go's typed broker rejection diagnostics for an agent prompt."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from google.protobuf import json_format

from sdo.agents.prompt_templates import RenderedPrompt, join_prompts, named_prompt, render_prompt

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sdo.contracts import MemoryDiagnostic


def render_validation_feedback(validation_attempt: int, diagnostics: Iterable[MemoryDiagnostic]) -> RenderedPrompt:
    """Return the retry section of a prompt, or "" for a first dispatch."""

    rows = list(diagnostics)
    if not rows:
        return named_prompt("reflection.validation_feedback", "")
    lines = [
        render_prompt(
            "lifecycle",
            "render_validation_feedback_01",
            part="reflection.validation_feedback",
            validation_attempt=validation_attempt,
        )
    ]
    for row in rows:
        location = row.path if not row.line else f"{row.path}:{row.line}"
        text = render_prompt(
            "lifecycle",
            "render_validation_feedback_02",
            part="reflection.validation_feedback",
            rule=row.rule,
            value=location + ": " if location else "",
            message=row.message,
        )
        if row.hint:
            text += render_prompt(
                "lifecycle", "render_validation_feedback_03", part="reflection.validation_feedback", hint=row.hint
            )
        if row.HasField("memory_conflict"):
            text += render_prompt(
                "lifecycle",
                "memory_conflict",
                part="reflection.validation_feedback",
                conflict_json=json.dumps(
                    json_format.MessageToDict(row.memory_conflict, preserving_proto_field_name=True), indent=2
                ),
            )
        lines.append(text)
    return join_prompts("\n", lines, part="reflection.validation_feedback") + named_prompt(
        "reflection.validation_feedback", "\n"
    )
