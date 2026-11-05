"""Render Go's typed broker rejection diagnostics for an agent prompt."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from google.protobuf import json_format

from sdo.agents.context_output import budget_for
from sdo.agents.prompt_templates import RenderedPrompt, join_prompts, named_prompt, render_prompt

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sdo.contracts import MemoryDiagnostic


def render_validation_feedback(
    validation_attempt: int, diagnostics: Iterable[MemoryDiagnostic], *, kept_edits: bool = False
) -> RenderedPrompt:
    """Return the retry section of a prompt, or "" for a first dispatch.

    ``kept_edits`` states that the broker carried the rejected edits forward
    instead of rolling the checkout back.
    """

    rows = list(diagnostics)
    if not rows:
        return named_prompt("reflection.validation_feedback", "")
    lines = [
        render_prompt(
            "lifecycle",
            "render_validation_feedback_04" if kept_edits else "render_validation_feedback_01",
            part="reflection.validation_feedback",
            validation_attempt=validation_attempt,
        )
    ]
    shown, omitted = _fitting_rows(rows)
    for row in shown:
        location = _location(row)
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
    for rule, count in omitted.items():
        lines.append(
            named_prompt(
                "reflection.validation_feedback",
                f"- {count} more [{rule}] diagnostics; run `sdo-operator memory check` to list them all",
            )
        )
    return join_prompts("\n", lines, part="reflection.validation_feedback") + named_prompt(
        "reflection.validation_feedback", "\n"
    )


_ROW_FIELDS = ("rule", "value", "message")


def _location(row: MemoryDiagnostic) -> str:
    return row.path if not row.line else f"{row.path}:{row.line}"


def _fitting_rows(rows: list[MemoryDiagnostic]) -> tuple[list[MemoryDiagnostic], dict[str, int]]:
    """Choose the diagnostics whose fields fit their shared placeholder budgets.

    Each placeholder budget bounds the sum over every rendered row, so a long
    list cannot be shown whole. The first diagnostic of each rule comes first,
    then repeats; the rest are counted by rule for the agent to list itself.
    """
    budgets = {
        field: budget_for(f"template_data.lifecycle-render_validation_feedback_02-{field}") for field in _ROW_FIELDS
    }
    budgets["hint"] = budget_for("template_data.lifecycle-render_validation_feedback_03-hint")
    used = dict.fromkeys(budgets, 0)
    seen: set[str] = set()
    firsts, repeats = [], []
    for row in rows:
        (repeats if row.rule in seen else firsts).append(row)
        seen.add(row.rule)
    shown: list[MemoryDiagnostic] = []
    omitted: dict[str, int] = {}
    for row in firsts + repeats:
        location = _location(row)
        sizes = {
            "rule": len(row.rule),
            "value": len(location) + 2 if location else 0,
            "message": len(row.message),
            "hint": len(row.hint),
        }
        if all(used[field] + size <= budgets[field] for field, size in sizes.items()):
            for field, size in sizes.items():
                used[field] += size
            shown.append(row)
        else:
            omitted[row.rule] = omitted.get(row.rule, 0) + 1
    order = {id(row): index for index, row in enumerate(rows)}
    shown.sort(key=lambda row: order[id(row)])
    return shown, omitted
