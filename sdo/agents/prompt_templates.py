"""Strict, byte-preserving Markdown templates and measurable production prompt parts."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING, Any

from libs.agent_cli.structured import AgentProvider, StructuredTurn
from libs.agent_cli.structured import run_structured_turn as _run_structured_turn
from sdo.agents.context_output import budget_for, cap_text

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass(frozen=True)
class PromptPart:
    name: str
    text: str


class RenderedPrompt(str):
    """A normal prompt string with the ordered parts that produced its exact bytes."""

    parts: tuple[PromptPart, ...]
    context_files: tuple[Path, ...]

    def __new__(cls, parts: Iterable[PromptPart]) -> RenderedPrompt:
        raw = tuple(parts)
        grouped: dict[str, str] = defaultdict(str)
        for part in raw:
            grouped[part.name] += part.text
        capped = {name: cap_text(text, budget_for(name)) for name, text in grouped.items()}
        emitted: set[str] = set()
        result: list[PromptPart] = []
        for part in raw:
            if capped[part.name] == grouped[part.name]:
                result.append(part)
            elif part.name not in emitted:
                result.append(PromptPart(part.name, capped[part.name]))
                emitted.add(part.name)
        ordered = tuple(result)
        value = super().__new__(cls, "".join(part.text for part in ordered))
        value.parts = ordered
        value.context_files = tuple(
            sorted(
                {
                    path
                    for text in (*[part.text for part in raw], *capped.values())
                    for path in getattr(text, "context_files", ())
                }
            )
        )
        return value

    @property
    def final(self) -> str:
        return str(self)

    def __add__(self, other: str) -> RenderedPrompt:
        return RenderedPrompt(
            (
                *self.parts,
                *(
                    other.parts
                    if isinstance(other, RenderedPrompt)
                    else named_prompt(self.parts[0].name.split(".", 1)[0] + ".incident_data", other).parts
                ),
            )
        )

    def __radd__(self, other: str) -> RenderedPrompt:
        return RenderedPrompt(
            (
                *(
                    other.parts
                    if isinstance(other, RenderedPrompt)
                    else named_prompt(self.parts[0].name.split(".", 1)[0] + ".incident_data", other).parts
                ),
                *self.parts,
            )
        )


def named_prompt(name: str, text: str) -> RenderedPrompt:
    """Name an opaque rendered data section, including its separators."""
    return RenderedPrompt((PromptPart(name, text),))


def count_chars(text: str) -> int:
    """Count Unicode code points, preserving CRLF and every production separator."""
    return len(text)


def render_prompt(role: str, name: str, /, *, part: str, **values: object) -> RenderedPrompt:
    """Read a sibling prompts/*.md file; missing substitutions always raise KeyError.

    Nested rendered values retain their own part names. Other substitutions are
    data belonging to this role; literal template text belongs to ``part``.
    Binary reading avoids silently normalizing CRLF before measuring the prompt.
    """
    template = Template((Path(__file__).parent / role / "prompts" / f"{name}.md").read_bytes().decode("utf-8"))
    manifest = json.loads(Path(__file__).with_name("prompt_sections.json").read_text())
    configured = manifest.get(f"{role}/{name}")
    identifiers = {
        match.group("named") or match.group("braced")
        for match in template.pattern.finditer(template.template)
        if match.group("named") or match.group("braced")
    }
    if configured is None or set(configured) != identifiers:
        raise ValueError(f"Missing prompt placeholder budget entries: {role}/{name}")
    for budget_name in configured.values():
        budget_for(budget_name)
    template.substitute(values)  # validate missing placeholders before writing evidence
    parts: list[PromptPart] = []
    cursor = 0
    for match in template.pattern.finditer(template.template):
        if match.start() > cursor:
            parts.append(PromptPart(part, template.template[cursor : match.start()]))
        key = match.group("named") or match.group("braced")
        if key is not None:
            value = values[key]
            if isinstance(value, RenderedPrompt):
                parts.extend(value.parts)
            else:
                parts.append(PromptPart(configured[key], str(value)))
        elif match.group("escaped") is not None:
            parts.append(PromptPart(part, "$"))
        cursor = match.end()
    if cursor < len(template.template):
        parts.append(PromptPart(part, template.template[cursor:]))
    rendered = RenderedPrompt(parts)
    return rendered


def join_prompts(separator: str, prompts: Iterable[str], *, part: str) -> RenderedPrompt:
    """Join sections without losing their provenance or separators."""
    parts: list[PromptPart] = []
    for index, prompt in enumerate(prompts):
        if index:
            parts.append(PromptPart(part, separator))
        parts.extend(prompt.parts if isinstance(prompt, RenderedPrompt) else (PromptPart(part, prompt),))
    return RenderedPrompt(parts)


def run_agent_turn(provider: AgentProvider, prompt: str, **options: Any) -> StructuredTurn:
    """Only prompts carrying capped production provenance may reach a provider."""
    if not isinstance(prompt, RenderedPrompt):
        raise TypeError("agent prompts must use the capped renderer")
    return _run_structured_turn(provider, RenderedPrompt(prompt.parts), **options)
