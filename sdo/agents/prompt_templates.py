"""Strict, byte-preserving Markdown templates and measurable production prompt parts."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING

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
    template.substitute(values)  # validate missing placeholders before writing evidence
    parts: list[PromptPart] = []
    cursor = 0
    role = part.split(".", 1)[0]
    for match in template.pattern.finditer(template.template):
        if match.start() > cursor:
            parts.append(PromptPart(part, template.template[cursor : match.start()]))
        key = match.group("named") or match.group("braced")
        if key is not None:
            value = values[key]
            if isinstance(value, RenderedPrompt):
                parts.extend(value.parts)
            else:
                parts.append(PromptPart(f"{role}.incident_data", str(value)))
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


def prompt_prefix(prompt: str, limit: int) -> str:
    """Clip an ordered prompt to code points without throwing away its part names."""
    if not isinstance(prompt, RenderedPrompt):
        return prompt[:limit]
    parts: list[PromptPart] = []
    remaining = limit
    for part in prompt.parts:
        if remaining <= 0:
            break
        parts.append(PromptPart(part.name, part.text[:remaining]))
        remaining -= len(part.text)
    return RenderedPrompt(parts)
