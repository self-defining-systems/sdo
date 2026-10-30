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
from sdo.agents.context_output import ContextText, budget_for, cap_text, save_context

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass(frozen=True)
class PromptPart:
    name: str
    text: str
    required: bool = True
    priority: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not isinstance(self.text, str):
            raise TypeError("prompt part name and text must be strings")
        if type(self.required) is not bool or type(self.priority) is not int:
            raise TypeError("required must be bool and priority must be int")
        if not self.required and self.name.endswith(".instructions"):
            raise ValueError(f"{self.name}: instructions are always required")


class PromptBudgetError(ValueError):
    """A named budget cannot retain the required prompt content."""


def cap_section(name: str, text: str) -> str:
    """The one place optional prompt context meets its registered budget."""
    return cap_text(text, budget_for(name))


def _omission(characters: int) -> str:
    return f"\n[Optional content omitted: {characters} characters]\n"


class RenderedPrompt(str):
    """A normal prompt string with the ordered parts that produced its exact bytes."""

    parts: tuple[PromptPart, ...]
    context_files: tuple[Path, ...]

    def __new__(cls, parts: Iterable[PromptPart]) -> RenderedPrompt:
        raw = tuple(parts)
        grouped: dict[str, list[tuple[int, PromptPart]]] = defaultdict(list)
        for index, part in enumerate(raw):
            grouped[part.name].append((index, part))
        replacements: dict[int, str] = {}
        for name, sections in grouped.items():
            limit = budget_for(name)
            required_size = sum(len(section.text) for _, section in sections if section.required)
            if required_size > limit:
                raise PromptBudgetError(
                    f"{name}: required content is {required_size} characters, budget is {limit}; "
                    "shorten the required content or raise its budget in a budget-only commit"
                )
            remaining = limit - required_size
            # Reserve required bytes first, then optional groups in priority order.
            optional: dict[int, list[tuple[int, PromptPart]]] = defaultdict(list)
            for index, section in sections:
                if not section.required:
                    optional[section.priority].append((index, section))
            groups = [optional[priority] for priority in sorted(optional, reverse=True)]
            for position, fragments in enumerate(groups):
                text = "".join(fragment.text for _, fragment in fragments)
                reserve = sum(
                    len(_omission(sum(len(fragment.text) for _, fragment in later))) for later in groups[position + 1 :]
                )
                available = max(0, remaining - reserve)
                if len(text) <= available:
                    remaining -= len(text)
                    continue
                try:
                    replacement = cap_section(name, text) if available == limit else cap_text(text, available)
                except ValueError as exc:
                    omission = _omission(len(text))
                    if isinstance(exc, PromptBudgetError):
                        raise
                    if len(omission) > available:
                        raise PromptBudgetError(f"{name}: required content cannot fit: {exc}") from exc
                    path, _ = save_context(text)
                    replacement = ContextText(omission, path, degraded=True)
                replacements[fragments[0][0]] = replacement
                for index, _ in fragments[1:]:
                    replacements[index] = ""
                remaining -= len(replacement)
        result = [
            PromptPart(part.name, replacements.get(index, part.text), part.required, part.priority)
            for index, part in enumerate(raw)
        ]
        ordered = tuple(result)
        value = super().__new__(cls, "".join(part.text for part in ordered))
        value.parts = ordered
        value.context_files = tuple(
            sorted(
                {
                    path
                    for text in (*[part.text for part in raw], *replacements.values())
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


def named_prompt(name: str, text: str, *, required: bool = True, priority: int = 0) -> RenderedPrompt:
    """Name data; optional sections must opt into ranked, counted summarization.

    A wrapper cannot erase the requirements or provenance of a rendered value.
    """
    if isinstance(text, RenderedPrompt):
        return RenderedPrompt(text.parts)
    return RenderedPrompt((PromptPart(name, text, required, priority),))


def count_chars(text: str) -> int:
    """Count Unicode code points, preserving CRLF and every production separator."""
    return len(text)


def render_prompt(
    role: str, name: str, /, *, part: str, optional_values: tuple[str, ...] = (), **values: object
) -> RenderedPrompt:
    """Read a sibling prompts/*.md file; missing substitutions always raise KeyError.

    Nested rendered values retain their own part names. Other substitutions are
    data belonging to this role; literal template text belongs to ``part``.
    ``optional_values`` marks summarizable runtime data, retaining each manifest
    budget. Embedded rendered values always preserve their own requirements.
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
    if not set(optional_values) <= identifiers:
        raise ValueError(f"Unknown optional prompt placeholders: {role}/{name}")
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
                parts.append(PromptPart(configured[key], str(value), required=key not in optional_values))
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
