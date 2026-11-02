"""Read the complete evidence that bounded production context explicitly references."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any


def expand_context(text: str) -> str:
    """Follow only the renderer's explicit pointers, never guess missing evidence."""

    def expand(value: str, visited: frozenset[str]) -> str:
        def replace(match: re.Match[str]) -> str:
            path = os.path.expandvars(match[1])
            if path in visited:
                return match[0]
            return expand(Path(path).read_text(), visited | {path}) + "\n"

        return re.sub(r"Full: ([^\s]+\.txt)\n", replace, value)

    return expand(str(text), frozenset())


def incident_document(text: str, *, full: bool = False) -> dict[str, Any]:
    if full:
        text = expand_context(text)
    for marker in (
        ("Incident request:\n", "Summary (partial):\n") if full else ("Summary (partial):\n", "Incident request:\n")
    ):
        if marker in text:
            value = text.split(marker, 1)[1]
            if value.lstrip().startswith("{"):
                return json.JSONDecoder().raw_decode(value.lstrip())[0]
    if not full:
        return incident_document(text, full=True)
    raise AssertionError("No complete incident document or generated JSON summary")


def incident_facts_path(text: str) -> Path:
    document = incident_document(text, full=True)
    return Path(document["details_file"])
