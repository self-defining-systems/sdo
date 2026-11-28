"""Bytes an onboarding agent reads before writing its first probe: the skill and the docs it links.

``SKILL.md`` alone is the floor. Recorded sessions also opened every reference the skill's
numbered steps link (worked example, probe design, map reference) in their first requests, so the
report also gives the skill plus the Markdown files it links, one level deep.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path  # noqa: TC003

LINK = re.compile(r"\]\(([^)#\s]+\.md)(?:#[^)]*)?\)")


@dataclass(frozen=True)
class FirstRead:
    skill: int
    linked: dict[str, int]

    @property
    def total(self) -> int:
        return self.skill + sum(self.linked.values())

    def text(self) -> str:
        lines = [f"first read: SKILL.md {self.skill} bytes; with linked references {self.total} bytes"]
        lines += [f"  {name} {size}" for name, size in self.linked.items()]
        return "\n".join(lines) + "\n"


def first_read(skill: Path) -> FirstRead:
    """Sizes of skill (a SKILL.md) and of each distinct Markdown file it links."""
    text = skill.read_text()
    linked: dict[str, int] = {}
    for target in LINK.findall(text):
        path = (skill.parent / target).resolve()
        if path.is_file():
            linked.setdefault(str(path.relative_to(skill.parent.parent.resolve())), path.stat().st_size)
    return FirstRead(len(text.encode()), linked)
