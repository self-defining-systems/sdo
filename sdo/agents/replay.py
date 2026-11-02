"""Trusted replay policy and the matching skill-text projection."""

from __future__ import annotations

import os
import re
import shutil
import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Literal, cast


def replay_mode() -> Literal["on", "off"]:
    value = os.environ.get("SDO_REPLAY") or "on"
    if value not in {"on", "off"}:
        raise ValueError(f"replay must be on or off, got {value!r}")
    return cast("Literal['on', 'off']", value)


def selected_skills_root(root: Path) -> Path:
    """Only replay-on workers receive replay-specific authoring instructions."""
    return root if replay_mode() == "on" else _project(root, "off")


@lru_cache(maxsize=4)
def _project(root: Path, mode: str) -> Path:
    destination = Path(tempfile.mkdtemp(prefix=f"sdo-skills-{mode}-"))
    shutil.copytree(root, destination, dirs_exist_ok=True)
    for path in destination.rglob("*.md"):
        text = path.read_text()
        for selected in ("on", "off"):
            pattern = rf"<!-- replay:{selected} -->\n?(.*?)<!-- /replay:{selected} -->"
            text = re.sub(pattern, r"\1" if selected == mode else "", text, flags=re.DOTALL)
        path.write_text(text)
    return destination
