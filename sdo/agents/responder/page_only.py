"""The page-only responder (``response.page_only``): a plain agent that gets only the incident page.

SDO detects, pages and verifies as usual. The agent works in an empty directory with the same cluster access,
no ``sdo`` command on its PATH, no skills and no memory checkout, and is told the page as plain text. It is the
reference for what SDO's page is worth without SDO's tools and memory.
"""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from sdo.agents.prompt_templates import render_prompt
from sdo.agents.responder.incident_opening import symptom

if TYPE_CHECKING:
    from sdo.contracts import IncidentRequest

#: The worker environment variable the Go operator sets from ``response.page_only``.
ENVIRONMENT = "SDO_RESPONDER_PAGE_ONLY"

_SDO = re.compile(r"\bsdo(?:-[a-z]+)?\b")
#: A pointer to a command ("more: sdo changes ...") through the end of its line.
_MORE = re.compile(r"\s*(?:more:\s*)?(?:sdo\b[^\n]*)$")
_SENTENCE = re.compile(r"\s*(?:Re-check one with )?`sdo[^`]*`\.?")
_COLON = re.compile(r":\s*sdo\b[^)\n]*")


def enabled() -> bool:
    return os.environ.get(ENVIRONMENT) == "1"


def plain_text(text: str) -> str:
    """The page with every pointer to an sdo command removed; a line that still names sdo is dropped."""
    lines: list[str] = []
    for line in text.splitlines():
        line = _SENTENCE.sub("", line)
        line = _COLON.sub("", line)
        line = _MORE.sub("", line)
        if line.strip().startswith("more:") or _SDO.search(line):
            continue
        if line.strip():
            lines.append(line.rstrip())
    return "\n".join(lines)


def page(request: IncidentRequest) -> str:
    """What SDO's responder would be told, as plain text: failing operations, recorded changes, clues, attempts."""
    parts = [plain_text(symptom(request))]
    changes = plain_text("\n".join(request.recent_changes))
    if changes:
        parts.append("Changes and clues near the onset, newest first:\n" + changes)
    if request.prior_response_progress:
        attempts = [
            f"- {action.kind} {action.target}"
            for progress in request.prior_response_progress
            for action in progress.successful_actions
        ]
        if attempts:
            parts.append("Earlier attempts on this problem (they did not hold):\n" + plain_text("\n".join(attempts)))
    return "\n\n".join(parts)


def prompt(request: IncidentRequest, branch_instruction: str) -> str:
    return render_prompt(
        "responder",
        "incident_page",
        part="responder.instructions",
        namespace=request.namespace,
        page=page(request),
        branch_instruction=branch_instruction,
    )


def scratch_directory() -> Path:
    """An empty working directory: no repository, no memory, no instructions."""
    return Path(tempfile.mkdtemp(prefix="page-only-"))


def path_without_sdo(path: str, directory: Path) -> str:
    """A PATH that holds every other command of ``path`` and none named sdo or sdo-*."""
    directory.mkdir(parents=True, exist_ok=True)
    for entry in path.split(os.pathsep):
        root = Path(entry)
        if not entry or not root.is_dir():
            continue
        for item in root.iterdir():
            if item.name == "sdo" or item.name.startswith("sdo-"):
                continue
            link = directory / item.name
            if not link.exists() and not link.is_symlink():
                link.symlink_to(item)
    return str(directory)
