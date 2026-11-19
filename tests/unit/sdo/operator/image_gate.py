"""The controller image's validation smoke gate as ``scripts/build_sdo_images.sh`` runs it."""

from __future__ import annotations

import re
import shlex
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]


def controller_validation_gate(script: str) -> tuple[list[str], dict[str, Path]]:
    """The ``sdo-operator`` argv after the image, and its volume mounts (container path -> repository path)."""
    lines = script.splitlines()
    (at,) = [i for i, line in enumerate(lines) if '"${image}:${tag}" sdo-operator' in line]
    argv = shlex.split(lines[at].split('"${image}:${tag}"', 1)[1])
    mounts: dict[str, Path] = {}
    i = at - 1
    while lines[i].rstrip().endswith("\\") and "docker run" not in lines[i]:
        for spec in re.findall(r'--volume "\$\{repo_root\}/([^:"]+):([^:"]+)(?::ro)?"', lines[i]):
            mounts[spec[1]] = REPO_ROOT / spec[0]
        i -= 1
    return argv, mounts
