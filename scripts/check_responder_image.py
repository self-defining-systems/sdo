#!/usr/bin/env python3
"""Reject answer-key paths in the benchmark responder's file inventory.

CI pipes ``find /opt/sdo/benchmarks -type f`` here. The Dockerfile scope tests
share this policy, including the exact recording-module exceptions.
"""

from __future__ import annotations

import re
import sys
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

ALLOWED_FASTLOOP_FILES = frozenset(
    f"benchmarks/sregym/fastloop/{module}.py"
    for module in ("__init__", "model_recording", "file_edits", "recording_worker", "checkpoint_limits")
)
FORBIDDEN_DIRECTORIES = frozenset(
    {"experiments", "assurance", "fastloop", "analysis", "runner", "agents", "docs", "tests"}
)
_BYTECODE = re.compile(r"(.+)\.cpython-[0-9]+(?:\.opt-[0-9]+)?\.pyc")


def forbidden_image_paths(paths: Iterable[str]) -> list[str]:
    """Return forbidden inventory entries, allowing only known recording source/bytecode."""
    leaked = []
    for raw in paths:
        path = PurePosixPath(raw.strip())
        relative = path.relative_to("/opt/sdo") if path.is_relative_to("/opt/sdo") else path
        source = relative
        if relative.parent.name == "__pycache__" and (match := _BYTECODE.fullmatch(relative.name)):
            source = relative.parent.parent / f"{match[1]}.py"
        if FORBIDDEN_DIRECTORIES.intersection(relative.parts[:-1]) and str(source) not in ALLOWED_FASTLOOP_FILES:
            leaked.append(str(path))
    return sorted(leaked)


def main() -> int:
    paths = [line.strip() for line in sys.stdin if line.strip()]
    if not paths:
        print("empty responder image inventory; check the image listing command", file=sys.stderr)
        return 1
    leaked = forbidden_image_paths(paths)
    if leaked:
        print("answer-key paths present in responder image:", *leaked, sep="\n", file=sys.stderr)
        return 1
    print("responder image inventory contains no answer-key paths")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
