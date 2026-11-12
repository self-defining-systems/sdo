"""The late-findings pull is removed: nothing sets or reads it."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_PATTERN = re.compile(r"late[_-]findings|LATE_FINDINGS|LateFindings", re.IGNORECASE)
# The retired proto field name stays reserved so stored data cannot reuse it.
_ALLOWED = {
    "sdo/operator/contracts/wire/retired.go",
    "sdo/contracts/proto/sdodev/contracts/v1alpha1/messages.proto",
    "sdo/operator/contracts/gen/sdodev/contracts/v1alpha1/messages.pb.go",
    "sdo/contracts/_gen/sdodev/contracts/v1alpha1/messages_pb2.py",
    "sdo/contracts/_gen/sdodev/contracts/v1alpha1/messages_pb2.pyi",
    "tests/unit/test_late_findings_removed.py",
}


def test_nothing_sets_or_reads_late_findings() -> None:
    files = (
        subprocess.run(["git", "ls-files", "-z"], cwd=_ROOT, capture_output=True, check=True)
        .stdout.decode()
        .split("\0")
    )
    offenders = []
    for name in files:
        if not name or name in _ALLOWED or name.startswith("third_party/"):
            continue
        path = _ROOT / name
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if _PATTERN.search(text):
            offenders.append(name)
    assert offenders == []
