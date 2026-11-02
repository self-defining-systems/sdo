"""The runtime image can reach a memory upstream over ssh as the uid every SDO pod runs as."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
OPERATOR = REPO_ROOT / "sdo" / "operator"


def test_runtime_base_ships_ssh_and_a_passwd_entry_for_the_pod_uid() -> None:
    dockerfile = (OPERATOR / "Dockerfile.runtime").read_text(encoding="utf-8")
    base = dockerfile.split(" AS runtime-base\n", 1)[1].split("\nFROM ", 1)[0]
    resources = (OPERATOR / "install" / "resources.go").read_text(encoding="utf-8")
    match = re.search(r"user := int64\((\d+)\)", resources)
    assert match, "install pod security no longer declares one numeric uid"
    uid = match.group(1)
    assert "openssh-client" in base
    # OpenSSH refuses to run for a uid without a passwd entry ("No user exists for uid").
    assert re.search(rf":x:{uid}:{uid}:", base), f"runtime-base lacks a passwd entry for pod uid {uid}"
