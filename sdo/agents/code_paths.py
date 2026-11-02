"""Built-in agent guidance selection; broker launch policy remains authoritative."""

from __future__ import annotations

import os
from pathlib import Path
from types import MappingProxyType
from typing import Literal, cast

SKILLS_ROOT = Path(__file__).resolve().parents[1] / "skills"


def agent_guidance() -> Literal["go", "shell"]:
    selected = os.environ.get("SDO_AGENT_GUIDANCE") or os.environ.get("SDO_CODE_PATH") or "go"
    if selected not in {"go", "shell"}:
        raise ValueError(f"unsupported agent guidance {selected!r}")
    return cast("Literal['go', 'shell']", selected)


AGENT_SKILLS = MappingProxyType(
    {
        "responder": ("sdo-memory", "sdo-incident-response"),
        "reflector": ("sdo-memory", "sdo-detector-authoring", "sdo-playbook-authoring"),
        "health-judge": ("sdo-memory", "sdo-detector-authoring", "sdo-playbook-authoring"),
        "deployer": ("sdo-memory",),
    }
)


def skills_for_role(role: str) -> tuple[str, ...]:
    if agent_guidance() == "shell" and role != "deployer":
        return ("sdo-shell-authoring",)
    return AGENT_SKILLS[role]
