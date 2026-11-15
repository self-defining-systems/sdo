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
        "responder": (
            "incident-response",
            "sdo-operating",
            "sdo-serving",
            "sdo-observability",
            "sdo-operations-map",
            "sdo-investigator",
        ),
        "investigator": ("sdo-investigator",),
        "reflector": ("playbook-writing",),
        "onboarding": ("sdo-onboarding", "sdo-serving", "sdo-observability", "sdo-operations-map", "sdo-detectors"),
        "health-judge": (
            "sdo-memory",
            "sdo-detectors",
            "sdo-playbook-authoring",
            "sdo-operations-map",
            "sdo-observability",
        ),
        "source_deployment": ("sdo-memory", "sdo-serving"),
    }
)


def skills_for_role(role: str) -> tuple[str, ...]:
    if agent_guidance() == "shell" and role not in {"onboarding", "source_deployment", "reflector", "investigator"}:
        return ("sdo-shell-authoring",)
    return AGENT_SKILLS[role]
