"""Built-in agent guidance selection; broker launch policy remains authoritative."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal, cast

SKILLS_ROOT = Path(__file__).resolve().parents[1] / "skills"


def agent_guidance() -> Literal["go", "shell"]:
    selected = os.environ.get("SDO_AGENT_GUIDANCE") or os.environ.get("SDO_CODE_PATH") or "go"
    if selected not in {"go", "shell"}:
        raise ValueError(f"unsupported agent guidance {selected!r}")
    return cast("Literal['go', 'shell']", selected)
