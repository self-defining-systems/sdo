"""Row-level harness outcomes, interpreted before any agent-credit requirements."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping


def infrastructure_error(row: Mapping[str, str | None]) -> str | None:
    value = str(row.get("infra_error") or "").strip()
    return value if value.lower() not in {"", "none", "null", "false", "0"} else None


def has_injection_evidence(row: Mapping[str, str | None]) -> bool:
    # The conductor flattens its Injection object. Accept a nested object from older exporters too.
    if any(key.startswith("Injection.") and str(value or "").strip() for key, value in row.items()):
        return True
    for key in ("Injection", "injection_evidence"):
        try:
            value = json.loads(str(row.get(key) or "null"))
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value:
            return True
    return False
