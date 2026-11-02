"""Recovery admission for fresh and cached native reflection-drain evidence."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping


def require_terminal_drain(evidence: Mapping[str, object]) -> None:
    """An interrupted, missing or indeterminate drain can never admit recovery.

    Call this on cached evidence too. Terminal decisions and acknowledgement
    still come from the native lifecycle; this gate cannot manufacture either.
    """
    if evidence.get("terminal") is not True or evidence.get("interrupted") is not False:
        raise RuntimeError(
            "eval: reflection drain interrupted or incomplete; recovery requires durable terminal reflection"
        )
