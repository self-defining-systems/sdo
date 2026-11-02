"""The standard release of a stalled rollout, as the command that applies it."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


def _ref(ref: Mapping[str, Any]) -> str:
    return "/".join(str(ref[part]) for part in ("kind", "namespace", "name") if ref.get(part))


def release_lines(pending: Iterable[Mapping[str, Any]]) -> list[str]:
    """One line per typed release in Go-owned rollout_pending documents, without repeats."""
    lines: list[str] = []
    for document in pending:
        for blocker in document.get("blockers") or []:
            for action in blocker.get("release") or []:
                retire = action.get("retire_old_revision")
                if not retire:
                    continue
                line = (
                    f"{_ref(blocker.get('ref') or {})} cannot roll out beside its old revision: "
                    f"sdo mitigate RetireOldRevision --ref {_ref(retire.get('ref') or {})}"
                )
                if line not in lines:
                    lines.append(line)
    return lines
