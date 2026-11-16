"""Sweep-by-kind table: one row per fault (first try, then what its repeats did), summarized by fault kind.

Reads the same manifest as ``learning_economics`` (``fault``, ``kind``, ``first``, ``repeats``, optional
``screen`` and ``note``; the note carries a hand-written miss class). Seconds are after inject.
"""

# ruff: noqa: E501  (markdown table rows are one line each)
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from benchmarks.sregym.analysis.cells import Cell, delta, load_cell

if TYPE_CHECKING:
    from pathlib import Path


def _fmt(value: float | None) -> str:
    return "-" if value is None else f"{value:.0f}"


def _repeat(cell: Cell) -> str:
    i = cell.first_incident
    if i is None:
        return f"{cell.name}: no incident, {cell.window_bad_seconds} bad s"
    if i.applied_playbook_ids and not i.used_responder:
        tag = "YES (" + ",".join(i.applied_playbook_ids) + ")"
    elif i.applied_playbook_ids:
        tag = "playbook then responder"
    else:
        tag = "NO (no playbook applied)"
    page = delta(i.detected_at, cell.inject_at)
    closed = delta(i.verified_at, cell.inject_at)
    return f"{cell.name}: {tag}, page {_fmt(page)}, closed {_fmt(closed)}, {i.classification}, {cell.window_bad_seconds} bad s, {i.responder.tokens} tok"


def rows(manifest: list[dict[str, Any]], pool: Path) -> list[dict[str, Any]]:
    out = []
    for entry in manifest:
        path = pool / entry["first"] if entry.get("first") else None
        first = load_cell(path) if path and path.exists() else None
        i = first.first_incident if first else None
        out.append(
            {
                "fault": entry["fault"],
                "kind": entry.get("kind", "unknown"),
                "screen": entry.get("screen", ""),
                "note": entry.get("note", ""),
                "ran": first is not None,
                "incident": i is not None,
                "paged_by": ",".join(i.opened_by) if i else "none",
                "page_s": delta(i.detected_at, first.inject_at) if i and first else None,
                "page_to_first_change_s": i.page_to_first_change if i else None,
                "fixed_in_window": first.recovered_in_window if first else None,
                "bad_seconds": first.window_bad_seconds if first else None,
                "outcome": i.classification if i else None,
                "learned": bool(i and i.learned_playbook_ids),
                "cost_usd": i.responder.cost_usd if i else None,
                "repeats": [_repeat(load_cell(pool / r)) for r in entry.get("repeats", []) if (pool / r).exists()],
                "repeat_served": [
                    bool(
                        (c := load_cell(pool / r)).first_incident
                        and c.first_incident.applied_playbook_ids
                        and not c.first_incident.used_responder
                    )
                    for r in entry.get("repeats", [])
                    if (pool / r).exists()
                ],
            }
        )
    return out


def render(table: list[dict[str, Any]]) -> str:
    lines = [
        "| fault | kind | screen | paged by | page | page to fix | fixed in window | outcome | learned | repeats | cost first try | note |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in table:
        if not r["ran"]:
            lines.append(
                f"| {r['fault']} | {r['kind']} | {r['screen']} | - | - | - | - | - | - | - | - | {r['note'] or 'not run'} |"
            )
            continue
        fixed = (
            "-"
            if r["fixed_in_window"] is None
            else ("yes" if r["fixed_in_window"] else "NO") + f" ({r['bad_seconds']} bad s)"
        )
        cost = "-" if not r["cost_usd"] else f"${r['cost_usd']:.2f}"
        lines.append(
            f"| {r['fault']} | {r['kind']} | {r['screen']} | {r['paged_by']} | {_fmt(r['page_s'])} | {_fmt(r['page_to_first_change_s'])} | {fixed} "
            f"| {r['outcome'] or '-'} | {'yes' if r['learned'] else 'no'} | {'; '.join(r['repeats']) or '-'} | {cost} | {r['note']} |"
        )
    lines += [
        "",
        "## Summary by kind",
        "",
        "| kind | faults run | incident opened | fixed in window | learned | repeats served by playbook |",
        "|---|---|---|---|---|---|",
    ]
    for kind in sorted({r["kind"] for r in table}):
        ran = [r for r in table if r["kind"] == kind and r["ran"]]
        served = [s for r in ran for s in r["repeat_served"]]
        lines.append(
            f"| {kind} | {len(ran)} | {sum(r['incident'] for r in ran)} | {sum(bool(r['fixed_in_window']) for r in ran)} "
            f"| {sum(r['learned'] for r in ran)} | {sum(served)} of {len(served)} |"
        )
    return "\n".join(lines) + "\n"
