"""Learning economics per fault: what the first try and its closing turn cost, and what each repeat saves.

Per fault the first try gives responder tokens/cost, page to first agent change, page to recovered and
the closing turn (reflection) tokens/cost/duration and the playbook ids it committed. Each repeat gives
page to playbook applied, applied to recovered, responder tokens/cost (zero when the playbook served it),
page to recovered and bad user seconds. Aggregates: saved cost and time per recurrence, break-even
recurrences (closing-turn cost / saving per recurrence), and the median repeat/first ratios.

*Recovered* is the closure's ``verified_at`` (alert set clear and stable). A repeat that opened no
incident has no page time and is excluded from the ratios (counted as ``missed``).

Cells come from a manifest, a JSON list of ``{"fault", "kind", "first", "repeats": [...]}`` whose cell
names resolve under ``--pool``; without one, cells named ``<fault>-<arm>-<rev>`` are grouped by fault
and arm ``a`` is the first try. Missing values print as ``?`` and are listed per row.
"""

# ruff: noqa: E501  (markdown table rows are one line each)
from __future__ import annotations

import json
import re
import statistics
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.analysis.cells import Cell, Incident, load_cell

if TYPE_CHECKING:
    from pathlib import Path

_NAME = re.compile(r"^(?P<fault>.+)-(?P<arm>[a-z]\d*)-[0-9a-f]{6,}$")


@dataclass
class Row:
    cell: str
    incident: Incident | None
    bad_seconds: int | None
    recovered_in_window: bool | None

    @property
    def served_by_playbook(self) -> bool:
        i = self.incident
        return bool(i and i.applied_playbook_ids and not i.used_responder)

    @property
    def cost(self) -> float | None:
        i = self.incident
        if i is None:
            return None
        return i.responder.cost_usd or 0.0


@dataclass
class Fault:
    fault: str
    kind: str
    first: Row
    repeats: list[Row] = field(default_factory=list)

    @property
    def usable_repeats(self) -> list[Row]:
        return [r for r in self.repeats if r.incident and r.incident.page_to_recovered is not None]

    def _ratio(self, get: Any) -> float | None:
        base = get(self.first)
        vals = [get(r) for r in self.usable_repeats]
        vals = [v for v in vals if v is not None]
        if not base or not vals:
            return None
        return statistics.median(vals) / base

    @property
    def time_ratio(self) -> float | None:
        return self._ratio(lambda r: r.incident.page_to_recovered if r.incident else None)

    @property
    def cost_ratio(self) -> float | None:
        return self._ratio(lambda r: r.cost)

    @property
    def saved_cost(self) -> float | None:
        base = self.first.cost
        vals = [r.cost for r in self.usable_repeats if r.cost is not None]
        return None if base is None or not vals else base - statistics.median(vals)

    @property
    def saved_seconds(self) -> float | None:
        base = self.first.incident.page_to_recovered if self.first.incident else None
        vals = [
            r.incident.page_to_recovered
            for r in self.usable_repeats
            if r.incident and r.incident.page_to_recovered is not None
        ]
        return None if base is None or not vals else base - statistics.median(vals)

    @property
    def learn_cost(self) -> float | None:
        i = self.first.incident
        return None if i is None else i.reflection.cost_usd

    @property
    def break_even(self) -> float | None:
        """Recurrences needed for the saving to repay the closing turn; ``None`` when there is no saving."""
        saved = self.saved_cost
        if self.learn_cost is None or saved is None or saved <= 0:
            return None
        return self.learn_cost / saved


def _row(cell: Cell) -> Row:
    return Row(cell.name, cell.first_incident, cell.window_bad_seconds, cell.recovered_in_window)


def _load(pool: Path, name: str) -> Row:
    return _row(load_cell(pool / name))


def faults_from_manifest(manifest: list[dict[str, Any]], pool: Path) -> list[Fault]:
    out = []
    for entry in manifest:
        if not entry.get("first") or not (pool / entry["first"]).exists():
            continue
        repeats = [_load(pool, r) for r in entry.get("repeats", []) if (pool / r).exists()]
        out.append(Fault(entry["fault"], entry.get("kind", "unknown"), _load(pool, entry["first"]), repeats))
    return out


def faults_from_cells(cells: list[Path], first_arm: str = "a") -> list[Fault]:
    groups: dict[str, dict[str, list[Path]]] = {}
    for path in cells:
        m = _NAME.match(path.name)
        if m:
            groups.setdefault(m["fault"], {"first": [], "repeat": []})[
                "first" if m["arm"] == first_arm else "repeat"
            ].append(path)
    return [
        Fault(fault, "unknown", _row(load_cell(g["first"][0])), [_row(load_cell(p)) for p in sorted(g["repeat"])])
        for fault, g in sorted(groups.items())
        if g["first"]
    ]


def _s(value: float | None, fmt: str = "{:.0f}") -> str:
    return "?" if value is None else fmt.format(value)


def _usd(value: float | None) -> str:
    return "?" if value is None else f"${value:.2f}"


def _tok(value: int | None) -> str:
    return "?" if value is None else f"{value / 1e6:.2f}M"


def aggregate(faults: list[Fault]) -> dict[str, Any]:
    def med(vals: list[float | None]) -> float | None:
        clean = [v for v in vals if v is not None]
        return statistics.median(clean) if clean else None

    def block(items: list[Fault]) -> dict[str, Any]:
        reps = [r for f in items for r in f.usable_repeats]
        return {
            "faults": sum(f.first.incident is not None for f in items),
            "repeats": sum(len(f.repeats) for f in items),
            "repeats_missed": sum(len(f.repeats) - len(f.usable_repeats) for f in items),
            "repeats_served_by_playbook": sum(r.served_by_playbook for r in reps),
            "first_cost_usd": sum(f.first.cost or 0.0 for f in items),
            "learn_cost_usd": sum(f.learn_cost or 0.0 for f in items),
            "median_time_ratio": med([f.time_ratio for f in items]),
            "median_cost_ratio": med([f.cost_ratio for f in items]),
            "median_saved_cost_usd": med([f.saved_cost for f in items]),
            "median_saved_seconds": med([f.saved_seconds for f in items]),
            "median_break_even_recurrences": med([f.break_even for f in items]),
            "never_breaks_even": sum(
                f.break_even is None and f.learn_cost is not None and bool(f.usable_repeats) for f in items
            ),
        }

    kinds = sorted({f.kind for f in faults})
    return {"all": block(faults), "by_kind": {k: block([f for f in faults if f.kind == k]) for k in kinds}}


def to_json(faults: list[Fault]) -> dict[str, Any]:
    def inc(r: Row) -> dict[str, Any]:
        i = r.incident
        if i is None:
            return {"cell": r.cell, "incident": False, "bad_seconds": r.bad_seconds}
        return {
            "cell": r.cell,
            "incident": True,
            "bad_seconds": r.bad_seconds,
            "classification": i.classification,
            "resolution_path": i.resolution_path,
            "responder_tokens": i.responder.tokens,
            "responder_cost_usd": r.cost,
            "page_to_first_change_s": i.page_to_first_change,
            "page_to_applied_s": i.page_to_applied,
            "applied_to_recovered_s": i.applied_to_recovered,
            "page_to_cleared_s": i.page_to_cleared,
            "page_to_recovered_s": i.page_to_recovered,
            "applied_playbooks": i.applied_playbook_ids,
            "reflection_decision": i.reflection_decision,
            "learned_playbook_ids": i.learned_playbook_ids,
            "reflection_tokens": i.reflection.tokens,
            "reflection_cost_usd": i.reflection.cost_usd,
            "reflection_seconds": i.reflection_seconds,
            "missing": i.missing,
        }

    return {
        "faults": [
            {
                "fault": f.fault,
                "kind": f.kind,
                "first": inc(f.first),
                "repeats": [inc(r) for r in f.repeats],
                "time_ratio": f.time_ratio,
                "cost_ratio": f.cost_ratio,
                "saved_cost_usd": f.saved_cost,
                "saved_seconds": f.saved_seconds,
                "break_even_recurrences": f.break_even,
            }
            for f in faults
        ],
        "aggregate": aggregate(faults),
    }


def render(faults: list[Fault]) -> str:
    lines = [
        "# Learning economics",
        "",
        "Seconds are page (incident detected) to recovered (closure verified). Cost is the responder's; learn is the closing turn.",
        "",
        "## First tries",
        "",
        "| fault | kind | responder tokens | responder cost | page to first change s | page to recovered s | learn cost | learn s | learned | bad s |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    missing: list[str] = []
    for f in faults:
        i = f.first.incident
        if i is None:
            lines.append(f"| {f.fault} | {f.kind} | no incident | | | | | | | {_s(f.first.bad_seconds)} |")
            continue
        learned = ", ".join(i.learned_playbook_ids) or i.reflection_decision or "?"
        lines.append(
            f"| {f.fault} | {f.kind} | {_tok(i.responder.tokens)} | {_usd(f.first.cost)} | {_s(i.page_to_first_change)} | {_s(i.page_to_recovered)} "
            f"| {_usd(i.reflection.cost_usd)} | {_s(i.reflection_seconds)} | {learned} | {_s(f.first.bad_seconds)} |"
        )
        missing += [f"{f.first.cell}: {m}" for m in i.missing]
    lines += [
        "",
        "## Repeats",
        "",
        "| fault | cell | path | page to applied s | applied to recovered s | page to recovered s | responder tokens | responder cost | bad s |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for f in faults:
        for r in f.repeats:
            i = r.incident
            if i is None:
                lines.append(f"| {f.fault} | {r.cell} | no incident (missed) | | | | | | {_s(r.bad_seconds)} |")
                continue
            path = (
                "playbook"
                if r.served_by_playbook
                else ("playbook then responder" if i.applied_playbook_ids else i.resolution_path or "?")
            )
            lines.append(
                f"| {f.fault} | {r.cell} | {path} | {_s(i.page_to_applied)} | {_s(i.applied_to_recovered)} | {_s(i.page_to_recovered)} "
                f"| {_tok(i.responder.tokens)} | {_usd(r.cost)} | {_s(r.bad_seconds)} |"
            )
            missing += [f"{r.cell}: {m}" for m in i.missing]
    lines += [
        "",
        "## Per fault",
        "",
        "| fault | time ratio | cost ratio | saved s | saved $ | break-even recurrences |",
        "|---|---|---|---|---|---|",
    ]
    for f in faults:
        if f.repeats:
            be = (
                "never"
                if f.break_even is None and f.learn_cost is not None and f.usable_repeats
                else _s(f.break_even, "{:.1f}")
            )
            lines.append(
                f"| {f.fault} | {_s(f.time_ratio, '{:.2f}')} | {_s(f.cost_ratio, '{:.2f}')} | {_s(f.saved_seconds)} | {_usd(f.saved_cost)} | {be} |"
            )
    agg = aggregate(faults)
    lines += [
        "",
        "## Aggregates",
        "",
        "| scope | faults | repeats (missed) | served by playbook | first-try $ | learn $ | median time ratio | median cost ratio | median saved s | median saved $ | median break-even |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for scope, b in [("all", agg["all"]), *agg["by_kind"].items()]:
        lines.append(
            f"| {scope} | {b['faults']} | {b['repeats']} ({b['repeats_missed']}) | {b['repeats_served_by_playbook']} | {_usd(b['first_cost_usd'])} | {_usd(b['learn_cost_usd'])} "
            f"| {_s(b['median_time_ratio'], '{:.2f}')} | {_s(b['median_cost_ratio'], '{:.2f}')} | {_s(b['median_saved_seconds'])} | {_usd(b['median_saved_cost_usd'])} | {_s(b['median_break_even_recurrences'], '{:.1f}')} |"
        )
    if missing:
        lines += ["", "## Missing from the records", ""] + [f"- {m}" for m in missing]
    return "\n".join(lines) + "\n"


def run(pool: Path | None, cells: list[Path], manifest: Path | None) -> tuple[str, dict[str, Any]]:
    if manifest is not None:
        faults = faults_from_manifest(json.loads(manifest.read_text()), pool or manifest.parent)
    else:
        faults = faults_from_cells(cells)
    return render(faults), to_json(faults)
