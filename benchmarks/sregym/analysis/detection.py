"""Detection latency and miss classification per fault, from the incident timeline and the grader record.

Per first-try cell: inject, harm onset (first failing user second), the page (incident detected), what the
page named, and the first alert that names an operation (a request-metrics error or a traffic probe, as
opposed to a rollout or pod alert). A miss is classified:

- ``not-run``: the cell has no run or grader record at all;
- ``harness``: the bench run did not finish (capped) or has no grader record;
- ``ok``: harm of at most ``MIN_HARM`` bad user seconds, or an incident that paged on an operation within
  ``LATE_PAGE`` seconds of harm onset and recovered within the window;
- ``detection``: bad user seconds but no incident, or a page that came late or named no operation;
- ``response``: an incident paged on time on an operation but did not recover in the window or closed
  not ``success``.
"""

# ruff: noqa: E501
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.analysis.cells import Cell, Incident, delta, load_cell

if TYPE_CHECKING:
    from pathlib import Path

MIN_HARM = 15
LATE_PAGE = 60.0
OPERATION_DETECTORS = ("request-metrics", "traffic-probe")


def names_operation(detector_id: str, rule_id: str) -> bool:
    if detector_id.startswith("traffic-probe"):
        return True
    return detector_id == "request-metrics" and rule_id.endswith(".errors")


@dataclass
class Detection:
    fault: str
    kind: str
    cell: str
    harm_onset_s: float | None  # after inject
    page_s: float | None  # after inject
    operation_named_s: float | None  # after inject
    page_after_harm_s: float | None
    operation_after_harm_s: float | None
    paged_by: list[str]
    bad_seconds: int | None
    recovered_in_window: bool | None
    classification: str
    miss_class: str

    def to_json(self) -> dict[str, Any]:
        return dict(self.__dict__)


def classify(
    cell: Cell, incident: Incident | None, page_after_harm: float | None, named_after_harm: float | None
) -> str:
    if cell.run_ok is None and cell.window_bad_seconds is None:
        return "not-run"
    if cell.run_ok is False or cell.window_bad_seconds is None:
        return "harness"
    bad = cell.window_bad_seconds
    if incident is None:
        return "ok" if bad <= MIN_HARM else "detection"
    on_time = named_after_harm is not None and named_after_harm <= LATE_PAGE
    recovered = bool(cell.recovered_in_window) and incident.classification == "success"
    if not on_time:
        return "ok" if bad <= MIN_HARM and recovered else "detection"
    return "ok" if recovered or bad <= MIN_HARM else "response"


def detect(cell: Cell, fault: str = "", kind: str = "") -> Detection:
    i = cell.first_incident
    inject, onset = cell.inject_at, cell.harm_onset
    named = min((t for d, r, t in (i.first_activations if i else []) if names_operation(d, r)), default=None)
    page_after = delta(i.detected_at, onset) if i else None
    named_after = delta(named, onset)
    return Detection(
        fault=fault or cell.name,
        kind=kind or "unknown",
        cell=cell.name,
        harm_onset_s=delta(onset, inject),
        page_s=delta(i.detected_at, inject) if i else None,
        operation_named_s=delta(named, inject),
        page_after_harm_s=page_after,
        operation_after_harm_s=named_after,
        paged_by=i.opened_by if i else [],
        bad_seconds=cell.window_bad_seconds,
        recovered_in_window=cell.recovered_in_window,
        classification=i.classification if i else "",
        miss_class=classify(cell, i, page_after, named_after),
    )


def rows(manifest: list[dict[str, Any]], pool: Path) -> list[Detection]:
    return [
        detect(load_cell(pool / e["first"]), e["fault"], e.get("kind", ""))
        for e in manifest
        if e.get("first") and (pool / e["first"]).exists()
    ]


def _s(v: float | None) -> str:
    return "-" if v is None else f"{v:.0f}"


def render(items: list[Detection]) -> str:
    lines = [
        "# Detection latency and misses",
        "",
        f"Seconds after inject unless noted. Miss class: harm <= {MIN_HARM} bad s is ok; a page is on time when an operation-naming alert fires within {LATE_PAGE:.0f} s of harm onset.",
        "",
        "| fault | kind | harm onset | page | operation named | page after harm | operation after harm | paged by | bad s | outcome | miss class |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    lines.extend(
        (
            f"| {d.fault} | {d.kind} | {_s(d.harm_onset_s)} | {_s(d.page_s)} | {_s(d.operation_named_s)} | {_s(d.page_after_harm_s)} "
            f"| {_s(d.operation_after_harm_s)} | {','.join(d.paged_by) or 'none'} | {d.bad_seconds if d.bad_seconds is not None else '-'} "
            f"| {d.classification or '-'} | {d.miss_class} |"
        )
        for d in items
    )
    counts = {c: sum(d.miss_class == c for d in items) for c in ("ok", "detection", "response", "harness", "not-run")}
    lines += ["", "Totals: " + ", ".join(f"{k} {v}" for k, v in counts.items()), ""]
    return "\n".join(lines)
