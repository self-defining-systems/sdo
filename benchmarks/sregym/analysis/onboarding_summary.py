"""Onboarding trial summary: one row per scorecard (``scorecard.json``) with its wall time and headline scores.

The grader (``scorecard.py``) keeps the scoring; this only reads the fields it wrote: started/finished,
coverage, precision (incidents raised while quiet) and recall (graded breaks raised within 90 s).
"""

# ruff: noqa: E501
from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path


def _t(value: Any) -> float | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def find(paths: list[Path]) -> list[Path]:
    out: list[Path] = []
    for p in paths:
        out += sorted(p.rglob("scorecard.json")) if p.is_dir() else [p]
    return out


def summarize(path: Path) -> dict[str, Any]:
    card = json.loads(path.read_text(encoding="utf-8"))
    start, end = _t(card.get("started")), _t(card.get("finished"))
    cov, pre, rec = card.get("coverage") or {}, card.get("precision") or {}, card.get("recall") or {}
    return {
        "scorecard": str(path),
        "memory": card.get("memory"),
        "build": card.get("build"),
        "wall_s": None if start is None or end is None else end - start,
        "coverage": cov.get("overall"),
        "precision_clean": pre.get("clean"),
        "precision_incidents": pre.get("incidents"),
        "recall": rec.get("recall"),
        "breaks": rec.get("breaks"),
        "graded": rec.get("graded"),
        "raised_within_90s": rec.get("raised_within_90s"),
    }


def _v(value: Any, fmt: str = "{}") -> str:
    return "?" if value is None else fmt.format(value)


def render(rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Onboarding trials",
        "",
        "| memory | build | wall min | coverage | clean while quiet | incidents raised | recall | graded of breaks |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        wall = None if r["wall_s"] is None else r["wall_s"] / 60
        lines.append(
            f"| {_v(r['memory'])} | {_v(r['build'])} | {_v(wall, '{:.1f}')} | {_v(r['coverage'])} | {_v(r['precision_clean'])} "
            f"| {_v(r['precision_incidents'])} | {_v(r['recall'])} | {_v(r['graded'])} of {_v(r['breaks'])} |"
        )
    return "\n".join(lines) + "\n"
