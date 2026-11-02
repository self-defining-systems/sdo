"""Cost states and paired stream statistics with explicit missing denominators.

The receipt/session reconciler owns evidence. This pure reporting boundary never
turns incomplete usage into zero, and never mixes responder and reflection costs.
"""

from __future__ import annotations

import math
import re
import statistics
from dataclasses import dataclass
from typing import Any, Literal

CostState = Literal["measured", "model-free", "missing"]


@dataclass(frozen=True)
class RoleCost:
    state: CostState
    usd: float | None

    def __post_init__(self) -> None:
        if self.state not in {"measured", "model-free", "missing"}:
            raise ValueError("unknown cost state")
        if self.state == "missing":
            if self.usd is not None:
                raise ValueError("missing cost must have usd=None")
        elif self.usd is None or not math.isfinite(self.usd) or self.usd < 0:
            raise ValueError("known cost must be finite and nonnegative")
        if self.state == "model-free" and self.usd != 0:
            raise ValueError("model-free cost must be zero")

    @classmethod
    def from_usage(cls, usage: dict[str, Any]) -> RoleCost:
        usd = usage.get("usd")
        if usage.get("usd_complete") is not True or usd is None:
            return cls("missing", None)
        return cls("model-free" if usage.get("no_model_dispatched") is True else "measured", float(usd))


def role_cost(row: dict[str, Any], role: Literal["responder", "reflection"]) -> RoleCost:
    return RoleCost.from_usage(row.get(role + "_usage") or {})


def format_role_cost(row: dict[str, Any], role: Literal["responder", "reflection"]) -> str:
    cost = role_cost(row, role)
    return "missing" if cost.usd is None else f"{cost.usd:.4f}({cost.state})"


def _values(row: dict[str, Any]) -> dict[str, float | None]:
    responder = role_cost(row, "responder").usd
    reflection = role_cost(row, "reflection").usd
    health = row.get("health_clear_seconds")
    return {
        "health_clear_seconds": float(health) if health is not None and math.isfinite(float(health)) else None,
        "responder_usd": responder,
        "reflection_usd": reflection,
        "total_usd": responder + reflection if responder is not None and reflection is not None else None,
    }


def _median(values: list[float | None]) -> dict[str, Any]:
    known = [value for value in values if value is not None]
    return {"median": statistics.median(known) if known else None, "n": len(known), "missing": len(values) - len(known)}


def summarize_pairs(pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Settled injected rows by build and fault count; matched known ratios.

    A build's marginal median includes only its own rows. A within-build ratio
    requires both rows on that build. A known zero first cost has an undefined
    repeat/first ratio and gets its own count, separate from missing evidence.
    """
    builds = sorted(
        {
            str(row.get("mount_sha") or "unknown")
            for pair in pairs
            for key in ("first", "repeat")
            if (row := pair.get(key)) and row.get("status") == "settled" and row.get("injection_started_at")
        }
    )
    groups = []
    for build in ["all", *builds]:
        for kind in ("all", "single", "multi", "unknown"):
            metrics: dict[str, Any] = {}
            states: dict[str, Any] = {}
            selected = []
            for pair in pairs:
                match = re.search(r"__n(\d+)__", pair.get("problem_id") or "")
                pair_kind = ("single" if int(match[1]) == 1 else "multi") if match else "unknown"
                if kind != "all" and kind != pair_kind:
                    continue
                rows = {}
                for key in ("first", "repeat"):
                    row = pair.get(key) or {}
                    if (
                        row.get("status") == "settled"
                        and row.get("injection_started_at")
                        and (build == "all" or str(row.get("mount_sha") or "unknown") == build)
                    ):
                        rows[key] = row
                if rows:
                    selected.append(rows)
            if not selected:
                continue
            for metric in ("health_clear_seconds", "responder_usd", "reflection_usd", "total_usd"):
                marginal = {
                    key: [_values(rows[key])[metric] for rows in selected if key in rows] for key in ("first", "repeat")
                }
                both = [
                    (_values(rows["first"])[metric], _values(rows["repeat"])[metric])
                    for rows in selected
                    if "first" in rows and "repeat" in rows
                ]
                ratios = [b / a for a, b in both if a is not None and b is not None and a > 0]
                metrics[metric] = {
                    **{key: _median(values) for key, values in marginal.items()},
                    "ratio": {
                        "median": statistics.median(ratios) if ratios else None,
                        "n": len(ratios),
                        "missing": sum(a is None or b is None for a, b in both),
                        "zero_denominator": sum(a == 0 and b is not None for a, b in both),
                        "unpaired": len(selected) - len(both),
                    },
                }
            for role in ("responder", "reflection"):
                states[role] = {
                    key: {
                        state: sum(role_cost(rows[key], role).state == state for rows in selected if key in rows)
                        for state in ("measured", "model-free", "missing")
                    }
                    for key in ("first", "repeat")
                }
            groups.append(
                {"build": build, "kind": kind, "pairs": len(selected), "metrics": metrics, "cost_states": states}
            )
    return groups


def main(argv: list[str] | None = None) -> int:
    """Reconcile frozen host projections and write an explicit analysis artifact."""
    import argparse
    import hashlib
    import json
    from pathlib import Path

    from benchmarks.sregym.analysis.claude_cost import reconcile_claude_row

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.resolve() in {path.resolve() for path in args.input}:
        parser.error("output must differ from recorded inputs")
    if any(part in {"results", "live", ".sdo", "application_workspace"} for part in args.output.parts):
        parser.error("output must be outside recorded run directories")
    pairs = []
    inputs = []
    seen = set()
    for path in args.input:
        raw = path.read_bytes()
        document = json.loads(raw)
        inputs.append(
            {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(), "at_epoch": document.get("at_epoch")}
        )
        for pair in document["pairs"]:
            identity = (pair.get("host"), pair.get("lane"), pair["sequence"])
            if identity in seen:
                parser.error(f"duplicate pair identity across inputs: {identity}")
            seen.add(identity)
            for key in ("first", "repeat"):
                row = pair.get(key)
                if row:
                    reconcile_claude_row(row)
            pairs.append(pair)
    document = {"inputs": inputs, "pairs": pairs, "summary": summarize_pairs(pairs)}
    args.output.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
