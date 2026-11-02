"""Re-derive a row's reflection cost from its run directory.

The controller appends one record per reflection turn (each resumed turn, retry
and correction) to ``sdo_runtime/sessions/<sha256(incident_id)[:32]>/reflection-usage.jsonl``.
Receipts written before the Go outcome record totalled every turn kept only the
last one, so an old row's ``reflection_usage`` understates its real cost. The
exported ``sessions/`` directory also holds other incidents' sessions, so only the
receipt incident's own log counts.

    uv run python -m benchmarks.sregym.analysis.reflection_cost <row-dir>... [--json out.json]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from benchmarks.sregym.analysis.incident_cost import RECEIPT_NAME, TokenUsage, logged_reflection_cost


def _receipt(run_dir: Path) -> dict[str, Any]:
    paths = sorted(run_dir.rglob(RECEIPT_NAME))
    if not paths:
        raise FileNotFoundError(f"no {RECEIPT_NAME} under {run_dir}")
    return json.loads(paths[0].read_text(encoding="utf-8"))


def recompute_row(run_dir: Path) -> dict[str, Any]:
    """The receipt's recorded reflection usage beside the total re-derived from the run's usage log."""

    receipt = _receipt(run_dir)
    incident_id = str(receipt.get("incident_id") or "")
    recorded = TokenUsage.from_mapping(receipt.get("reflection_usage"))
    logged = logged_reflection_cost(run_dir, incident_id)
    row: dict[str, Any] = {
        "row": str(run_dir),
        "incident_id": incident_id,
        "recorded_input_tokens": recorded.input_tokens,
        "recorded_output_tokens": recorded.output_tokens,
        "recorded_requests": recorded.requests,
    }
    if logged is None:
        return {**row, "turns": None, "understated": None}
    return {
        **row,
        "turns": logged.turns,
        "session_ids": list(logged.session_ids),
        "recomputed_input_tokens": logged.usage.input_tokens,
        "recomputed_cached_input_tokens": logged.usage.cached_input_tokens,
        "recomputed_output_tokens": logged.usage.output_tokens,
        "recomputed_requests": logged.usage.requests,
        "recomputed_cost_usd": logged.cost_usd,
        "understated": logged.usage.input_tokens + logged.usage.output_tokens
        > recorded.input_tokens + recorded.output_tokens,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("rows", nargs="+", type=Path, help="row result directories (each with a strict receipt)")
    parser.add_argument("--json", type=Path, help="write the recomputed rows here")
    args = parser.parse_args(argv)
    rows = [recompute_row(path) for path in args.rows]
    text = json.dumps(rows, indent=2, sort_keys=True)
    if args.json:
        args.json.write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
