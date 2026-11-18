"""``python -m benchmarks.sregym.eval plan|dry-run --config FILE``."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import tomllib

from benchmarks.sregym.eval.plan import EvalConfig, build_plan


def load_config(path: Path) -> EvalConfig:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    for table in ("row", "store", "gate", "accountant"):  # the live runner's tables, read by ``live``
        data.pop(table, None)
    return EvalConfig.model_validate(data)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name, text in (
        ("plan", "write plan.json and print a summary"),
        ("dry-run", "run the plan against scripted fakes"),
        ("run", "run the plan live: fastloop rows on the configured lanes ([row] and [store] tables in the config)"),
    ):
        command = commands.add_parser(name, help=text)
        command.add_argument("--config", type=Path, required=True)
    args = parser.parse_args(argv)
    config = load_config(args.config)
    plan = build_plan(config)
    root = config.runs_root / config.stage
    if args.command == "plan":
        plan.write(root / "plan.json")
        counts: dict[str, int] = {}
        for cell in plan.cells:
            counts[cell.pass_] = counts.get(cell.pass_, 0) + 1
        print(json.dumps({"plan": str(root / "plan.json"), "cells": len(plan.cells), "per_pass": counts}, indent=2))
        return 0
    if args.command == "run":
        from benchmarks.sregym.eval.live import run_live

        return run_live(args.config, plan)
    from benchmarks.sregym.eval.dryrun import DryRunStore, dry_run_context
    from benchmarks.sregym.eval.results import ResultsTable
    from benchmarks.sregym.eval.scheduler import LoadGate, Scheduler

    plan.write(root / "plan.json")
    store = DryRunStore()
    gate = LoadGate(config.max_load, 0, loadavg=lambda: 0.0)
    rows = Scheduler(plan, dry_run_context(config), store, ResultsTable(root), gate=gate).run()
    print(
        json.dumps(
            {
                "cells": len(rows),
                "graded": sum(r.status == "graded" for r in rows),
                "results": str(root / "results.csv"),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
