"""Assemble the live runner from a config file: ``[row]`` (fastloop rows on lanes) and optional ``[store]``.

The config is the eval TOML plus two tables:

    [row]                      # RowSettings: code_root, lane_environments, snapshot_repos, probe_urls, model, ...
    [store]                    # memory snapshots; required when a pass 1/2 cell runs
    root = "/path/store"       # where per-app snapshot repositories are kept
    [store.bases]              # app -> {repo = "...", commit = "..."}: the onboarding memory

Pluggable pieces are the evidence gate and accountant: stopgaps are used until ``--gate``/``--accountant`` name
``module:attr`` factories (the accounting agent's ledger-based ones).
"""

from __future__ import annotations

import importlib
import json
from typing import TYPE_CHECKING, Any

import tomllib

from benchmarks.sregym.eval.cell import CellContext
from benchmarks.sregym.eval.fastloop_row import FastloopRowRunner, RecordAccountant, RowSettings, TranscriptPresenceGate
from benchmarks.sregym.eval.results import ResultsTable
from benchmarks.sregym.eval.scheduler import LoadGate, Scheduler, SnapshotStore

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.eval.plan import Cell, Plan


class NoStore:
    """Vanilla-only runs have no memory snapshots; asking for one is a configuration error."""

    def resolve(self, app: str, key: str) -> str:
        raise ValueError("eval: this cell needs a memory snapshot store; add a [store] table to the config")

    def record_first_try(self, app: str, problem: str, memory_commit_after: str | None) -> None:
        raise ValueError("eval: no snapshot store configured")

    def merge(self, app: str, problems: Any) -> object:
        raise ValueError("eval: no snapshot store configured")


def load_factory(spec: str) -> Any:
    module, _, attribute = spec.partition(":")
    if not attribute:
        raise ValueError(f"factory {spec!r} must be module:attr")
    return getattr(importlib.import_module(module), attribute)


def build_store(table: dict[str, Any] | None) -> tuple[SnapshotStore, Any]:
    """The snapshot store and the first-try import hook it provides (``None`` without a store)."""
    if not table:
        return NoStore(), None
    from benchmarks.sregym.eval.snapshots import GitSnapshotStore

    store = GitSnapshotStore.from_config(table)
    return store, store.import_first_try


def run_live(config_path: Path, plan: Plan) -> int:
    raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    config = plan.config
    store, import_memory = build_store(raw.get("store"))
    row = dict(raw["row"])
    if "snapshot_repos" not in row and raw.get("store"):
        row["snapshot_repos"] = store.repositories()  # type: ignore[attr-defined]
    settings = RowSettings.model_validate(row)
    runner = FastloopRowRunner(settings, config.stage, import_memory=import_memory)
    gate = load_factory(raw["gate"])() if "gate" in raw else TranscriptPresenceGate()
    accountant = load_factory(raw["accountant"])() if "accountant" in raw else RecordAccountant(runner.records)

    def resolve(cell: Cell) -> str | None:
        return None if cell.snapshot is None else store.resolve(cell.app, cell.snapshot)

    context = CellContext(
        config=config,
        backend=runner,
        executors={"sdo": runner, "vanilla": runner},
        accountant=accountant,
        gate=gate,
        resolve_snapshot=resolve,
    )
    root = config.runs_root / config.stage
    plan.write(root / "plan.json")
    load_gate = LoadGate(config.max_load, config.admit_spacing_seconds)
    rows = Scheduler(plan, context, store, ResultsTable(root), gate=load_gate).run()
    summary = {
        "cells": len(rows),
        "graded": sum(r.status == "graded" for r in rows),
        "not_graded": sum(r.status == "not_graded" for r in rows),
        "error": sum(r.status == "error" for r in rows),
        "results": str(root / "results.csv"),
    }
    print(json.dumps(summary))
    return 0 if summary["error"] == 0 else 1
