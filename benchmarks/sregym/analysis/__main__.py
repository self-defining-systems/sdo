"""``python -m benchmarks.sregym.analysis <report> ...``: the one entry point for the analysis reports.

Reports: ``wall-time`` (agent_wall_time), ``learning-economics`` and ``sweep`` (cells from ``--manifest`` with
``--pool``, or cell directories named ``<fault>-<arm>-<rev>``). ``all <out-dir>`` is the end-of-batch hook:
it runs every report that has its inputs and writes ``<out-dir>/analysis/<report>.md`` and ``.json``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from benchmarks.sregym.analysis import agent_wall_time, learning_economics, sweep_table


def _manifest(args: argparse.Namespace) -> tuple[list[dict[str, Any]], Path]:
    if args.manifest:
        return json.loads(args.manifest.read_text()), args.pool or args.manifest.parent
    cells = [p for p in args.paths if p.is_dir()]
    synthetic = []
    for fault, g in sorted(_group(cells).items()):
        synthetic.append({"fault": fault, "first": g["first"].name, "repeats": [p.name for p in g["repeats"]]})
    return synthetic, cells[0].parent if cells else Path()


def _group(cells: list[Path]) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for f in learning_economics.faults_from_cells(cells):
        pool_of = {p.name: p for p in cells}
        groups[f.fault] = {"first": pool_of[f.first.cell], "repeats": [pool_of[r.cell] for r in f.repeats]}
    return groups


def _write(out: Path, name: str, md: str, data: Any) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{name}.md").write_text(md, encoding="utf-8")
    (out / f"{name}.json").write_text(json.dumps(data, indent=1), encoding="utf-8")


def _economics(args: argparse.Namespace) -> tuple[str, Any]:
    manifest, pool = _manifest(args)
    faults = learning_economics.faults_from_manifest(manifest, pool)
    return learning_economics.render(faults), learning_economics.to_json(faults)


def _sweep(args: argparse.Namespace) -> tuple[str, Any]:
    manifest, pool = _manifest(args)
    table = sweep_table.rows(manifest, pool)
    return sweep_table.render(table), table


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.sregym.analysis")
    parser.add_argument("report", choices=["wall-time", "learning-economics", "sweep", "all"])
    parser.add_argument("paths", nargs="*", type=Path, help="cell directories (or transcripts for wall-time)")
    parser.add_argument("--manifest", type=Path, help="JSON list of {fault, kind, first, repeats}")
    parser.add_argument("--pool", type=Path, help="directory the manifest's cell names resolve under")
    parser.add_argument("--out", type=Path, help="write <out>/<report>.md and .json instead of printing")
    args = parser.parse_args(argv)
    if args.report == "wall-time":
        return agent_wall_time.main_cli([str(p) for p in args.paths])
    if args.report == "all":
        out = args.out or Path("analysis")
        for name, fn in (("learning-economics", _economics), ("sweep", _sweep)):
            try:
                md, data = fn(args)
            except Exception as exc:  # the hook must never fail a batch
                print(f"{name}: skipped ({exc})", file=sys.stderr)
                continue
            _write(out, name, md, data)
        return 0
    md, data = (_economics if args.report == "learning-economics" else _sweep)(args)
    if args.out:
        _write(args.out, args.report, md, data)
    else:
        print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
