"""``python -m benchmarks.sregym.analysis <report> ...``: the one entry point for the analysis reports.

Reports: ``wall-time`` (agent transcripts of the given cells, or transcript files), ``phases`` (marks from inject to close, and where the agent's span went), ``learning-economics``,
``sweep`` and ``detection`` (cells from ``--manifest`` with ``--pool``, or cell directories named
``<fault>-<arm>-<rev>``), and ``onboarding`` (directories holding ``scorecard.json``). ``all`` is the
end-of-batch hook: it runs every report whose inputs exist and writes ``<out>/<report>.md`` and ``.json``
(default ``./analysis``). A failing report is skipped, never fatal.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from benchmarks.sregym.analysis import (
    agent_wall_time,
    detection,
    learning_economics,
    onboarding_summary,
    phases,
    sweep_table,
)
from benchmarks.sregym.analysis.cells import load_cell

_NAME = re.compile(r"^(?P<fault>.+)-(?P<arm>[a-z]\d*)-[0-9a-f]{6,}$")
Report = tuple[str, Any]


def is_cell(path: Path) -> bool:
    return path.is_dir() and (path / "runs").is_dir()


def expand(paths: list[Path]) -> list[Path]:
    """A directory of cells stands for its cells; a cell or any other path stands for itself."""
    out: list[Path] = []
    for p in paths:
        children = sorted(c for c in p.iterdir() if is_cell(c)) if p.is_dir() and not is_cell(p) else []
        out += children or [p]
    return out


def _manifest(args: argparse.Namespace) -> tuple[list[dict[str, Any]], Path]:
    if args.manifest:
        return json.loads(args.manifest.read_text()), args.pool or args.manifest.parent
    cells = [p for p in args.paths if is_cell(p)]
    groups: dict[str, dict[str, Any]] = {}
    for p in sorted(cells):
        m = _NAME.match(p.name)
        if m:
            g = groups.setdefault(m["fault"], {"fault": m["fault"], "first": None, "repeats": []})
            if m["arm"] == "a":
                g["first"] = p.name
            else:
                g["repeats"].append(p.name)
    return [g for g in groups.values() if g["first"]], cells[0].parent if cells else Path()


def _economics(args: argparse.Namespace) -> Report:
    manifest, pool = _manifest(args)
    faults = learning_economics.faults_from_manifest(manifest, pool)
    return learning_economics.render(faults), learning_economics.to_json(faults)


def _sweep(args: argparse.Namespace) -> Report:
    manifest, pool = _manifest(args)
    table = sweep_table.rows(manifest, pool)
    return sweep_table.render(table), table


def _detection(args: argparse.Namespace) -> Report:
    manifest, pool = _manifest(args)
    items = detection.rows(manifest, pool)
    return detection.render(items), [d.to_json() for d in items]


def _onboarding(args: argparse.Namespace) -> Report:
    rows = [onboarding_summary.summarize(p) for p in onboarding_summary.find(args.paths)]
    return onboarding_summary.render(rows), rows


def _phases(args: argparse.Namespace) -> Report:
    cells = [p for p in args.paths if is_cell(p)]
    if not cells:
        raise ValueError("no cells")
    items = phases.rows(cells)
    return phases.render(items), [p.to_json() for p in items]


def _transcripts(paths: list[Path]) -> list[Path]:
    out: list[Path] = []
    for p in paths:
        out += load_cell(p).transcripts() if is_cell(p) else [p]
    return out


def _wall_time(args: argparse.Namespace) -> Report:
    files = _transcripts(args.paths)
    if not files:
        raise ValueError("no agent transcripts")
    report = agent_wall_time.analyze(files, agent_wall_time.DEFAULT_RULES)
    flat = {
        "main": {"wall_s": report["main"].wall, "buckets_s": dict(report["main"].buckets)},
        "subagents": [{"agent": s.key, "wall_s": s.wall, "buckets_s": dict(s.buckets)} for s in report["subagents"]],
    }
    return "```\n" + agent_wall_time.render(report, 5) + "\n```\n", flat


REPORTS = {
    "learning-economics": _economics,
    "sweep": _sweep,
    "detection": _detection,
    "onboarding": _onboarding,
    "wall-time": _wall_time,
    "phases": _phases,
}


def _write(out: Path, name: str, md: str, data: Any) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{name}.md").write_text(md, encoding="utf-8")
    (out / f"{name}.json").write_text(json.dumps(data, indent=1, default=str), encoding="utf-8")


def _applies(name: str, paths: list[Path]) -> bool:
    if name == "onboarding":
        return any(onboarding_summary.find([p]) for p in paths if p.exists())
    return any(is_cell(p) for p in paths)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.sregym.analysis")
    parser.add_argument("report", choices=[*REPORTS, "all"])
    parser.add_argument("paths", nargs="*", type=Path, help="cell directories, scorecard directories or transcripts")
    parser.add_argument("--manifest", type=Path, help="JSON list of {fault, kind, first, repeats}")
    parser.add_argument("--pool", type=Path, help="directory the manifest's cell names resolve under")
    parser.add_argument("--out", type=Path, help="write <out>/<report>.md and .json instead of printing")
    args = parser.parse_args(argv)
    args.paths = expand(args.paths)
    if args.report == "all":
        out = args.out or Path("analysis")
        for name, fn in REPORTS.items():
            if not (_applies(name, args.paths) or (args.manifest and name != "onboarding")):
                continue
            try:
                md, data = fn(args)
            except Exception as exc:  # the hook must never fail a batch
                print(f"{name}: skipped ({exc})", file=sys.stderr)
                continue
            _write(out, name, md, data)
        return 0
    md, data = REPORTS[args.report](args)
    if args.out:
        _write(args.out, args.report, md, data)
    else:
        print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
