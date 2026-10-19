"""Learning-curve summary for the incident-stream experiment (SDO vs. memoryless Codex).

Reads an SDO persistent-controller pipeline directory and one or more plain
Codex experiment directories that replayed the same stream, joins both to the
stream manifest (``stream_learning_curve_manifest.json``, written by
``benchmarks.sregym.runner.incident_stream --out-dir <dir>``), and writes per-incident
rows, rolling averages, numbers split by incident type, SDO memory growth and a
timeline plot. Timing and token accounting reuse :mod:`incident_cost`: TTM is the
judge-free headline, TTD sits beside it, and a failed incident is reported as
``failed`` with its time, never averaged into the TTM curve.

Usage::

    uv run --with matplotlib python -m benchmarks.sregym.analysis.stream_curve \\
        --manifest <stream-dir>/stream_learning_curve_manifest.json \\
        --sdo third_party/sregym/logs/<pipeline> --codex <dir1> <dir2> --out <out-dir>
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from benchmarks.sregym.analysis.incident_cost import (
    RECEIPT_NAME,
    ArmPricing,
    CodexRun,
    IncidentCostError,
    SdoStage,
    TokenUsage,
    UsageSummary,
    _pricing_context,
    load_codex_runs,
    load_sdo_pipeline,
    pipeline_stage_dirs,
)
from benchmarks.sregym.detector_manifests import git_detector_manifests

WINDOW = 6
KINDS = ("first", "novel", "exact", "variant")


@dataclass
class Row:
    arm: str
    index: int
    problem_id: str
    kind: str
    family: str
    passed: bool | None
    ttd_s: float | None
    ttm_s: float | None
    raw_incl_judge_s: float | None
    tokens_raw: int | None
    tokens_weighted: float | None
    usd: float | None
    requests: int | None
    # SDO only
    responder_tokens_raw: int | None = None
    reflection_tokens_raw: int | None = None
    reflection_weighted: float | None = None
    warm_path: bool | None = None
    reflection_skipped: str | None = None
    match_reasons: str = ""
    detectors_after: int | None = None
    incident_detectors_after: int | None = None
    playbooks_after: int | None = None
    fired_incident_detectors: str = ""
    foreign_detector_firings: str = ""
    # Detector firing telemetry. None means the run predates it (unknown, not False).
    firing_telemetry: bool | None = None
    incident_detector_fired_before_dispatch: bool | None = None
    incident_detector_fired_after_dispatch: bool | None = None
    no_incident_detector_fired: bool | None = None
    fired_detectors: str = ""


FIRING_STREAM_NAME = "detector_firings.jsonl"


def load_firing_stream(path: Path) -> list[dict[str, Any]]:
    """The controller's detector firing records; a missing file or a torn line is skipped."""

    if not path.is_file():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


_RELATION_RANK = {"before_dispatch": 0, "after_dispatch": 1, "no_incident": 2}


def firing_columns(receipt: dict[str, Any], stream: list[dict[str, Any]]) -> dict[str, Any]:
    """Which detectors fired for one incident, and the three derived booleans.

    The receipt's closure timeline is authoritative. A receipt without one (an
    older controller) falls back to the stream records tagged with the incident
    id, which can show before/after dispatch but not health-only firing. With
    neither, everything is unknown (None) rather than False.
    """

    unknown: dict[str, Any] = {
        "firing_telemetry": None,
        "incident_detector_fired_before_dispatch": None,
        "incident_detector_fired_after_dispatch": None,
        "no_incident_detector_fired": None,
        "fired_detectors": "",
    }
    if receipt.get("detector_firing_available"):
        timeline = [e for e in receipt.get("detector_timeline") or [] if isinstance(e, dict)]
        fired = {str(e.get("detector_id")): str(e.get("relation")) for e in timeline}
        return {
            "firing_telemetry": True,
            "incident_detector_fired_before_dispatch": receipt.get("incident_detector_fired_before_dispatch"),
            "incident_detector_fired_after_dispatch": receipt.get("incident_detector_fired_after_dispatch"),
            "no_incident_detector_fired": receipt.get("no_incident_detector_fired"),
            "fired_detectors": ";".join(f"{name}:{relation}" for name, relation in fired.items()),
        }
    incident_id = receipt.get("incident_id")
    tagged = [r for r in stream if incident_id and r.get("incident_id") == incident_id]
    if not tagged:
        return unknown
    relations: dict[str, str] = {}
    classes: dict[str, str] = {}
    for record in tagged:
        if record.get("event") not in ("activated", "batched"):
            continue
        name = str(record.get("detector_id"))
        relation = str(record.get("dispatch_relation") or "before_dispatch")
        classes[name] = str(record.get("detector_class"))
        if _RELATION_RANK.get(relation, 3) < _RELATION_RANK.get(relations.get(name, ""), 3):
            relations[name] = relation
    incident_relations = {n: r for n, r in relations.items() if classes[n] != "health"}
    return {
        "firing_telemetry": True,
        "incident_detector_fired_before_dispatch": "before_dispatch" in incident_relations.values(),
        "incident_detector_fired_after_dispatch": "after_dispatch" in incident_relations.values(),
        "no_incident_detector_fired": None,
        "fired_detectors": ";".join(f"{name}:{relation}" for name, relation in relations.items()),
    }


def load_manifest(path: Path) -> list[dict[str, Any]]:
    return list(json.loads(path.read_text(encoding="utf-8"))["incidents"])


def _summary(usage: TokenUsage, pricing: ArmPricing) -> UsageSummary:
    return UsageSummary.of(usage, pricing)


def _git_show(repo: Path, rev: str, path: str) -> str | None:
    done = subprocess.run(
        ["git", "-C", str(repo), "show", f"{rev}:{path}"], capture_output=True, text=True, check=False
    )
    return done.stdout if done.returncode == 0 else None


def memory_at(repo: Path, rev: str) -> tuple[int, int, int] | None:
    """(detectors, incident detectors, playbooks) registered in ``.sdo/`` at commit ``rev``."""

    detectors = git_detector_manifests(repo, rev)
    if detectors is None:
        return None
    listing = subprocess.run(
        ["git", "-C", str(repo), "ls-tree", "-r", "--name-only", rev, ".sdo/playbooks"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.splitlines()
    playbooks = [p for p in listing if p.endswith("/README.md") and p.count("/") == 3]
    return len(detectors), sum(1 for d in detectors if d.get("class") == "incident"), len(playbooks)


def _receipts(pipeline_dir: Path) -> list[dict[str, Any]]:
    """Strict receipts in stage order (an empty dict where a stage has none)."""

    out: list[dict[str, Any]] = []
    for _, _, stage_dir in pipeline_stage_dirs(pipeline_dir):
        found = sorted(stage_dir.rglob(RECEIPT_NAME))
        out.append(json.loads(found[-1].read_text(encoding="utf-8")) if found else {})
    return out


def _firing_streams(pipeline_dir: Path) -> list[list[dict[str, Any]]]:
    """Per stage, the firing stream copied next to its strict receipt (empty when absent)."""

    out: list[list[dict[str, Any]]] = []
    for _, _, stage_dir in pipeline_stage_dirs(pipeline_dir):
        found = sorted(stage_dir.rglob(FIRING_STREAM_NAME))
        out.append(load_firing_stream(found[-1]) if found else [])
    return out


def _detector_origins(repo: Path, rev: str) -> dict[str, str | None]:
    detectors = git_detector_manifests(repo, rev) or []
    return {
        str(d.get("id") or d.get("name")): d.get("originatingIncident")
        for d in detectors
        if isinstance(d, dict) and d.get("class") == "incident"
    }


def sdo_rows(pipeline_dir: Path, manifest: list[dict[str, Any]], pricing: ArmPricing) -> list[Row]:
    stages = load_sdo_pipeline(pipeline_dir)
    receipts = _receipts(pipeline_dir)
    streams = _firing_streams(pipeline_dir)
    final_workspace = pipeline_stage_dirs(pipeline_dir)[-1][2] / "application_workspace"
    incident_family = {
        str(r.get("incident_id")): manifest[i]["family"] for i, r in enumerate(receipts) if r.get("incident_id")
    }
    rows: list[Row] = []
    for stage in stages:
        meta = manifest[stage.index]
        if stage.problem_id != meta["problem_id"]:
            raise IncidentCostError(f"stage {stage.index} ran {stage.problem_id}, manifest says {meta['problem_id']}")
        receipt = receipts[stage.index] if stage.index < len(receipts) else {}
        stream = streams[stage.index] if stage.index < len(streams) else []
        rows.append(_sdo_row(stage, meta, receipt, final_workspace, incident_family, pricing, stream))
    return rows


def _sdo_row(
    stage: SdoStage,
    meta: dict[str, Any],
    receipt: dict[str, Any],
    workspace: Path,
    incident_family: dict[str, str],
    pricing: ArmPricing,
    stream: list[dict[str, Any]] | None = None,
) -> Row:
    verdict = stage.verdict
    total = stage.responder + stage.reflection
    total_summary = _summary(total, pricing)
    memory = memory_at(workspace, str(receipt["reflection_commit"])) if receipt.get("reflection_commit") else None
    fired: list[str] = []
    foreign: list[str] = []
    origins = _detector_origins(workspace, str(receipt["reflection_commit"])) if memory else {}
    for state in receipt.get("incident_detector_states") or []:
        if isinstance(state, dict) and state.get("firing", state.get("state") == "firing"):
            name = str(state.get("detector") or state.get("id") or state.get("name"))
            fired.append(name)
            origin = origins.get(name)
            if origin and incident_family.get(str(origin), meta["family"]) != meta["family"]:
                foreign.append(name)
    return Row(
        arm="sdo",
        index=stage.index,
        problem_id=stage.problem_id,
        kind=meta["kind"],
        family=meta["family"],
        passed=verdict.passed if verdict.diagnosis is not None else None,
        ttd_s=verdict.diagnosis_seconds,
        ttm_s=verdict.ttm_seconds,
        raw_incl_judge_s=verdict.raw_incl_judge_seconds,
        tokens_raw=total.total(),
        tokens_weighted=total_summary.weighted_tokens,
        usd=total_summary.usd,
        requests=total.requests,
        responder_tokens_raw=stage.responder.total(),
        reflection_tokens_raw=stage.reflection.total(),
        reflection_weighted=_summary(stage.reflection, pricing).weighted_tokens,
        warm_path=stage.warm_path,
        reflection_skipped=stage.reflection_skipped_reason,
        match_reasons=";".join(stage.match_reasons),
        detectors_after=memory[0] if memory else None,
        incident_detectors_after=memory[1] if memory else None,
        playbooks_after=memory[2] if memory else None,
        fired_incident_detectors=";".join(fired),
        foreign_detector_firings=";".join(foreign),
        **firing_columns(receipt, stream or []),
    )


def codex_rows(directories: list[str], manifest: list[dict[str, Any]], pricing: ArmPricing) -> list[Row]:
    runs: list[CodexRun] = []
    for spec in directories:
        # ``DIR:N`` keeps only the first N runs of DIR (a directory that ran a discarded stream tail).
        path, _, take = spec.partition(":")
        loaded = load_codex_runs(Path(path))
        runs += loaded[: int(take)] if take else loaded
    rows: list[Row] = []
    for index, run in enumerate(runs):
        meta = manifest[index]
        if run.problem_id != meta["problem_id"]:
            raise IncidentCostError(f"codex run {index} is {run.problem_id}, manifest says {meta['problem_id']}")
        summary = _summary(run.tokens, pricing)
        rows.append(
            Row(
                arm="codex",
                index=index,
                problem_id=run.problem_id,
                kind=meta["kind"],
                family=meta["family"],
                passed=run.verdict.passed,
                ttd_s=run.verdict.diagnosis_seconds,
                ttm_s=run.verdict.ttm_seconds,
                raw_incl_judge_s=run.verdict.raw_incl_judge_seconds,
                tokens_raw=run.tokens.total(),
                tokens_weighted=summary.weighted_tokens,
                usd=summary.usd,
                requests=run.tokens.requests,
            )
        )
    return rows


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def rolling(rows: list[Row], field: str, window: int = WINDOW, *, passed_only: bool = False) -> list[float | None]:
    """Trailing mean of ``field`` over the last ``window`` incidents (only passing ones when asked)."""

    out: list[float | None] = []
    for i in range(len(rows)):
        chunk = rows[max(0, i - window + 1) : i + 1]
        values = [
            float(getattr(r, field)) for r in chunk if getattr(r, field) is not None and (not passed_only or r.passed)
        ]
        out.append(_mean(values))
    return out


def by_kind(rows: list[Row]) -> list[dict[str, Any]]:
    table: list[dict[str, Any]] = []
    for arm in sorted({r.arm for r in rows}):
        for kind in (*KINDS, "all"):
            chosen = [r for r in rows if r.arm == arm and (kind == "all" or r.kind == kind)]
            if not chosen:
                continue
            ok = [r for r in chosen if r.passed]

            def avg(field: str, pool: list[Row]) -> float | None:
                return _mean([float(getattr(r, field)) for r in pool if getattr(r, field) is not None])

            table.append(
                {
                    "arm": arm,
                    "kind": kind,
                    "n": len(chosen),
                    "passed": len(ok),
                    "ttm_s_passed_mean": avg("ttm_s", ok),
                    "ttd_s_passed_mean": avg("ttd_s", ok),
                    "tokens_raw_mean": avg("tokens_raw", chosen),
                    "tokens_weighted_mean": avg("tokens_weighted", chosen),
                    "usd_mean": avg("usd", chosen),
                }
            )
    return table


def exclude_incidents(rows: list[Row], indices: set[int]) -> list[Row]:
    """Drop incidents that could not be measured on one arm (e.g. the fault never manifested) from both arms."""

    return [r for r in rows if r.index not in indices]


def write_csv(path: Path, records: list[dict[str, Any]]) -> None:
    if not records:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)


def plot(rows: list[Row], out: Path) -> Path | None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None
    fig, axes = plt.subplots(1, 3, figsize=(16, 4))
    for arm, colour in (("codex", "#7a7a7a"), ("sdo", "#1f6feb")):
        arm_rows = [r for r in rows if r.arm == arm]
        xs = [r.index + 1 for r in arm_rows]
        axes[0].plot(xs, rolling(arm_rows, "ttm_s", passed_only=True), color=colour, label=arm)
        axes[1].plot(xs, rolling(arm_rows, "tokens_weighted"), color=colour, label=arm)
    axes[0].set_title(f"Judge-free TTM, rolling mean (window {WINDOW}, passing incidents)")
    axes[1].set_title(f"Cost-weighted tokens per incident, rolling mean (window {WINDOW})")
    sdo_rows_ = [r for r in rows if r.arm == "sdo" and r.playbooks_after is not None]
    xs = [r.index + 1 for r in sdo_rows_]
    axes[2].step(xs, [r.playbooks_after for r in sdo_rows_], where="post", label="playbooks", color="#1f6feb")
    axes[2].step(xs, [r.detectors_after for r in sdo_rows_], where="post", label="detectors", color="#d29922")
    axes[2].step(
        xs, [r.incident_detectors_after for r in sdo_rows_], where="post", label="incident detectors", color="#2da44e"
    )
    axes[2].set_title("SDO operational memory")
    for axis in axes:
        axis.set_xlabel("incident")
        axis.legend()
        axis.grid(alpha=0.3)
    fig.tight_layout()
    path = out / "learning_curve.png"
    fig.savefig(path, dpi=140)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--sdo", type=Path, required=True)
    parser.add_argument("--codex", nargs="+", required=True, help="experiment dirs, optionally DIR:N")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--exclude",
        type=int,
        nargs="*",
        default=[],
        help="0-based incident indices to drop from both arms in the aggregate tables and curves",
    )
    args = parser.parse_args(argv)
    manifest = load_manifest(args.manifest)
    context = _pricing_context(None, [])
    pricing = context.arm("codex", "gpt-6-luna")
    rows = sdo_rows(args.sdo, manifest, pricing) + codex_rows(args.codex, manifest, pricing)
    args.out.mkdir(parents=True, exist_ok=True)
    write_csv(args.out / "incidents_all.csv", [asdict(r) for r in rows])
    rows = exclude_incidents(rows, set(args.exclude))
    write_csv(args.out / "incidents.csv", [asdict(r) for r in rows])
    kinds = by_kind(rows)
    write_csv(args.out / "by_kind.csv", kinds)
    write_csv(
        args.out / "detector_firing.csv",
        [
            {
                "index": r.index,
                "problem_id": r.problem_id,
                "kind": r.kind,
                "warm_path": r.warm_path,
                "firing_telemetry": r.firing_telemetry,
                "incident_detector_fired_before_dispatch": r.incident_detector_fired_before_dispatch,
                "incident_detector_fired_after_dispatch": r.incident_detector_fired_after_dispatch,
                "no_incident_detector_fired": r.no_incident_detector_fired,
                "fired_detectors": r.fired_detectors,
            }
            for r in rows
            if r.arm == "sdo"
        ],
    )
    timeline: list[dict[str, Any]] = []
    for arm in ("sdo", "codex"):
        arm_rows = [r for r in rows if r.arm == arm]
        ttm, tok, wtok = (
            rolling(arm_rows, "ttm_s", passed_only=True),
            rolling(arm_rows, "tokens_raw"),
            rolling(arm_rows, "tokens_weighted"),
        )
        timeline += [
            {
                "arm": arm,
                "incident": r.index + 1,
                "rolling_ttm_s": a,
                "rolling_tokens_raw": b,
                "rolling_tokens_weighted": c,
            }
            for r, a, b, c in zip(arm_rows, ttm, tok, wtok, strict=True)
        ]
    write_csv(args.out / "rolling_window6.csv", timeline)
    figure = plot(rows, args.out)
    (args.out / "summary.json").write_text(
        json.dumps({"window": WINDOW, "by_kind": kinds, "figure": str(figure) if figure else None}, indent=2)
    )
    for record in kinds:
        print(record)
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
