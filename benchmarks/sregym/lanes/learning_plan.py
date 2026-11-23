"""Write a lanes plan that measures SREGym learning: p1 learns, p2 starts from p1's memory, cold starts from the base.

``python -m benchmarks.sregym.lanes.learning_plan --name NAME --pin SHA --host HOST --inputs DIR --out DIR
--app ALIAS=APP:BASELINE ... --problem ALIAS=PROBLEM ... (--tag TAG --auth-file PATH --vertex-project ID | --stub)``

``APP`` is a directory under ``benchmarks/sregym/lanes/apps``; ``BASELINE`` is its base memory
(``python -m benchmarks.sregym.lanes.baseline``). ``--problem`` items run in the order given: put the slowest
p1 -> p2 chains first. ``--inputs`` holds ``instances.json``.

Arms: ``p1`` (learns; its exported memory seeds ``p2`` through the plan edge), ``cold`` (base memory) and ``p2``.
Only p1 keeps the closing turn: cold's and p2's learning is never used, so they end at the acknowledged outcome
(``learning.closing_turn: false``) unless ``--all-arms-learn``. ``--warmup`` adds an untimed arm ``w`` that runs
first on each lane so measured rows start warm; ``w`` is the p1 spec, because only an identical spec can claim a
pool lane built from it. ``--pools`` writes one hold-pool spec per app (``pool-<alias>.json``) whose lanes the
first rows claim. ``--stub`` writes a single scripted-worker arm ``stub`` instead: the model-free proof a problem
must pass on the lane platform before a real-agent plan spends money on it.

Nothing is added to the manager: run ``lanes plan add DIR/plan.yaml`` (and ``lanes add DIR/pool-*.json``) yourself.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any

APPS_DIR = Path(__file__).resolve().parent / "apps"
WORKLOAD = "benchmarks/sregym/lanes/run.sh workload"
DEADLINE_FACTOR = 2.5


@dataclass(frozen=True)
class App:
    alias: str
    name: str
    baseline: str

    def __post_init__(self) -> None:
        if not (APPS_DIR / self.name / "app.json").is_file():
            raise ValueError(f"unknown SREGym lanes app {self.name!r}: no {APPS_DIR / self.name / 'app.json'}")

    @property
    def path(self) -> str:
        return f"benchmarks/sregym/lanes/apps/{self.name}"

    @property
    def namespace(self) -> str:
        return str(json.loads((APPS_DIR / self.name / "app.json").read_text())["namespace"])


@dataclass(frozen=True)
class Timing:
    """Row deadline = ``ready_timeout`` + ``cap`` + the workload's close-out."""

    ready_timeout: str = "40m"
    cap: str = "45m"

    @classmethod
    def from_speed_report(cls, path: Path) -> Timing:
        """``DEADLINE_FACTOR`` times the median ready gate and the median rest of a measured run's rows
        (``final-speed.json`` of ``benchmarks.sregym.lanes.row_speed``)."""
        rows = json.loads(path.read_text())
        if not rows:
            raise ValueError(f"{path}: no measured rows to derive deadlines from")
        gate = statistics.median(r["stages"]["gate"] for r in rows)
        rest = statistics.median(r["wall"] - r["stages"]["gate"] for r in rows)
        return cls(f"{math.ceil(DEADLINE_FACTOR * gate / 60)}m", f"{math.ceil(DEADLINE_FACTOR * rest / 60)}m")


def _pair(value: str, flag: str) -> tuple[str, str]:
    key, sep, rest = value.partition("=")
    if not sep or not key or not rest:
        raise ValueError(f"{flag} wants ALIAS=VALUE, got {value!r}")
    return key, rest


def parse_app(value: str) -> App:
    alias, rest = _pair(value, "--app")
    name, sep, baseline = rest.partition(":")
    if not sep or not baseline:
        raise ValueError(f"--app wants ALIAS=APP:BASELINE, got {value!r}")
    return App(alias, name, str(Path(baseline).resolve()))


def real_settings(args: argparse.Namespace, *, learns: bool) -> dict[str, Any]:
    config: dict[str, Any] = {
        "agent": {
            "extra_env": [
                "CLAUDE_CODE_USE_VERTEX=1",
                "CLOUD_ML_REGION=global",
                f"ANTHROPIC_VERTEX_PROJECT_ID={args.vertex_project}",
                "GOOGLE_APPLICATION_CREDENTIALS=/sdo/credentials/.credentials.json",
            ]
        },
        "response": {"after_playbook": "agent"},
    }
    if not learns:
        config["learning"] = {"closing_turn": False}
    return {
        "provider": "claude",
        "model": args.model,
        "effort": args.effort,
        "auth_file": args.auth_file,
        "controller_image": f"sdo-controller:{args.tag}",
        "responder_image": f"sdo-sregym-responder:{args.tag}",
        "config": config,
    }


def arm_spec(args: argparse.Namespace, timing: Timing, *, seed: bool, settings: dict[str, Any]) -> dict[str, Any]:
    operator: dict[str, Any] = {"hooks": "sregym", "settings": settings}
    if seed:
        operator["seed"] = "${artifact.memory}"
    return {
        "workload": WORKLOAD,
        "operator": operator,
        "faults": {"instances": str(Path(args.inputs).resolve() / "instances.json"), "ids": ["${item.problem}"]},
        "sequences": [[1]],
        "run": {},
        "ready_for": "20s",
        "ready_timeout": timing.ready_timeout,
        "cap": timing.cap,
        "lanes": 1,
        "hosts": [args.host],
        "lane": {"cpus": args.lane_cpus, "memory": args.lane_memory},
        "keep_lanes": True,
        "retries": 0,
    }


def build(args: argparse.Namespace) -> dict[str, Any]:
    """Write the plan, its arm specs and the pool specs under ``args.out``; return the plan."""
    apps = {app.alias: app for app in map(parse_app, args.app)}
    problems = [_pair(value, "--problem") for value in args.problem]
    unknown = sorted({alias for alias, _ in problems} - set(apps))
    if unknown:
        raise ValueError(f"--problem names apps with no --app: {', '.join(unknown)}")
    if args.stub and (args.tag or args.warmup or args.all_arms_learn):
        raise ValueError(
            "--stub takes no --tag, --warmup or --all-arms-learn: stub rows use the scripted images of the host's"
            " SDO_IMAGE_TAG and do not learn"
        )
    if not args.stub and not (args.tag and args.auth_file and args.vertex_project):
        raise ValueError("a real-agent plan needs --tag, --auth-file and --vertex-project (or --stub)")
    if args.timing_from and (args.ready_timeout or args.cap):
        raise ValueError("--timing-from sets ready_timeout and cap: drop --ready-timeout and --cap")
    if args.timing_from:
        timing = Timing.from_speed_report(Path(args.timing_from))
    else:
        timing = Timing(args.ready_timeout or Timing.ready_timeout, args.cap or Timing.cap)
    out = Path(args.out)
    (out / "specs").mkdir(parents=True, exist_ok=True)

    if args.stub:
        arms = [("stub", False, {"startup_bound": "0s"})]
    else:
        arms = [
            (arm, arm == "p2", real_settings(args, learns=arm in ("p1", "w") or args.all_arms_learn))
            for arm in (("w",) if args.warmup else ()) + ("p1", "cold", "p2")
        ]
    specs = {arm: arm_spec(args, timing, seed=seed, settings=settings) for arm, seed, settings in arms}
    for arm, spec in specs.items():
        (out / "specs" / f"{arm}.json").write_text(json.dumps(spec, indent=1) + "\n")

    counter: dict[str, int] = {}
    items = []
    for alias, problem in problems:
        counter[alias] = counter.get(alias, 0) + 1
        items.append({"id": f"{alias}-{counter[alias]}", "group": alias, "vars": {"problem": problem}})
    groups: dict[str, Any] = {
        alias: {"spec": {"app": app.path, "namespace": app.namespace, "operator": {"baseline": app.baseline}}}
        for alias, app in apps.items()
        if alias in counter
    }
    for alias, group in groups.items():
        if args.select:
            group["spec"]["selector"] = {"row": alias}
        if args.pools:
            group["spec"]["claim"] = f"{args.name}-pool-{alias}"
            # a claimant that finds no pool lane (a p2) takes a free lane that holds its app and reseeds there
            group["spec"]["claim_only"] = False
    if args.pools:
        first = specs["stub" if args.stub else "p1"]
        for alias, app in apps.items():
            mine = [item for item in items if item["group"] == alias]
            if not mine:
                continue
            lanes = args.pool_lanes or 2 * len(mine)
            pool = {
                **first,
                "name": f"{args.name}-pool-{alias}",
                "app": app.path,
                "namespace": app.namespace,
                "hold": True,
                "lanes": lanes,
                "sequences": [[1]] * lanes,
                "fuzzops": args.pin,
                "faults": {**first["faults"], "ids": [mine[0]["vars"]["problem"]]},
                "operator": {**first["operator"], "baseline": app.baseline, "revision": args.pin},
            }
            if args.select:
                pool["selector"] = {"row": alias}
            (out / f"pool-{alias}.json").write_text(json.dumps(pool, indent=1) + "\n")

    edges: list[dict[str, str]] = []
    artifacts: dict[str, Any] = {}
    if not args.stub:
        edges.append({"from": "p1", "to": "p2", "artifact": "memory"})
        artifacts["memory"] = {"path": "${run}/exec/memory", "verify": "exists"}
        if args.warmup:
            edges += [{"from": "w", "to": "p1"}, {"from": "w", "to": "cold"}]
    plan: dict[str, Any] = {
        "plan": args.name,
        "pins": {"workload": f"sregym@{args.pin}", "subject": f"sregym@{args.pin}"},
        "arms": [{"name": arm, "spec": f"specs/{arm}.json"} for arm, _, _ in arms],
        "groups": groups,
        "items": items,
        "reps": 1,
        "order": "rep-major",
        # the manager hands lanes to released nodes in plan order; parallel = the lane count fixes that order
        "parallel": args.parallel,
        "edges": edges,
        "artifacts": artifacts,
        "alarms": {"factor": 3, "idle_lane": "30m", "arm_stall": "120m"},
    }
    if not edges:
        del plan["edges"], plan["artifacts"]
    (out / "plan.yaml").write_text(json.dumps(plan, indent=1) + "\n")
    return plan


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--name", required=True, help="plan name; pools are <name>-pool-<alias>")
    ap.add_argument("--pin", required=True, help="full SDO commit registered in hosts.yaml checkouts")
    ap.add_argument("--host", required=True, help="hosts.yaml host the rows run on")
    ap.add_argument("--inputs", required=True, help="directory holding instances.json")
    ap.add_argument("--out", required=True)
    ap.add_argument("--app", action="append", required=True, help="ALIAS=APP:BASELINE (repeatable)")
    ap.add_argument("--problem", action="append", required=True, help="ALIAS=PROBLEM in run order (repeatable)")
    ap.add_argument("--stub", action="store_true", help="one scripted-worker arm 'stub' (no model, no learning)")
    ap.add_argument("--tag", default="", help="tag of the plain controller and responder images built at the pin")
    ap.add_argument("--auth-file", default="", help="Vertex application-default credentials file")
    ap.add_argument("--vertex-project", default="", help="ANTHROPIC_VERTEX_PROJECT_ID")
    ap.add_argument("--model", default="claude-sonnet-5-5")
    ap.add_argument("--effort", default="medium")
    ap.add_argument("--warmup", action="store_true", help="untimed arm w before p1 and cold on each lane")
    ap.add_argument("--all-arms-learn", action="store_true", help="keep the closing turn on cold and p2")
    ap.add_argument("--pools", action="store_true", help="write hold pools per app and make the rows claim them")
    ap.add_argument("--pool-lanes", type=int, default=0, help="lanes per pool (default 2 per item)")
    ap.add_argument("--select", action="store_true", help="pin each app to lanes labelled row=<alias>")
    ap.add_argument("--parallel", type=int, default=3, help="nodes in flight: set it to the lane count")
    ap.add_argument("--lane-cpus", type=int, default=8)
    ap.add_argument("--lane-memory", default="28Gi")
    ap.add_argument("--ready-timeout", default="")
    ap.add_argument("--cap", default="")
    ap.add_argument("--timing-from", default="", help="final-speed.json of a measured run: 2.5x its medians")
    return ap


def main(argv: list[str] | None = None) -> None:
    args = parser().parse_args(argv)
    try:
        plan = build(args)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    print(f"{len(plan['items'])} items x {len(plan['arms'])} arms -> {args.out}")


if __name__ == "__main__":
    main()
