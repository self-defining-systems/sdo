"""Sequential mixed-app dispatch through independently prepared fastloop environments.

Run with ``python -m benchmarks.sregym.fastloop.dispatch --config dispatch.json
--output-dir /outside/repo/run --run-id mixed -- --max-response-loops 10``. The
process is foreground work; the shared detached launcher owns process lifetime.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.records import IncidentRecord, append_record, load_records
from benchmarks.sregym.fastloop.stream import StreamConfig, load_stream_config

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


@dataclass(frozen=True)
class DispatchConfig:
    plan: tuple[str, ...]
    plan_sha256: str
    stream: StreamConfig
    environments: dict[str, FastloopEnvironment]

    def __post_init__(self) -> None:
        if len(self.plan) != 50:
            raise ValueError("dispatch requires the complete 50-slot plan")
        if not re.fullmatch(r"[0-9a-f]{64}", self.plan_sha256):
            raise ValueError("plan_sha256 must be a SHA-256 digest")
        if len(self.stream.incidents) != 10 or self.stream.incidents != tuple((p,) for p in self.plan[:10]):
            raise ValueError("stream must be the exact ordered first ten slots of the 50-slot plan")
        apps = {problem_app(p) for p in self.plan[:10]}
        if set(self.environments) != apps:
            raise ValueError(f"environments must match selected apps: {sorted(apps)}")
        seen_paths: list[Path] = []
        controllers: set[tuple[str, str]] = set()
        for app, env in self.environments.items():
            if env.application != app.replace("_", "-"):
                raise ValueError(f"{app}: environment application is {env.application!r}")
            if not env.seed_commit or not re.fullmatch(r"[0-9a-f]{40}", env.seed_commit):
                raise ValueError(f"{app}: a pinned verified seed commit is required")
            controller = (env.cluster, env.namespace)
            if controller in controllers:
                raise ValueError("apps must have separate controllers")
            controllers.add(controller)
            # Sibling workspaces within one environment are fine; no state tree may
            # alias or contain another app's workspace, controller registry or tmp.
            paths = [env.run_dir.resolve(), env.workspace.resolve()]
            if env.private_tmp is not None:
                paths.append(env.private_tmp.resolve())
            for path in paths:
                if any(path == other or path in other.parents or other in path.parents for other in seen_paths):
                    raise ValueError("apps must have isolated run directories, workspaces and private tmp")
            seen_paths.extend(paths)


def problem_app(problem_id: str) -> str:
    parts = problem_id.split("__")
    if len(parts) < 3 or parts[0] != "mf" or not parts[1]:
        raise ValueError(f"mixed dispatch requires generated app-qualified problem IDs: {problem_id!r}")
    return parts[1]


def load_dispatch_config(path: Path) -> DispatchConfig:
    """Load paths relative to the dispatch config, retaining the exact plan hash."""
    data = json.loads(path.read_text(encoding="utf-8"))
    root = path.resolve().parent
    plan_bytes = (root / data["plan"]).read_bytes()
    rows = json.loads(plan_bytes)
    if not isinstance(rows, list) or any(
        not isinstance(row, dict)
        or row.get("sequence") != i
        or not isinstance(row.get("problem_id"), str)
        or not row["problem_id"].strip()
        for i, row in enumerate(rows)
    ):
        raise ValueError("plan must contain contiguous ordered sequence IDs")
    digest = hashlib.sha256(plan_bytes).hexdigest()
    if data.get("plan_sha256", digest) != digest:
        raise ValueError("plan SHA-256 does not match the pinned plan")
    environments = {}
    for app, binding in data["environments"].items():
        env = FastloopEnvironment.load((root / binding["run_dir"]).resolve())
        if env.run_dir.resolve() != (root / binding["run_dir"]).resolve():
            raise ValueError(f"{app}: saved run directory differs from its binding")
        if env.seed_commit != binding["seed_commit"]:
            raise ValueError(f"{app}: environment seed differs from the configured verified seed")
        environments[app] = env
    return DispatchConfig(
        tuple(row["problem_id"] for row in rows),
        digest,
        load_stream_config(root / data["stream"]),
        environments,
    )


class SlotEnvironment(Protocol):
    @property
    def environment(self) -> FastloopEnvironment: ...

    def accepted_commit(self) -> str: ...

    def run_slot(self, index: int, problem_id: str, run_id: str) -> IncidentRecord: ...


@dataclass(frozen=True)
class FastloopSlotEnvironment:
    environment: FastloopEnvironment
    stream: StreamConfig
    run_args: tuple[str, ...] = ()

    def accepted_commit(self) -> str:
        env = self.environment
        # A seed pin is provenance, not permission to invent seed evidence. Normal
        # lifecycle/broker validation remains authoritative in the child run.
        subprocess.run(
            ["git", "-C", str(env.workspace), "merge-base", "--is-ancestor", str(env.seed_commit), "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return subprocess.run(
            ["git", "-C", str(env.workspace), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def command(self, index: int, problem_id: str, run_id: str) -> list[str]:
        return [
            sys.executable,
            "-m",
            "benchmarks.sregym.fastloop",
            "run",
            *self.run_args,
            "--run-dir",
            str(self.environment.run_dir),
            "--agent",
            "sdo",
            "--problem",
            problem_id,
            "--incidents",
            "1",
            "--slot-offset",
            str(index),
            "--run-id",
            run_id,
            "--require-full-cleanup",
            "--diagnosis-grading",
            self.stream.diagnosis_grading,
            "--judge-model-id",
            self.stream.judge_model_id,
        ]

    def run_slot(self, index: int, problem_id: str, run_id: str) -> IncidentRecord:
        path = self.environment.run_dir / "results" / run_id / "incidents.jsonl"
        if path.parent.exists():
            raise FileExistsError(f"refusing to reuse slot results: {path.parent}")
        # A fresh interpreter prevents process-global Kubernetes/model state from
        # following an app switch. The saved environment and Git memory persist.
        subprocess.run(self.command(index, problem_id, run_id), check=True)
        records = load_records(path)
        if len(records) != 1:
            raise ValueError(f"slot {index} must produce exactly one incident record")
        return records[0]


def _save_manifest(output: Path, manifest: dict[str, Any]) -> None:
    path = output / "run_manifest.json"
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def dispatch(
    config: DispatchConfig,
    environments: Mapping[str, SlotEnvironment],
    output: Path,
    *,
    run_id: str,
) -> int:
    """Run one global slot at a time, stopping on any failed recovery or child."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", run_id):
        raise ValueError("run_id must be a safe directory name")
    if set(environments) != set(config.environments) or any(
        item.environment != config.environments[app] for app, item in environments.items()
    ):
        raise ValueError("slot environments must match the validated app bindings")
    output.mkdir(parents=True, exist_ok=False)
    manifest: dict[str, Any] = {
        "schema_version": "sdo.mixed-dispatch/v1",
        "run_id": run_id,
        "status": "running",
        "plan_sha256": config.plan_sha256,
        "plan": [{"sequence": i, "problem_id": p} for i, p in enumerate(config.plan)],
        "environments": {
            app: {
                "seed_commit": env.seed_commit,
                "workspace": str(env.workspace),
                "run_dir": str(env.run_dir),
                "cluster": env.cluster,
                "namespace": env.namespace,
                "controller_state": str(env.state_path),
                "accepted_commit": None,
            }
            for app, env in config.environments.items()
        },
        "slots": [],
    }
    _save_manifest(output, manifest)
    try:
        for app, item in environments.items():
            commit = item.accepted_commit()
            manifest["environments"][app].update(initial_accepted_commit=commit, accepted_commit=commit)
        _save_manifest(output, manifest)
        for index, (problem_id,) in enumerate(config.stream.incidents):
            app = problem_app(problem_id)
            item = environments[app]
            slot: dict[str, Any] = {
                "index": index,
                "app": app,
                "problem_id": problem_id,
                "status": "running",
                "run_id": f"{run_id}-slot-{index:03d}",
                "accepted_commit_before": item.accepted_commit(),
            }
            manifest["slots"].append(slot)
            _save_manifest(output, manifest)
            try:
                record = item.run_slot(index, problem_id, slot["run_id"])
            finally:
                commit = item.accepted_commit()
                slot["accepted_commit_after"] = commit
                manifest["environments"][app]["accepted_commit"] = commit
            if record.index != index or record.problem_id != problem_id or record.run_id != slot["run_id"]:
                raise ValueError(f"slot {index} returned a mismatched incident identity")
            append_record(output / "incidents.jsonl", record)
            if (
                record.error
                or record.infra_error
                or record.recovery_error
                or record.cleanup_skipped_reason
                or not record.full_cleanup_verified
                or record.recovery_oracle is None
                or not record.recovery_oracle.success
            ):
                raise RuntimeError(
                    f"slot {index} did not pass full cleanup and recovery: {record.error or record.recovery_error}"
                )
            slot["status"] = "completed"
            _save_manifest(output, manifest)
        manifest["status"] = "completed"
    except BaseException as exc:
        manifest.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        if manifest["slots"]:
            manifest["slots"][-1]["status"] = "failed"
        _save_manifest(output, manifest)
        if not isinstance(exc, Exception):
            raise
        return 1
    _save_manifest(output, manifest)
    return 0


def validate_run_args(args: Sequence[str]) -> None:
    # Only behavior settings can be forwarded. Routing, cardinality, output paths
    # and failure policy belong to this dispatcher, never to appended CLI args.
    from benchmarks.sregym.fastloop.cli import build_parser

    parser = build_parser()
    base = ["run", "--run-dir", "/dispatch", "--agent", "sdo"]
    defaults = vars(parser.parse_args(base))
    supplied = vars(parser.parse_args([*base, *args]))
    owned = (
        "run_dir",
        "agent",
        "problem",
        "stream",
        "incidents",
        "slot_offset",
        "run_id",
        "continue_on_error",
        "sweep_service",
        "diagnosis_grading",
        "judge_model_id",
    )
    if any(supplied[key] != defaults[key] for key in owned):
        raise ValueError("run arguments cannot override dispatch routing, stream settings or failure policy")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("run_args", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    run_args = tuple(args.run_args[1:] if args.run_args[:1] == ["--"] else args.run_args)
    validate_run_args(run_args)
    config = load_dispatch_config(args.config)
    environments = {
        app: FastloopSlotEnvironment(env, config.stream, run_args) for app, env in config.environments.items()
    }
    return dispatch(config, environments, args.output_dir.resolve(), run_id=args.run_id)


if __name__ == "__main__":
    raise SystemExit(main())
