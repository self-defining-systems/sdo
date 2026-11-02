"""Host-side pool processes: deploy a lane, or run one pair through fastloop's parallel runner."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.pool.scheduler import Ready

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.parallel import ParallelPlan
    from benchmarks.sregym.pool.scheduler import DeploySpec, PairSpec


def source_digest(environment: FastloopEnvironment) -> str:
    """What the lane deployed: app, deploy problem, seeded source commit and SDO images."""
    facts = {
        "app": environment.problem_application or environment.application,
        "deploy_problem": environment.deploy_problem,
        "seed_commit": environment.seed_commit,
        "images": environment.images.model_dump(),
    }
    return "sha256:" + hashlib.sha256(json.dumps(facts, sort_keys=True).encode()).hexdigest()


def prepare(spec: DeploySpec) -> None:
    """Run the configured deploy steps, then record the deployed lane for the scheduler."""
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    env = {**os.environ, "SDO_MEMORY_REPO": spec.memory.repo, "SDO_MEMORY_BRANCH": spec.memory.branch}
    for step in spec.steps:
        print(f"pool prepare {spec.lane}: {' '.join(step)}", flush=True)
        subprocess.run(step, check=True, env=env)
    environment = FastloopEnvironment.load(spec.run_dir.resolve())
    ready = Ready(
        app=spec.app, source_digest=source_digest(environment), build=spec.build, kubeconfig=environment.kubeconfig
    )
    spec.ready.parent.mkdir(parents=True, exist_ok=True)
    temporary = spec.ready.with_suffix(".tmp")
    temporary.write_text(ready.model_dump_json(indent=2) + "\n")
    temporary.replace(spec.ready)


def pair(spec: PairSpec) -> None:
    """One first+repeat (or a retried remainder) on one lane, with fastloop's guarded lane runner."""
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.parallel import Encounter, ParallelPlan, run_parallel

    environment = FastloopEnvironment.load(spec.run_dir.resolve())
    if bool(environment.shared_accepted_id) != (spec.lane_memory == "shared"):
        raise ValueError(f"pool: lane {spec.lane} memory differs from the run's lane_memory={spec.lane_memory}")
    if spec.lane_memory == "private":
        private_pair(spec, environment)
        return
    plan = ParallelPlan(
        run_id=spec.run_id,
        accepted_repository=environment.workspace,
        lanes={spec.lane: spec.run_dir},
        encounters=[Encounter(problem=spec.problem, lane=spec.lane) for _ in spec.encounters],
        timeout_seconds=spec.timeout_seconds,
        provider=spec.provider,
        model=spec.model,
        capability_mode=spec.capability_mode,
        permissive_responder=spec.capability_mode == "permissive",
        diagnosis_grading=spec.diagnosis_grading,
    )
    run_parallel(plan, spec.output)


def private_pair(spec: PairSpec, environment: FastloopEnvironment) -> None:
    """A lane with its own memory runs the same guarded encounter command as a parallel lane."""
    from benchmarks.sregym.fastloop.parallel import lane_process_environment, lane_run_command
    from benchmarks.sregym.fastloop.records import load_records, write_records

    results = environment.run_dir / "results" / spec.run_id
    if results.exists():
        raise ValueError(f"pool: refusing to replay existing lane results: {results}")
    spec.output.mkdir(parents=True)
    stream = spec.output / "stream.json"
    stream.write_text(
        json.dumps(
            {
                "incidents": [[spec.problem] for _ in spec.encounters],
                "diagnosis_grading": spec.diagnosis_grading,
                "capability_mode": spec.capability_mode,
                "permissive_responder": spec.capability_mode == "permissive",
            }
        )
        + "\n"
    )
    command = lane_run_command(
        environment.run_dir,
        spec.run_id,
        stream,
        spec.timeout_seconds,
        ["--provider", spec.provider, "--model", spec.model],
    )
    with (spec.output / "worker.log").open("w") as log:
        completed = subprocess.run(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
            env=lane_process_environment(environment, 1),
        )
    path = results / "incidents.jsonl"
    write_records(spec.output / "incidents.jsonl", load_records(path) if path.exists() else [])
    if completed.returncode:
        raise SystemExit(completed.returncode)


def attach_lane(
    run_dir: Path,
    lane: str,
    problem: str,
    accepted: Path,
    report: Path,
    *,
    prepare_shared: Callable[[ParallelPlan, Path], Any] | None = None,
) -> None:
    """Prepare one lane and attach it to its app's shared accepted memory (fastloop ``parallel --prepare``).

    The first lane of an app clones the root's accepted repository; later lanes join it.
    """
    from benchmarks.sregym.fastloop.parallel import Encounter, ParallelPlan
    from benchmarks.sregym.fastloop.shared_memory import prepare_shared_lanes

    plan = ParallelPlan(
        run_id=f"attach-{lane}",
        accepted_repository=accepted,
        lanes={lane: run_dir},
        encounters=[Encounter(problem=problem, lane=lane)],
    )
    (prepare_shared or prepare_shared_lanes)(plan, report)
