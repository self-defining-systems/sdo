"""Host-side pool processes: deploy a lane, or run one pair through fastloop's parallel runner."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from typing import TYPE_CHECKING

from benchmarks.sregym.pool.scheduler import Ready

if TYPE_CHECKING:
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
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
    """One first+repeat (or a retried remainder) on one lane, with fastloop's guarded parallel runner."""
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.parallel import Encounter, ParallelPlan, run_parallel

    environment = FastloopEnvironment.load(spec.run_dir.resolve())
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
