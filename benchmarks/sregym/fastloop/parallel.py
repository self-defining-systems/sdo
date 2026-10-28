"""Concurrent fastloop lanes, each with private process environment and one shared memory."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path  # noqa: TC003 - Pydantic resolves this annotation at runtime.
from typing import TYPE_CHECKING, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from benchmarks.sregym.fastloop.records import EVAL_CLEANUP_PASSES, IncidentRecord, load_records, write_records

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


class Encounter(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    problem: str = Field(min_length=1)
    lane: str = Field(min_length=1)


class ParallelPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$")
    accepted_repository: Path
    lanes: dict[str, Path]
    encounters: list[Encounter] = Field(min_length=1)
    timeout_seconds: int = Field(default=3600, gt=0)
    agent_registrations: Path | None = None

    @model_validator(mode="after")
    def distinct_lanes(self) -> Self:
        if not 1 <= len(self.lanes) <= 8:
            raise ValueError("parallel runs require 1..8 private lanes")
        if any(not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", name) for name in self.lanes):
            raise ValueError("lane names must be distinct DNS label prefixes")
        paths = [path.resolve() for path in self.lanes.values()]
        if len(set(paths)) != len(paths) or any(
            left in right.parents or right in left.parents for i, left in enumerate(paths) for right in paths[i + 1 :]
        ):
            raise ValueError("lanes require separate non-overlapping run directories")
        if {e.lane for e in self.encounters} != set(self.lanes):
            raise ValueError("every encounter must name a configured lane and every lane must have work")
        accepted = self.accepted_repository.resolve()
        if any(accepted == path or accepted in path.parents or path in accepted.parents for path in paths):
            raise ValueError("accepted memory must be separate from private lane storage")
        return self


def preflight_lane(name: str, run_dir: Path, plan: ParallelPlan) -> None:
    from argparse import Namespace

    from benchmarks.sregym.adapter import assert_lane_resources
    from benchmarks.sregym.fastloop.cli import check_host_capacity
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.shared_memory import assert_lane_isolation, assert_shared_lane
    from benchmarks.sregym.fastloop.source_preflight import assert_harness_sources

    environment = FastloopEnvironment.load(run_dir.resolve())
    assert_lane_isolation([FastloopEnvironment.load(path.resolve()) for path in plan.lanes.values()])
    check_host_capacity(Namespace(run_dir=run_dir))
    assert_lane_resources(environment.kubeconfig, app_namespaces={environment.namespace})
    if environment.workspace.resolve() != plan.accepted_repository.resolve():
        raise ValueError(f"eval: lane {name} does not use the selected accepted repository")
    if environment.parallel_lane != name:
        raise ValueError(f"eval: lane identity differs for {name}")
    assert_harness_sources(environment.sregym_dir)
    assert_shared_lane(environment)


def execute_lane(
    name: str, directory: Path, encounters: Sequence[Encounter], plan: ParallelPlan
) -> list[IncidentRecord]:
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    run_dir = plan.lanes[name].resolve()
    environment = FastloopEnvironment.load(run_dir)
    run_id = f"{plan.run_id}-{name}"
    results = run_dir / "results" / run_id
    if results.exists():
        raise ValueError(f"eval: refusing to replay existing lane results: {results}")
    directory.mkdir(parents=True, exist_ok=True)
    stream = directory / "stream.json"
    stream.write_text(
        json.dumps({"incidents": [[e.problem] for e in encounters], "diagnosis_grading": "inline"}) + "\n"
    )
    command = [
        sys.executable,
        "-m",
        "benchmarks.sregym.fastloop",
        "run",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--agent",
        "sdo",
        "--stream",
        str(stream),
        "--timeout",
        str(plan.timeout_seconds),
        "--healthy-baseline",
        "--inject-before-resume",
        "--require-full-cleanup",
        "--cleanup-passes",
        str(EVAL_CLEANUP_PASSES),
    ]
    if plan.agent_registrations:
        command.extend(["--agent-registrations", str(plan.agent_registrations.resolve())])
    jobs = max(1, 8 // len(plan.lanes))
    child_environment = {
        **os.environ,
        "KUBECONFIG": str(environment.kubeconfig),
        "SREGYM_BASE_KUBECONFIG": str(environment.kubeconfig),
        "GOMAXPROCS": str(jobs),
        "GOFLAGS": f"{os.environ.get('GOFLAGS', '')} -p={jobs}".strip(),
    }
    with (directory / "worker.log").open("w") as log:
        completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=False, env=child_environment)
    path = results / "incidents.jsonl"
    rows = load_records(path) if path.exists() else []
    if completed.returncode or len(rows) != len(encounters):
        # Preserve every completed encounter even if a later encounter failed.
        write_records(directory / "incidents.jsonl", rows)
        raise RuntimeError(f"eval: lane {name} exited {completed.returncode}; completed {len(rows)}/{len(encounters)}")
    return rows


def run_parallel(
    plan: ParallelPlan,
    output: Path,
    *,
    preflight: Callable[[str, Path, ParallelPlan], None] = preflight_lane,
    execute: Callable[[str, Path, Sequence[Encounter], ParallelPlan], list[IncidentRecord]] = execute_lane,
) -> list[IncidentRecord]:
    """Preflight all lanes, join every child, and durably preserve successful peers on failure."""
    if output.exists():
        raise ValueError(f"eval: output already exists: {output}")
    for name, run_dir in plan.lanes.items():
        preflight(name, run_dir, plan)
    output.mkdir(parents=True)
    (output / "parallel_plan.json").write_text(plan.model_dump_json(indent=2) + "\n")
    rows: list[IncidentRecord] = []
    errors: dict[str, str] = {}
    incident_ids: set[str] = set()
    by_lane = {name: [e for e in plan.encounters if e.lane == name] for name in plan.lanes}
    positions = {name: [i for i, e in enumerate(plan.encounters) if e.lane == name] for name in plan.lanes}
    with ThreadPoolExecutor(max_workers=len(plan.lanes)) as pool:
        futures = {
            pool.submit(execute, name, output / "lanes" / name, by_lane[name], plan): name for name in plan.lanes
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                lane_rows = future.result()
            except Exception as exc:
                errors[name] = f"{type(exc).__name__}: {exc}"
                path = output / "lanes" / name / "incidents.jsonl"
                lane_rows = load_records(path) if path.exists() else []
            if len(lane_rows) != len(by_lane[name]):
                errors[name] = errors.get(name, f"lane returned {len(lane_rows)}/{len(by_lane[name])} encounters")
            for position, expected, row in zip(positions[name], by_lane[name], lane_rows, strict=False):
                if row.problem_id != expected.problem:
                    errors[name] = "lane returned mismatched encounter identity"
                    continue
                if row.incident_id:
                    if row.incident_id in incident_ids:
                        errors[name] = f"duplicate incident identity: {row.incident_id}"
                        continue
                    incident_ids.add(row.incident_id)
                row.index, row.lane_id = position, name
                rows.append(row)
                if row.error or row.infra_error or row.recovery_error:
                    errors[name] = f"encounter {position} failed; inspect its result row"
            rows.sort(key=lambda row: row.index)
            write_records(output / "incidents.jsonl", rows)
            (output / "parallel_result.json").write_text(
                json.dumps(
                    {
                        "passed": not errors and len(rows) == len(plan.encounters),
                        "completed": len(rows),
                        "requested": len(plan.encounters),
                        "errors": errors,
                    },
                    indent=2,
                )
                + "\n"
            )
    if errors:
        raise RuntimeError(f"eval: parallel lanes failed: {errors}")
    return rows
