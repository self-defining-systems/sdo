"""Offline tracking gate for an entire experiment plan, before any lane work."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

from benchmarks.sregym.fastloop.stream import load_stream
from benchmarks.sregym.faults.description import describe_sregym_problems, validate_suite


def plan_incidents(path: Path) -> tuple[tuple[str, ...], ...]:
    """Accept canonical sequence/draw plans and ordinary fastloop streams."""
    data: Any = json.loads(path.read_text())
    if isinstance(data, dict) and "problems" in data:
        data = data["problems"]
    if isinstance(data, list) and data and all(isinstance(row, dict) for row in data):
        ids = tuple(row.get("problem_id", row.get("id")) for row in data)
        if any(not isinstance(item, str) or not item.strip() for item in ids):
            raise ValueError("plan rows require non-empty problem_id or id")
        return tuple((problem_id,) for problem_id in ids)
    return load_stream(path)


def plan_problem_ids(path: Path) -> tuple[str, ...]:
    return tuple(dict.fromkeys(problem_id for incident in plan_incidents(path) for problem_id in incident))


def check_plan(path: Path, *, sregym_dir: Path, environment: FastloopEnvironment | None = None) -> dict[str, Any]:
    incidents = plan_incidents(path)
    ids = tuple(dict.fromkeys(problem_id for incident in incidents for problem_id in incident))
    descriptions = describe_sregym_problems(ids, sregym_dir=sregym_dir)
    if set(descriptions) != set(ids):
        raise ValueError("plan fault discovery returned incomplete problem coverage")
    lane_report = {}
    if environment is not None:
        from benchmarks.sregym.fastloop.lane_installability import LaneTopology, lane_plan

        decisions = lane_plan(descriptions, LaneTopology.of(environment), sregym_dir)
        lane_report = {key: asdict(item) for key, item in decisions.items()}
        descriptions = {key: item for key, item in descriptions.items() if decisions[key].status == "runnable"}
    # This checks observability, independently of production repair permissions.
    # Permissive capability mode never weakens the required tracking gate.
    validate_suite(descriptions, require_tracking=True, capability_mode="permissive")
    for incident in incidents:
        if any(problem_id not in descriptions for problem_id in incident):
            continue
        names = [fault.name for problem_id in incident for fault in descriptions[problem_id].faults]
        if len(names) != len(set(names)):
            raise ValueError(f"{'+'.join(incident)}: per-fault tracking has duplicate probe identities")
    return {
        "problems": len(descriptions),
        "faults": sum(len(item.faults) for item in descriptions.values()),
        **({"lane_plan": lane_report, "runnable_problem_ids": list(descriptions)} if environment is not None else {}),
    }
