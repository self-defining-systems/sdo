"""Durable row-generation provenance produced by the completed native reset."""

from __future__ import annotations

import json
import os
import subprocess
from typing import TYPE_CHECKING, TypedDict, cast
from uuid import UUID, uuid4

from benchmarks.sregym.fastloop.parallel_memory import memory_head

if TYPE_CHECKING:
    from pathlib import Path

RESET_GENERATION = "reset-generation.json"


class ResetGeneration(TypedDict):
    schema: str
    run_id: str
    generation: str
    workspace: str
    planned_commit: str
    pre_reset_head: str
    accepted_before: str


def publish_reset_generation(path: Path, workspace: Path, planned: str, before: str) -> ResetGeneration:
    """Publish only after a successful reset; a requested SHA is not an observation."""
    actual = memory_head(workspace)
    expected = subprocess.check_output(
        ["git", "-C", str(workspace), "rev-parse", "--verify", f"{planned}^{{commit}}"], text=True
    ).strip()
    if actual != expected:
        raise ValueError("eval: reset accepted head differs from planned commit")
    receipt: ResetGeneration = {
        "schema": "sdo.fastloop-reset-generation/v1",
        "run_id": path.parent.name,
        "generation": str(uuid4()),
        "workspace": str(workspace.resolve()),
        "planned_commit": expected,
        "pre_reset_head": before,
        "accepted_before": actual,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise ValueError("eval: reset generation already published; never overwrite native evidence")
    temporary = path.with_suffix(".tmp")
    with temporary.open("x") as handle:
        handle.write(json.dumps(receipt, indent=2) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return receipt


def read_reset_generation(path: Path, workspace: Path) -> ResetGeneration:
    """Reject absent, incomplete or foreign reset facts rather than infer a start."""
    receipt = json.loads(path.read_text())
    if (
        not isinstance(receipt, dict)
        or receipt.get("schema") != "sdo.fastloop-reset-generation/v1"
        or receipt.get("run_id") != path.parent.name
        or receipt.get("workspace") != str(workspace.resolve())
        or any(not isinstance(receipt.get(key), str) or not receipt[key] for key in ResetGeneration.__required_keys__)
        or not receipt.get("generation")
        or not receipt.get("accepted_before")
        or receipt.get("planned_commit") != receipt.get("accepted_before")
        or not receipt.get("pre_reset_head")
    ):
        raise ValueError("eval: reset generation identity or accepted memory is incomplete")
    UUID(receipt["generation"])
    for key in ("accepted_before", "pre_reset_head"):
        value = receipt[key]
        if len(value) != 40 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError("eval: reset generation accepted memory is not a commit SHA")
    return cast("ResetGeneration", receipt)
