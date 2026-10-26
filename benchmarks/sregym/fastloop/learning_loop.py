"""Real-lane, model-free learning-loop proof and fail-closed live admission."""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.fastloop.dry_run import code_revision

if TYPE_CHECKING:
    import argparse
    from pathlib import Path

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

SCHEMA = "sdo.learning-loop-proof/v1"
STAGES = ("injection", "closure", "grade", "publication", "recovery", "repeat")
REPORT = "learning-loop-proof/report.json"


def scripted_agents(args: argparse.Namespace, environment: FastloopEnvironment) -> bool:
    from benchmarks.sregym.fastloop.cli import REPO_ROOT

    registrations = getattr(args, "agent_registrations", None)
    return args.agent == "sdo" and (
        registrations.resolve() == (REPO_ROOT / "benchmarks/sregym/adapter/scripted_registrations").resolve()
        if registrations is not None
        else environment.app == "toy"
    )


def image_identity(environment: FastloopEnvironment) -> dict[str, dict[str, str]]:
    """Resolve the installed image content on every lane node, including mutable tags."""
    nodes = subprocess.run(
        ["kind", "get", "nodes", "--name", environment.cluster],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    ).stdout.splitlines()
    if not nodes:
        raise ValueError("learning-loop: lane has no nodes")
    result: dict[str, dict[str, str]] = {}
    for role, reference in environment.images.model_dump().items():
        result[role] = {}
        for node in nodes:
            inspected = subprocess.run(
                ["docker", "exec", node, "crictl", "inspecti", reference],
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            )
            digest = json.loads(inspected.stdout)["status"]["id"]
            if not isinstance(digest, str) or not digest.startswith("sha256:"):
                raise ValueError(f"learning-loop: {role} on {node} has no content digest")
            result[role][node] = digest
    return result


def validate_report(report: dict[str, Any], *, revision: str, lane: str, images: dict[str, Any]) -> None:
    """A proof is usable only for the exact measured launch inputs and every stage."""
    expected = {
        "schema_version": SCHEMA,
        "passed": True,
        "code_revision": revision,
        "lane": lane,
        "images": images,
        "live_model_calls": False,
    }
    for key, value in expected.items():
        if report.get(key) != value:
            raise ValueError(f"learning-loop: missing or mismatched {key}")
    seconds = report.get("seconds")
    if type(seconds) not in (int, float) or not 0 < seconds <= 300:
        raise ValueError("learning-loop: proof must complete within 300 seconds")
    stages = report.get("stages")
    for stage in STAGES:
        row = stages.get(stage) if isinstance(stages, dict) else None
        if not isinstance(row, dict) or row.get("passed") is not True or not row.get("evidence"):
            raise ValueError(f"learning-loop: {stage} has no passing evidence")


def require_proof(environment: FastloopEnvironment, path: Path | None = None) -> None:
    path = path or environment.run_dir / REPORT
    command = f"uv run python -m benchmarks.sregym.fastloop learning-loop-proof --run-dir {environment.run_dir}"
    try:
        report = json.loads(path.read_text())
        validate_report(report, revision=code_revision(), lane=environment.cluster, images=image_identity(environment))
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        raise ValueError(f"learning-loop preflight refused live run: {exc}; run: {command}") from exc
