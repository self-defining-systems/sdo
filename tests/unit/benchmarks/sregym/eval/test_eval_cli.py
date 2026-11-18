"""The dry run drives the real scheduler, cell machine and files with no cluster."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.eval.__main__ import main
from benchmarks.sregym.eval.results import verify_manifest

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def write_config(tmp_path: Path) -> Path:
    path = tmp_path / "eval.toml"
    path.write_text(
        f"""
stage = "dry"
runs_root = "{tmp_path / "runs"}"
seed = 3
admit_spacing_seconds = 0

[[apps]]
name = "hotel"
problems = ["a", "b", "c"]
sdo_lanes = ["h1", "h2"]
vanilla_lanes = ["v1"]
"""
    )
    return path


def test_plan_writes_plan_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["plan", "--config", str(write_config(tmp_path))]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["cells"] == 12
    assert (tmp_path / "runs" / "dry" / "plan.json").is_file()


def test_dry_run_completes_every_cell_with_verified_manifests(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["dry-run", "--config", str(write_config(tmp_path))]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary == {"cells": 12, "graded": 12, "results": str(tmp_path / "runs" / "dry" / "results.csv")}
    cell = tmp_path / "runs" / "dry" / "hotel" / "pass2-own" / "a"
    assert verify_manifest(cell) == []
    timings = json.loads((cell / "timings.json").read_text())
    assert set(timings["phases"]) == {"claim_reset", "deploy", "inject", "page", "agent", "settle", "grade", "collect"}
