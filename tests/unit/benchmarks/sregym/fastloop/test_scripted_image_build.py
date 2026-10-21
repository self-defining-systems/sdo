"""Scripted image builds use the same builder as the SDO runtime images."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]


def test_scripted_worker_build_uses_runtime_target_and_tag(tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "docker-calls"
    docker = bin_dir / "docker"
    docker.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$SDO_DOCKER_CALLS"\n', encoding="utf-8")
    docker.chmod(0o755)
    go = bin_dir / "go"
    go.write_text('#!/bin/sh\nprintf "schema-test\\n"\n', encoding="utf-8")
    go.chmod(0o755)
    result = subprocess.run(
        ["bash", "scripts/build_sdo_images.sh"],
        cwd=ROOT,
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "SDO_DOCKER_CALLS": str(calls),
            "SDO_SCRIPTED_WORKER": "1",
            "SDO_IMAGE_TAG": "level1",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    builds = [line for line in calls.read_text(encoding="utf-8").splitlines() if line.startswith("build ")]
    scripted = [line for line in builds if "--tag sdo-sregym-responder:level1" in line]
    assert len(scripted) == 1
    assert "--file " + str(ROOT / "sdo/operator/Dockerfile.runtime") in scripted[0]
    assert "--target scripted-worker" in scripted[0]
    assert "--build-arg BASE=" not in scripted[0]
