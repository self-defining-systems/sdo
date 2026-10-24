"""Scripted image builds use the same builder as the SDO runtime images."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]


def test_compiler_cache_lives_only_in_controller_image() -> None:
    runtime = (ROOT / "sdo/operator/Dockerfile.runtime").read_text()
    controller = runtime.split(" AS controller\n")[1].split("\nFROM ")[0]
    assert "COPY --from=go-deps /go/build-cache /go/build-cache" in controller
    assert "COPY --from=go-runtime /go/build-cache-delta/ /go/build-cache/" in controller
    assert "SDO_GO_CACHE_SEED=/go/build-cache" in runtime


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
    builds = [line for line in calls.read_text().splitlines() if line.startswith("buildx bake ")]
    assert len(builds) == 1
    assert "--file scripts/sdo-images.hcl --load controller validator responder benchmark" in builds[0]
    graph = (ROOT / "scripts/sdo-images.hcl").read_text()
    assert "target = SDO_WORKER_TARGET" in graph
    assert 'scripted-base = "target:scripted-runtime"' in graph
