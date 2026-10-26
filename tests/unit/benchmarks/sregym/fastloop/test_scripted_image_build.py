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


def test_scripted_worker_build_uses_runtime_target_and_tag(tmp_path: Path, image_build_source: Path) -> None:
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
        cwd=image_build_source,
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


def test_scripted_image_copy_set_can_import_both_replay_entry_points(tmp_path: Path) -> None:
    """Model Docker COPY against an empty filesystem; no Docker/cluster is used."""
    import shlex
    import shutil
    import sys

    dockerfile = ROOT / "benchmarks/sregym/assurance/Dockerfile.scripted"
    for line in dockerfile.read_text().splitlines():
        if not line.startswith("COPY ") or "--from=" in line:
            continue
        *sources, destination = shlex.split(line)[1:]
        target = tmp_path / destination.lstrip("/")
        for source in sources:
            source_path = ROOT / source
            if source_path.is_dir():
                shutil.copytree(source_path, target, dirs_exist_ok=True)
            else:
                target.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, target / source_path.name)
    # The production base supplies these packages and installed dependencies.
    (tmp_path / "opt/sdo").mkdir(parents=True, exist_ok=True)
    (tmp_path / "opt/sdo/sdo").symlink_to(ROOT / "sdo", target_is_directory=True)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import scripted_codex.cli; import benchmarks.sregym.adapter.scripted_worker as worker; "
            "from pathlib import Path; Path(worker.__file__).relative_to(Path.cwd()); "
            "from benchmarks.sregym.adapter import SCRIPTED_REPLAY_SLOTS; assert len(SCRIPTED_REPLAY_SLOTS) == 10",
        ],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": f"{tmp_path}/opt/sdo:{tmp_path}/opt/sdo-scripted"},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    worker = (ROOT / "benchmarks/sregym/Dockerfile.worker").read_text()
    production, scripted = worker.split("AS scripted-worker", 1)
    assert "scripted_replay_catalog.py" not in production
    assert "scripted_replay.py" in scripted
    assert "scripted_replay_catalog.py" in scripted
