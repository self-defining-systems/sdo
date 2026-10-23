"""Scripted image builds use the same builder as the SDO runtime images."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]


def test_runtime_and_validator_images_carry_the_prebuilt_go_cache() -> None:
    runtime = (ROOT / "sdo/operator/Dockerfile.runtime").read_text(encoding="utf-8")
    validator = (ROOT / "sdo/operator/Dockerfile.validator").read_text(encoding="utf-8")
    assert "COPY --from=go-deps /go/build-cache /opt/sdo/go-build-cache" in runtime
    assert "COPY --from=go-runtime /go/build-cache-delta/ /opt/sdo/go-build-cache/" in runtime
    assert "SDO_GO_CACHE_SEED=/opt/sdo/go-build-cache" in runtime
    assert "SDO_GO_CACHE_SEED=/go/build-cache" in validator


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
    assert "--file " + str(ROOT / "benchmarks/sregym/Dockerfile.worker") in scripted[0]
    assert "--target scripted-worker" in scripted[0]
    assert "--build-arg SDO_RESPONDER_IMAGE=sdo-responder:level1" in scripted[0]


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
