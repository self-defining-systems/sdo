from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from scripts.operator_test_binary import build_operator
from tests.reflection_memory import (
    memory_binary as memory_binary,
)
from tests.reflection_memory import (
    prepare_reflection_memory as prepare_reflection_memory,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def image_build_source(tmp_path: Path) -> Path:
    """Real committed inputs independent of edits in the developer checkout."""
    repository = Path(__file__).resolve().parents[1]
    source = tmp_path / "image-source"
    (source / "scripts").mkdir(parents=True)
    (source / "sdo/operator").mkdir(parents=True)
    (source / "sdo/operator/go.mod").write_text("module fixture\n")
    for filename in ("build_sdo_images.sh", "sdo-images.hcl"):
        shutil.copy2(repository / "scripts" / filename, source / "scripts" / filename)
    for args in (
        ["init", "-q"],
        ["add", "."],
        [
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.com",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-qm",
            "image inputs",
        ],
    ):
        subprocess.run(["git", "-C", str(source), *args], check=True, capture_output=True, text=True)
    return source


@pytest.fixture(scope="session")
def operator_binary() -> Path:
    repository = Path(__file__).resolve().parents[1]
    return build_operator(repository / "sdo/operator", repository / ".fastloop/test-bin")


@pytest.fixture(scope="session", autouse=True)
def _warm_shared_go_caches() -> Iterator[None]:
    """Point the real-compile detector-sandbox tests at a warm, shared Go cache.

    ``LocalSandboxRunner`` isolates each validation in a throwaway ``GOCACHE``,
    so every detector-sandbox test used to pay a full cold Go build (compiling
    the stdlib, protovalidate, and the SDK -- ~150-220s each, dominating the
    unit suite). Exporting a shared ``GOCACHE``/``GOMODCACHE`` for the session
    turns all but the first such build into an incremental one, and lets the
    repeated ``go mod tidy`` reuse a single module cache instead of
    re-downloading per test.

    We reuse the host's default Go caches (already warm on a developer machine;
    populated once and restored from ``actions/cache`` on CI). Anything the
    caller already exported wins, so CI can pin explicit paths. Set
    ``SDO_TEST_NO_SHARED_GO_CACHE=1`` to opt out and reproduce a cold build.
    """

    if os.environ.get("SDO_TEST_NO_SHARED_GO_CACHE"):
        yield
        return
    go = shutil.which("go")
    if go is None:
        yield
        return

    previous = {name: os.environ.get(name) for name in ("GOCACHE", "GOMODCACHE")}
    for name in ("GOCACHE", "GOMODCACHE"):
        if os.environ.get(name):
            continue
        try:
            resolved = subprocess.run(
                [go, "env", name],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            resolved = ""
        if resolved:
            os.environ[name] = resolved
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


@pytest.fixture(scope="session")
def committed_sregym_registry() -> str:
    """The registry at this repository's committed gitlink, independent of worktree HEAD."""
    repository = Path(__file__).resolve().parents[1]
    harness = repository / "third_party/sregym"
    if not (harness / "agents.yaml").is_file():
        pytest.skip("requires the optional external SREGym registry fixture")
    link = subprocess.check_output(["git", "ls-tree", "HEAD", "third_party/sregym"], cwd=repository, text=True).split()[
        2
    ]
    return subprocess.check_output(["git", "show", f"{link}:agents.yaml"], cwd=harness, text=True)
