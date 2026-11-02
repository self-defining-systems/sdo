from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from scripts.operator_test_binary import build_operator, source_identity, verify_operator
from tests.reflection_memory import (
    memory_binary as memory_binary,
)
from tests.reflection_memory import (
    prepare_reflection_memory as prepare_reflection_memory,
)

pytest_plugins = ("tests.sregym_prerequisites",)

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # This shared fixture invokes the real memory validator, which compiles
    # authored programs. Route its transitive consumers before -m selection.
    for item in items:
        if "memory_binary" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.go_integration)


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_protocol(item: pytest.Item) -> Iterator[None]:
    # Surround fixture setup too: a module/session fixture can compile before
    # function-scoped autouse fixtures have started.
    with pytest.MonkeyPatch.context() as patch:
        if item.get_closest_marker("go_integration") is None:
            guard = Path(__file__).resolve().parents[1] / "scripts/unit-tools"
            go = shutil.which("go")
            if go == str(guard / "go"):
                go = os.environ.get("SDO_UNIT_REAL_GO")
            if go:
                patch.setenv("SDO_UNIT_REAL_GO", go)
                patch.setenv("PATH", f"{guard}{os.pathsep}{os.environ['PATH']}")
        yield


@pytest.fixture
def image_build_source(tmp_path: Path) -> Path:
    """Real committed inputs independent of edits in the developer checkout."""
    repository = Path(__file__).resolve().parents[1]
    source = tmp_path / "image-source"
    (source / "scripts").mkdir(parents=True)
    (source / "sdo/operator").mkdir(parents=True)
    (source / "sdo/operator/go.mod").write_text("module fixture\n")
    for path in (
        "scripts/build_sdo_images.sh",
        "scripts/sdo-images.hcl",
        "sdo/operator/Dockerfile.runtime",
        ".dockerignore",
        "benchmarks/sregym/fastloop/image_provenance.py",
        "benchmarks/sregym/repository_git.py",
    ):
        (source / path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(repository / path, source / path)
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


@pytest.fixture(autouse=True)
def _isolated_opus_pool(monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory) -> None:
    """Fake-agent tests share a real temporary lock pool, never the live account pool."""
    # tmp_path itself is often the proposal repository. Coordination metadata
    # belongs in a sibling directory, outside every agent-authored checkout.
    monkeypatch.setenv("SDO_OPUS_POOL_DIR", str(tmp_path_factory.mktemp("provider-capacity")))
    monkeypatch.setenv("SDO_OPUS_MAX_SESSIONS", "8")


@pytest.fixture(scope="session")
def operator_binary() -> Path:
    repository = Path(__file__).resolve().parents[1]
    prepared = os.environ.get("SDO_OPERATOR_BIN")
    if prepared:
        binary = Path(prepared)
        verify_operator(binary, source_identity(repository / "sdo/operator"))
        return binary
    return build_operator(repository / "sdo/operator", repository / ".fastloop/test-bin")


@pytest.fixture(autouse=True)
def _no_host_codex_login(monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory) -> None:
    """Hide the developer's Codex login and API keys from unit tests.

    A test that silently used ``~/.codex/auth.json`` passed on a logged-in host
    and failed on CI. Tests that need a login use ``fake_codex_login``; opt-in
    live-agent tests keep the real credentials.
    """

    if os.environ.get("SDO_RUN_LIVE_AGENTS"):
        return
    monkeypatch.setenv("CODEX_HOME", str(tmp_path_factory.getbasetemp() / "no-host-codex-login"))
    for name in ("CODEX_API_KEY", "OPENAI_API_KEY"):
        monkeypatch.delenv(name, raising=False)


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
    link = subprocess.check_output(["git", "ls-tree", "HEAD", "third_party/sregym"], cwd=repository, text=True).split()[
        2
    ]
    return subprocess.check_output(["git", "show", f"{link}:agents.yaml"], cwd=harness, text=True)
