"""The agent image pins the Python packages the checked-in code was locked against.

The images install their Python dependencies with pip, outside ``uv``. A pin
that falls behind ``uv.lock`` builds an image whose code fails to import (for
example agentshim 0.6.8 lacks the 0.7 usage API that ``libs/agent_cli`` uses).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import tomllib

REPO_ROOT = Path(__file__).resolve().parents[4]
PYTHON_DOCKERFILES = ("sdo/operator/Dockerfile.runtime",)
ALL_DOCKERFILES = (*PYTHON_DOCKERFILES, "benchmarks/sregym/Dockerfile.worker")
_PIN = re.compile(r"^\s+([A-Za-z0-9_.-]+)==([0-9][^\s\\]*)", re.MULTILINE)


def _locked_versions() -> dict[str, set[str]]:
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text(encoding="utf-8"))
    versions: dict[str, set[str]] = {}
    for package in lock["package"]:
        versions.setdefault(package["name"].lower().replace("_", "-"), set()).add(package["version"])
    return versions


@pytest.mark.parametrize("dockerfile", PYTHON_DOCKERFILES)
def test_image_pip_pins_match_the_lockfile(dockerfile: str) -> None:
    pins = _PIN.findall((REPO_ROOT / dockerfile).read_text(encoding="utf-8"))
    assert pins, f"{dockerfile} pins no Python packages"
    locked = _locked_versions()
    drift = {
        name: (version, sorted(locked.get(name.lower().replace("_", "-"), set())))
        for name, version in pins
        if version not in locked.get(name.lower().replace("_", "-"), set())
    }
    assert drift == {}, f"{dockerfile} pins differ from uv.lock: {drift}"


#: The interpreter the worker image installs packages into (Debian bookworm's python3).
IMAGE_PYTHON = {"sdo/operator/Dockerfile.runtime": "3.11"}
_PIP_INSTALL = re.compile(r"pip install(?P<args>(?:[^\n]*\\\n)*[^\n]*)")
_FROM = re.compile(r"^FROM\s+(\S+)", re.MULTILINE)
_NPM_INSTALL = re.compile(r"npm install(?P<args>[^\n&]*)")
_SEMVER = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")


def _lock_graph() -> dict[str, list[dict[str, str]]]:
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text(encoding="utf-8"))
    graph: dict[str, list[dict[str, str]]] = {}
    for package in lock["package"]:
        graph.setdefault(package["name"].lower().replace("_", "-"), []).extend(package.get("dependencies", []))
    return graph


def _closure(roots: set[str], python: str) -> set[str]:
    from packaging.markers import Marker

    environment = {
        "sys_platform": "linux",
        "platform_system": "Linux",
        "os_name": "posix",
        "platform_machine": "x86_64",
        "implementation_name": "cpython",
        "platform_python_implementation": "CPython",
        "python_version": python,
        "python_full_version": f"{python}.0",
        "extra": "",
    }
    graph = _lock_graph()
    seen: set[str] = set()
    pending = list(roots)
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        for dependency in graph.get(name, []):
            marker = dependency.get("marker")
            if marker and not Marker(marker).evaluate(environment):
                continue
            pending.append(dependency["name"].lower().replace("_", "-"))
    return seen


@pytest.mark.parametrize("dockerfile", PYTHON_DOCKERFILES)
def test_image_pins_every_transitive_python_dependency(dockerfile: str) -> None:
    text = (REPO_ROOT / dockerfile).read_text(encoding="utf-8")
    pinned = {name.lower().replace("_", "-") for name, _ in _PIN.findall(text)}

    unpinned = _closure(pinned, IMAGE_PYTHON[dockerfile]) - pinned

    assert unpinned == set(), f"{dockerfile} lets pip resolve {sorted(unpinned)} freely; pin them from uv.lock"


@pytest.mark.parametrize("dockerfile", PYTHON_DOCKERFILES)
def test_image_pip_installs_never_resolve_unpinned_packages(dockerfile: str) -> None:
    installs = [match.group("args") for match in _PIP_INSTALL.finditer((REPO_ROOT / dockerfile).read_text())]

    assert installs
    assert all("--no-deps" in args for args in installs), f"{dockerfile}: every pip install needs --no-deps"


@pytest.mark.parametrize("dockerfile", ALL_DOCKERFILES)
def test_external_base_images_are_pinned_by_digest(dockerfile: str) -> None:
    text = (REPO_ROOT / dockerfile).read_text(encoding="utf-8")
    stages = set(re.findall(r"^FROM\s+\S+\s+AS\s+(\S+)", text, re.MULTILINE))

    floating = [
        image
        for image in _FROM.findall(text)
        if image not in stages and not image.startswith("${") and "@sha256:" not in image
    ]

    assert floating == []


def test_validator_image_contains_no_python_dependency_install() -> None:
    text = (
        (REPO_ROOT / "sdo/operator/Dockerfile.runtime")
        .read_text(encoding="utf-8")
        .split(" AS validator\n", 1)[1]
        .split("\nFROM ", 1)[0]
    )
    assert "pip install" not in text
    assert "python3" not in text
    assert "COPY --from=go-runtime /usr/local/bin/sdo-operator" in text


def test_worker_image_ships_offline_program_example_and_toolchain() -> None:
    dockerfile = (REPO_ROOT / "sdo/operator/Dockerfile.runtime").read_text(encoding="utf-8")
    assert "COPY examples/playbook-programs /opt/sdo/examples/playbook-programs" in dockerfile
    assert "COPY --from=go-deps /usr/local/go /usr/local/go" in dockerfile
    assert "COPY --from=go-runtime /usr/local/bin/sdo-operator /usr/local/bin/sdo-operator" in dockerfile
    assert "COPY --from=go-deps /go/pkg/mod /go/pkg/mod" in dockerfile
    assert "COPY --from=go-runtime /opt/sdo/sdo/operator /opt/sdo/sdo/operator" in dockerfile
    assert "GOPROXY=off" in dockerfile
    assert 'GOFLAGS="-mod=mod -p=2"' in dockerfile
    assert "SDO_PLAYBOOK_PROGRAM_EXAMPLE_DIR=/opt/sdo/examples/playbook-programs" in dockerfile
    assert "! command -v go" in (REPO_ROOT / "scripts/build_sdo_images.sh").read_text()
    for relative in (
        "playbooks/service-targetport-mismatch/diagnose.go",
        "playbooks/service-targetport-mismatch/mitigate.go",
        "lib/ports/ports.go",
        "corpus/targetport-mismatch/open.json",
        "corpus/targetport-mismatch/fixed.json",
        "corpus/targetport-mismatch/healthy.json",
        "corpus/targetport-mismatch/recordings.json",
        "corpus/targetport-mismatch/labels.json",
    ):
        assert (REPO_ROOT / "examples/playbook-programs" / relative).is_file(), relative


def test_validator_image_uses_its_immutable_offline_toolchain() -> None:
    runtime = (REPO_ROOT / "sdo/operator/Dockerfile.runtime").read_text()
    assert 'cd "$dir" && go mod download all' in runtime
    assert "GOPROXY=off" in runtime
    assert 'GOFLAGS="-mod=mod -p=2"' in runtime
    assert "FROM validator-toolchain AS validator" in runtime
    assert "ENV GOCACHEPROG=/usr/local/bin/sdo-go-cache" in runtime


def test_health_refresh_forwards_the_typed_controller_toolchain() -> None:
    # This launch wiring must hold before an in-cluster judge can compile health.
    source = (REPO_ROOT / "sdo/operator/operatorcli/health_refresh.go").read_text()
    assert re.search(r"ToolchainImage:\s*config\.GetToolchainImage\(\)", source)


def test_image_build_smokes_offline_go_tools_as_nonroot() -> None:
    script = (REPO_ROOT / "scripts/build_sdo_images.sh").read_text(encoding="utf-8")
    for image in ("sdo-detector-validator", "sdo-controller", "sdo-responder", "sdo-sregym-responder"):
        assert image in script
    assert "--user 65532:65532" in script
    assert "command -v go" in script
    assert "command -v sdo-operator" in script
    assert "go list ./program" in script
    assert "tests/fixtures/sdo/playbook_program_app:/fixture:ro" in script
    assert "sdo-operator validate --app /fixture --expect-schema" in script


def test_image_npm_installs_are_pinned_to_exact_versions() -> None:
    text = (REPO_ROOT / "sdo" / "operator" / "Dockerfile.runtime").read_text(encoding="utf-8")
    defaults = dict(re.findall(r"^ARG (\w+)=(\S+)$", text, re.MULTILINE))
    specs = [spec.strip('"') for match in _NPM_INSTALL.finditer(text) for spec in match.group("args").split()]
    packages = [spec for spec in specs if spec != "\\" and not spec.startswith("-")]

    assert packages
    for spec in packages:
        name, _, version = spec.rpartition("@")
        assert name, f"npm package {spec} has no version"
        arg = re.fullmatch(r"\$\{(\w+)\}", version)
        resolved = defaults.get(arg.group(1), "") if arg else version
        assert _SEMVER.match(resolved), f"npm package {spec} resolves to {resolved!r}, not an exact version"


def test_ci_pins_actions_to_commits_and_installs_from_the_lockfile() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    actions = re.findall(r"uses:\s*(\S+)", workflow)

    assert actions
    assert [action for action in actions if not re.search(r"@[0-9a-f]{40}$", action)] == []
    syncs = re.findall(r"uv sync[^\n]*", workflow)
    assert syncs
    assert all("--locked" in sync for sync in syncs)
    setup_uv = re.findall(r"setup-uv@[0-9a-f]{40}.*?(?=\n\s+- |\Z)", workflow, re.DOTALL)
    assert setup_uv
    assert all(re.search(r"\n\s+version:\s*\"?\d+\.\d+\.\d+", block) for block in setup_uv)
