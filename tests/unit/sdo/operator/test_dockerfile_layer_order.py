"""Go source edits must not invalidate the module, dependency-cache, or package-manager layers."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
OPERATOR = REPO_ROOT / "sdo" / "operator"
DOCKERFILES = ("Dockerfile.runtime",)
FULL_SOURCE_COPY = "COPY sdo/operator ./sdo/operator"


def _lines(name: str) -> list[str]:
    return (OPERATOR / name).read_text(encoding="utf-8").splitlines()


@pytest.mark.parametrize("name", DOCKERFILES)
def test_the_shipped_sources_are_copied_after_every_stable_layer(name: str) -> None:
    lines = _lines(name)
    copies = [index for index, line in enumerate(lines) if line.strip() == FULL_SOURCE_COPY]
    # The first full copy feeds the dependency-list stage, which only emits package names;
    # the second one starts the compile steps of the module being shipped.
    assert len(copies) == 2
    before = "\n".join(lines[: copies[1]])
    compiled = "\n".join(lines[copies[1] :]).split("\nFROM ", 1)[0]
    for stable in ("go mod download all", "go build $(cat", "apt-get install"):
        assert stable in before
        assert stable not in compiled


@pytest.mark.parametrize("name", DOCKERFILES)
def test_modules_are_downloaded_from_manifests_alone(name: str) -> None:
    text = "\n".join(_lines(name))
    download = text.index("go mod download all")
    assert text.index(FULL_SOURCE_COPY) > download
    for module in ("", "contracts/", "core/", "runtime/", "sdk/"):
        assert f"COPY sdo/operator/{module}go.mod sdo/operator/{module}go.sum" in text


def test_every_operator_module_is_downloaded_from_its_manifest() -> None:
    modules = {path.parent.relative_to(OPERATOR).as_posix() for path in OPERATOR.rglob("go.mod")}
    for name in DOCKERFILES:
        text = "\n".join(_lines(name))
        copied = set(re.findall(r"COPY sdo/operator/(?:(\w+)/)?go\.mod", text))
        assert {module or "." for module in copied} == modules


def test_controller_and_validator_use_the_stable_dependency_cache() -> None:
    text = "\n".join(_lines("Dockerfile.runtime"))
    assert "COPY --from=go-deps /go/build-cache /go/build-cache" in text
    assert "COPY --from=go-runtime /go/build-cache-delta/ /go/build-cache/" in text


def test_image_builds_run_concurrently_and_still_smoke_check_each_image() -> None:
    script = (REPO_ROOT / "scripts" / "build_sdo_images.sh").read_text(encoding="utf-8")
    assert "docker buildx bake" in script
    assert script.count("docker run --rm") >= 6
    syntax = subprocess.run(["bash", "-n", str(REPO_ROOT / "scripts" / "build_sdo_images.sh")], check=False)
    assert syntax.returncode == 0


def test_worker_targets_share_the_compiler_free_base():
    text = "\n".join(_lines("Dockerfile.runtime"))
    stages = {}
    current = None
    for line in text.splitlines():
        match = re.match(r"FROM (\S+) AS (\S+)", line)
        if match:
            current = match[2]
            stages[current] = (match[1], [])
        elif current:
            stages[current][1].append(line)
    for target in ("responder", "scripted-runtime"):
        stage = target
        lineage = []
        while stage in stages:
            lineage.append(stage)
            parent, lines = stages[stage]
            assert not parent.startswith("golang:")
            assert not any(
                "COPY --from=" in line
                and any(path in line for path in ("/usr/local/go", "/go/pkg/mod", "/go/build-cache"))
                for line in lines
            )
            stage = parent
        assert "runtime-base" in lineage
    assert stages["controller"][0] == "runtime-sources"
    assert any("COPY --from=go-deps /usr/local/go" in line for line in stages["controller"][1])


def test_build_scripts_never_flatten_image_layers():
    for script in (REPO_ROOT / "scripts").glob("*.sh"):
        text = script.read_text()
        assert not re.search(r"docker\s+(?:container\s+)?export\b|docker\s+(?:image\s+)?import\b|--squash", text), (
            script
        )


def test_validator_inherits_the_immutable_compiler_and_cache_layers():
    text = "\n".join(_lines("Dockerfile.runtime"))
    stage = text.split("FROM runtime-base AS validator-toolchain\n", 1)[1].split("FROM model-deps", 1)[0]
    assert "FROM validator-toolchain AS validator" in stage
    for source in ("/usr/local/go", "/go/pkg/mod", "/go/build-cache"):
        assert f"COPY --from=go-deps {source} {source}" in stage
    assert "ENV GOCACHEPROG=/usr/local/bin/sdo-go-cache" in stage
    assert "RUN test -x /usr/local/go/bin/go" in stage


def test_workers_do_not_wait_for_offline_validator_cache_warming() -> None:
    stages = {}
    current = None
    for line in _lines("Dockerfile.runtime"):
        if match := re.match(r"FROM (\S+) AS (\S+)", line):
            current = match[2]
            stages[current] = (match[1], [])
        elif current:
            stages[current][1].append(line)
    assert stages["go-runtime"][0] == "go-binaries"
    for name in ("worker-runtime", "scripted-runtime"):
        copies = [line for line in stages[name][1] if line.startswith("COPY --from=")]
        assert any(line.startswith("COPY --from=go-binaries /usr/local/bin/sdo-operator ") for line in copies)
        assert not any(line.startswith("COPY --from=go-runtime ") for line in copies)
