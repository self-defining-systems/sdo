"""The link cache serves equal links and can never serve a different one."""

import os
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
FAKE_LINK = """#!/usr/bin/env bash
# Stand-in linker: records each real run and writes a deterministic output.
set -eu
out=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    -fail) exit 3 ;;
    *) shift ;;
  esac
done
echo run >> "$FAKE_LINK_RUNS"
cat "$FAKE_LINK_INPUT" > "$out"
chmod 755 "$out"
"""


@pytest.fixture(scope="module")
def wrapper(tmp_path_factory) -> Path:
    binary = tmp_path_factory.mktemp("linkcache") / "linkcache"
    subprocess.run(["go", "build", "-o", str(binary), str(ROOT / "scripts/linkcache/main.go")], check=True)
    return binary


class Harness:
    def __init__(self, wrapper: Path, tmp_path: Path) -> None:
        self.wrapper = wrapper
        self.dir = tmp_path
        self.cache = tmp_path / "cache"
        self.runs = tmp_path / "runs"
        self.runs.write_text("")
        self.linker = tmp_path / "link"
        self.linker.write_text(FAKE_LINK)
        self.linker.chmod(0o755)
        self.archive = tmp_path / "main.a"
        self.archive.write_text("archive-1")
        self.dependency = tmp_path / "dep.a"
        self.dependency.write_text("dependency-1")
        self.importcfg = tmp_path / "importcfg"
        self.write_importcfg(self.dependency)

    def write_importcfg(self, dependency: Path) -> None:
        self.importcfg.write_text(f"# import config\npackagefile dep={dependency}\n")

    def link(self, out: str = "out", *extra: str, cache: bool = True) -> subprocess.CompletedProcess:
        environment = {**os.environ, "FAKE_LINK_RUNS": str(self.runs), "FAKE_LINK_INPUT": str(self.archive)}
        if cache:
            environment["SDO_LINK_CACHE"] = str(self.cache)
        else:
            environment.pop("SDO_LINK_CACHE", None)
        return subprocess.run(
            [
                str(self.wrapper),
                str(self.linker),
                "-o",
                str(self.dir / out),
                "-importcfg",
                str(self.importcfg),
                "-buildmode=exe",
                *extra,
                str(self.archive),
            ],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

    @property
    def real_links(self) -> int:
        return len(self.runs.read_text().split())


@pytest.fixture
def harness(wrapper: Path, tmp_path: Path) -> Harness:
    return Harness(wrapper, tmp_path)


def test_identical_inputs_link_once_into_any_output_path(harness: Harness):
    assert harness.link("first").returncode == 0
    assert harness.link("second").returncode == 0
    assert harness.real_links == 1
    assert (harness.dir / "first").read_text() == (harness.dir / "second").read_text() == "archive-1"
    # Go rewrites the build ID in place, so a hit is a private, executable copy.
    assert not (harness.dir / "first").samefile(harness.dir / "second")
    assert (harness.dir / "second").stat().st_mode & stat.S_IXUSR


@pytest.mark.parametrize("change", ["archive", "dependency", "flag", "importcfg"])
def test_any_changed_linker_input_relinks(harness: Harness, change: str):
    harness.link("before")
    extra: tuple[str, ...] = ()
    if change == "archive":
        harness.archive.write_text("archive-2")
    elif change == "dependency":
        # A path outside Go's content-addressed cache is hashed by its bytes.
        harness.dependency.write_text("dependency-2")
    elif change == "flag":
        extra = ("-X=main.version=2",)
    else:
        harness.importcfg.write_text(harness.importcfg.read_text() + "packagefile other=/missing/other.a\n")
    result = harness.link("after", *extra)
    if change == "importcfg":
        assert result.returncode == 0  # an unreadable dependency falls back to the real linker
    assert harness.real_links == 2
    assert (harness.dir / "after").read_text() == harness.archive.read_text()


def test_content_addressed_dependencies_are_keyed_by_name(harness: Harness):
    addressed = harness.dir / ("a" * 64 + "-d")
    addressed.write_text("one")
    harness.write_importcfg(addressed)
    harness.link("one")
    addressed.write_text("two")  # the name is the content hash in Go's cache
    harness.link("two")
    assert harness.real_links == 1


def test_failed_links_are_not_cached(harness: Harness):
    assert harness.link("out", "-fail").returncode == 3
    assert harness.link("out", "-fail").returncode == 3
    assert not list(harness.cache.glob("*")) if harness.cache.exists() else True


def test_without_a_cache_directory_every_link_runs(harness: Harness):
    harness.link("a", cache=False)
    harness.link("b", cache=False)
    assert harness.real_links == 2
    assert not harness.cache.exists()


def test_other_tools_pass_through_untouched(wrapper: Path, tmp_path: Path):
    result = subprocess.run(
        [str(wrapper), "echo", "compile-output"],
        env={**os.environ, "SDO_LINK_CACHE": str(tmp_path / "cache")},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stdout.strip() == "compile-output"
    assert not (tmp_path / "cache").exists()


@pytest.mark.go_integration
def test_real_go_toolchain_reuses_links_and_still_relinks_changed_source(wrapper: Path, tmp_path: Path):
    """The real linker feeds the real cache; a changed source cannot hit it."""
    module = tmp_path / "module"
    module.mkdir()
    (module / "go.mod").write_text("module cache.example/m\n\ngo 1.24\n")
    program = module / "main.go"
    cache = tmp_path / "cache"
    environment = {
        **os.environ,
        "SDO_LINK_CACHE": str(cache),
        "GOFLAGS": f"-trimpath -toolexec={wrapper}",
        "GOPROXY": "off",
    }

    def build(message: str) -> str:
        program.write_text(f'package main\nimport "fmt"\nfunc main() {{ fmt.Print({message!r}) }}\n'.replace("'", '"'))
        binary = module / "out"
        binary.unlink(missing_ok=True)
        subprocess.run(["go", "build", "-o", str(binary), "."], cwd=module, env=environment, check=True)
        return subprocess.run([str(binary)], capture_output=True, text=True, check=True).stdout

    assert build("first") == "first"
    assert len(list(cache.glob("[0-9a-f]*"))) == 1
    assert build("first") == "first"
    assert len(list(cache.glob("[0-9a-f]*"))) == 1
    assert build("second") == "second"
    assert len(list(cache.glob("[0-9a-f]*"))) == 2
