"""Source identity of a Bake image: git objects of exactly the paths its build reads.

The path set is derived from the Bake target and its Dockerfile stage graph at the
commit being described, so it cannot drift from what the build consumes. A commit
that leaves those objects unchanged (for example a benchmarks-only patch) keeps
the identity of an image built earlier. Stdlib only: the image build script runs
this file directly to label new images.
"""

from __future__ import annotations

import hashlib
import json
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from benchmarks.sregym.adapter import WaitKind, wait_for

BAKE_FILE = "scripts/sdo-images.hcl"
SOURCE_TREE_LABEL = "dev.sdo.source-tree"
REVISION_LABEL = "org.opencontainers.image.revision"
# The build context filter changes which bytes COPY sends.
_CONTEXT_FILES = (".dockerignore",)
_MISSING = "missing"


@dataclass(frozen=True)
class ImageSource:
    """Git objects of every context path one Bake target reads at one commit."""

    target: str
    dockerfile: str
    stage: str
    objects: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if not self.objects:
            raise ValueError(f"image source for {self.target} has no build paths")

    @property
    def paths(self) -> tuple[str, ...]:
        return tuple(path for path, _ in self.objects)

    @property
    def identity(self) -> str:
        digest = hashlib.sha256(f"{self.dockerfile}\0{self.stage}\0".encode())
        for path, object_id in self.objects:
            digest.update(f"{path}\0{object_id}\0".encode())
        return f"tree-v1:{digest.hexdigest()}"


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, timeout=wait_for(WaitKind.COMMAND)
    )


def _show(repo: Path, commit: str, path: str) -> str:
    result = _git(repo, "show", f"{commit}:{path}")
    if result.returncode:
        raise ValueError(f"image build file {path} is missing at {commit}: {result.stderr.strip()}")
    return result.stdout


def bake_target(bake: str, target: str) -> tuple[str, str]:
    """Return the Dockerfile and stage of a Bake target built from the repository root."""
    block = re.search(rf'^target "{re.escape(target)}" \{{(.*?)^\}}', bake, re.MULTILINE | re.DOTALL)
    if block is None:
        raise ValueError(f"Bake target {target} is not defined in {BAKE_FILE}")
    fields = dict(re.findall(r'^\s*(\w+)\s*=\s*"([^"]*)"\s*$', block[1], re.MULTILINE))
    if fields.get("context") != "." or "contexts" in re.findall(r"^\s*(\w+)\s*=", block[1], re.MULTILINE):
        raise ValueError(f"Bake target {target} must build from the repository root without named contexts")
    if "dockerfile" not in fields or "target" not in fields:
        raise ValueError(f"Bake target {target} must name a dockerfile and a stage")
    return fields["dockerfile"], fields["target"]


def _instructions(dockerfile: str) -> list[tuple[str, str]]:
    joined = re.sub(r"\\\n", " ", dockerfile)
    result = []
    for line in joined.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            keyword, _, rest = stripped.partition(" ")
            result.append((keyword.upper(), rest.strip()))
    return result


def stage_sources(dockerfile: str, stage: str) -> tuple[str, ...]:
    """Context paths read by ``stage`` and every stage it builds on or copies from."""
    stages: dict[str, list[tuple[str, str]]] = {}
    parents: dict[str, str] = {}
    current = None
    for keyword, rest in _instructions(dockerfile):
        if keyword == "FROM":
            words = [word for word in rest.split() if not word.startswith("--")]
            if len(words) != 3 or words[1].upper() != "AS":
                raise ValueError(f"Dockerfile stage must be named: FROM {rest}")
            current = words[2]
            parents[current] = words[0]
            stages[current] = []
        elif current is not None:
            stages[current].append((keyword, rest))
    if stage not in stages:
        raise ValueError(f"Dockerfile stage {stage} is not defined")
    sources: set[str] = set()
    pending, seen = [stage], set()
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        if parents[name] in stages:
            pending.append(parents[name])
        for keyword, rest in stages[name]:
            if keyword == "RUN" and re.search(r"--mount=\S*type=bind", rest) and "from=" not in rest:
                raise ValueError(f"stage {name} bind-mounts the build context; declare it with COPY")
            if keyword not in {"COPY", "ADD"}:
                continue
            words = json.loads(rest) if rest.startswith("[") else shlex.split(rest)
            flags = [word for word in words if word.startswith("--")]
            arguments = [word for word in words if not word.startswith("--")]
            source_stage = next((flag.split("=", 1)[1] for flag in flags if flag.startswith("--from=")), None)
            if source_stage is not None:
                if source_stage in stages:
                    pending.append(source_stage)
                continue
            sources.update(path.rstrip("/").removeprefix("./") for path in arguments[:-1])
    return tuple(sorted(sources))


def _covering(paths: set[str]) -> tuple[str, ...]:
    """Drop paths already covered by a listed parent directory."""
    return tuple(
        sorted(path for path in paths if not any(path != parent and path.startswith(f"{parent}/") for parent in paths))
    )


def image_source(repo: Path, commit: str, target: str) -> ImageSource:
    """Describe ``target`` as built from ``commit`` using that commit's build files."""
    dockerfile, stage = bake_target(_show(repo, commit, BAKE_FILE), target)
    paths = _covering({dockerfile, *_CONTEXT_FILES, *stage_sources(_show(repo, commit, dockerfile), stage)})
    objects = []
    for path in paths:
        result = _git(repo, "rev-parse", "--verify", "--quiet", f"{commit}:{path}")
        objects.append((path, result.stdout.strip() if result.returncode == 0 else _MISSING))
    return ImageSource(target=target, dockerfile=dockerfile, stage=stage, objects=tuple(objects))


def changed_build_paths(repo: Path, built: ImageSource, current: ImageSource, old: str, new: str) -> list[str]:
    """Name the files that differ between two descriptions of one image."""
    if (built.dockerfile, built.stage) != (current.dockerfile, current.stage):
        return [f"{BAKE_FILE} target {current.target}"]
    differing = sorted(dict(built.objects).items() ^ dict(current.objects).items())
    paths = sorted({path for path, _ in differing})
    files = _git(repo, "diff", "--name-only", old, new, "--", *paths).stdout.split()
    return files or paths


def main(argv: list[str]) -> int:
    if len(argv) != 3 or argv[0] not in {"identity", "paths"}:
        print("usage: image_provenance.py identity|paths <bake-target> <commit>", file=sys.stderr)
        return 2
    command, target, commit = argv
    source = image_source(Path.cwd(), commit, target)
    print(source.identity if command == "identity" else "\n".join(source.paths))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
