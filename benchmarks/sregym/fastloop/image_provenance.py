"""Source identity of a Bake image: git blobs of exactly the files its build context sends.

The roots are derived from the Bake target and its Dockerfile stage graph at the
commit being described, and the files under them from the ``.dockerignore`` at that
commit, so the identity cannot drift from what the build consumes. A commit that
leaves those blobs unchanged (a benchmarks-only patch, a Go ``_test.go`` edit, a
README) keeps the identity of an image built earlier. This module is the one owner
of that set: the build script labels images with it and the preflight checks them
against it. Stdlib only: the image build script runs this file directly.
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

BAKE_FILE = "scripts/sdo-images.hcl"
SOURCE_TREE_LABEL = "dev.sdo.source-tree"
REVISION_LABEL = "org.opencontainers.image.revision"
# The build context filter changes which bytes COPY sends.
_CONTEXT_FILES = (".dockerignore",)
_MISSING = "missing"
_BAKE_BLOCK = "{bake}#target-{target}"


@dataclass(frozen=True)
class ImageSource:
    """Every file one Bake target's build context sends, and its Bake definition, at one commit.

    ``roots`` are the declared COPY sources; ``objects`` are the files below them that survive
    ``.dockerignore``, plus one entry for the target's own Bake block.
    """

    target: str
    dockerfile: str
    stage: str
    roots: tuple[str, ...]
    objects: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if not self.objects:
            raise ValueError(f"image source for {self.target} has no build paths")

    @property
    def identity(self) -> str:
        digest = hashlib.sha256(f"{self.dockerfile}\0{self.stage}\0".encode())
        for path, object_id in self.objects:
            digest.update(f"{path}\0{object_id}\0".encode())
        return f"tree-v2:{digest.hexdigest()}"


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, timeout=20)


def _show(repo: Path, commit: str, path: str) -> str:
    result = _git(repo, "show", f"{commit}:{path}")
    if result.returncode:
        raise ValueError(f"image build file {path} is missing at {commit}: {result.stderr.strip()}")
    return result.stdout


def _bake_block(bake: str, target: str) -> str:
    block = re.search(rf'^target "{re.escape(target)}" \{{(.*?)^\}}', bake, re.MULTILINE | re.DOTALL)
    if block is None:
        raise ValueError(f"Bake target {target} is not defined in {BAKE_FILE}")
    return block[1]


def bake_target(bake: str, target: str) -> tuple[str, str]:
    """Return the Dockerfile and stage of a Bake target built from the repository root."""
    block = _bake_block(bake, target)
    fields = dict(re.findall(r'^\s*(\w+)\s*=\s*"([^"]*)"\s*$', block, re.MULTILINE))
    if fields.get("context") != "." or "contexts" in re.findall(r"^\s*(\w+)\s*=", block, re.MULTILINE):
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


def _ignore_matcher(dockerignore: str) -> re.Pattern[str] | None:
    """Compile ``.dockerignore`` lines; a path is ignored when it or a parent directory matches."""
    alternatives = []
    for raw in dockerignore.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("!"):
            raise ValueError(f".dockerignore exception {line!r} is not supported by image provenance")
        pattern, index = "", 0
        line = line.strip("/")
        while index < len(line):
            if line.startswith("**/", index):
                pattern, index = pattern + "(?:.*/)?", index + 3
            elif line.startswith("**", index):
                pattern, index = pattern + ".*", index + 2
            elif line[index] == "*":
                pattern, index = pattern + "[^/]*", index + 1
            elif line[index] == "?":
                pattern, index = pattern + "[^/]", index + 1
            else:
                pattern, index = pattern + re.escape(line[index]), index + 1
        alternatives.append(pattern)
    return re.compile(rf"(?:{'|'.join(alternatives)})(?:/.*)?\Z") if alternatives else None


def _is_ignored(matcher: re.Pattern[str] | None, path: str) -> bool:
    return matcher is not None and matcher.match(path) is not None


def _dockerignore(repo: Path, commit: str) -> str:
    result = _git(repo, "show", f"{commit}:.dockerignore")
    return result.stdout if result.returncode == 0 else ""


def image_source(repo: Path, commit: str, target: str) -> ImageSource:
    """Describe ``target`` as built from ``commit`` using that commit's build files."""
    bake = _show(repo, commit, BAKE_FILE)
    dockerfile, stage = bake_target(bake, target)
    roots = _covering({dockerfile, *_CONTEXT_FILES, *stage_sources(_show(repo, commit, dockerfile), stage)})
    matcher = _ignore_matcher(_dockerignore(repo, commit))
    listing = _git(repo, "ls-tree", "-r", "-z", commit, "--", *roots)
    if listing.returncode:
        raise ValueError(f"cannot list image build files at {commit}: {listing.stderr.strip()}")
    blobs = {}
    for record in filter(None, listing.stdout.split("\0")):
        header, _, path = record.partition("\t")
        if not _is_ignored(matcher, path):
            blobs[path] = header.split()[2]
    for root in roots:
        if not any(path == root or path.startswith(f"{root}/") for path in blobs):
            blobs[root] = _MISSING
    block = hashlib.sha256(_bake_block(bake, target).encode()).hexdigest()
    blobs[_BAKE_BLOCK.format(bake=BAKE_FILE, target=target)] = block
    return ImageSource(
        target=target, dockerfile=dockerfile, stage=stage, roots=roots, objects=tuple(sorted(blobs.items()))
    )


def uncommitted_build_files(repo: Path, sources: list[ImageSource]) -> list[str]:
    """Name uncommitted files an image build would send; files ``.dockerignore`` drops are not build input."""
    roots = sorted({root for source in sources for root in source.roots})
    status = _git(repo, "status", "--porcelain", "-z", "--untracked-files=all", "--", *roots)
    if status.returncode:
        raise ValueError(f"cannot read image build file status: {status.stderr.strip()}")
    matcher = _ignore_matcher((repo / ".dockerignore").read_text() if (repo / ".dockerignore").is_file() else "")
    paths, records = [], iter(filter(None, status.stdout.split("\0")))
    for record in records:
        paths.append(record[3:])
        if record[0] in "RC":
            paths.append(next(records, ""))
    return sorted({path for path in paths if path and not _is_ignored(matcher, path)})


def changed_build_paths(built: ImageSource, current: ImageSource) -> list[str]:
    """Name the files that differ between two descriptions of one image."""
    if (built.dockerfile, built.stage) != (current.dockerfile, current.stage):
        return [f"{BAKE_FILE} target {current.target}"]
    differing = sorted({path for path, _ in dict(built.objects).items() ^ dict(current.objects).items()})
    return [BAKE_FILE if path.startswith(f"{BAKE_FILE}#") else path for path in differing]


def main(argv: list[str]) -> int:
    if len(argv) != 3 or argv[0] not in {"identity", "paths", "dirty"}:
        print("usage: image_provenance.py identity|paths|dirty <bake-target> <commit>", file=sys.stderr)
        return 2
    command, target, commit = argv
    source = image_source(Path.cwd(), commit, target)
    if command == "identity":
        print(source.identity)
    elif command == "paths":
        print("\n".join(source.roots))
    else:
        print("\n".join(uncommitted_build_files(Path.cwd(), [source])))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
