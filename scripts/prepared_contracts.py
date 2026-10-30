#!/usr/bin/env python3
"""Publish selected race test executables, never their compiler dependency cache."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TypedDict

from go_test_timing import run_tests

ROOT = Path(__file__).resolve().parents[1]


class PreparedBinary(TypedDict):
    scope: str
    package: str
    names: list[str]
    binary: str
    sha256: str
    directory: str


class PreparedManifest(TypedDict):
    revision: str
    plan_sha256: str
    bin: int
    entries: list[PreparedBinary]
    test2json_sha256: str


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def packages(data: dict, members: list[str]) -> dict[tuple[str, str], list[str]]:
    selected: dict[tuple[str, str], list[str]] = {}
    for key in members:
        item = data["items"][key]
        if item["kind"] == "go":
            for package in item["packages"]:
                selected.setdefault((item["scope"], package), []).append(item["name"])
    return selected


def prepare(plan: Path, output: Path, *, root: Path = ROOT) -> None:
    data = json.loads(plan.read_text())
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    output.mkdir(parents=True, exist_ok=True)
    selected = packages(data, [key for members in data["bins"] for key in members])
    objects = output / "objects"
    objects.mkdir(exist_ok=True)

    compiled: dict[tuple[str, str], tuple[Path, str, str]] = {}
    for scope in sorted({scope for scope, _ in selected}):
        module = root / "sdo/operator" / ("runtime" if scope == "runtime" else "")
        names = sorted(package for selected_scope, package in selected if selected_scope == scope)
        # Each Go invocation loads the module graph. Discover all selected
        # directories once and let Go schedule their links in one build graph.
        metadata = subprocess.check_output(
            ["go", "list", "-race", "-f", "{{.ImportPath}}\t{{.Dir}}", *names], cwd=module, text=True
        )
        directories = dict(line.split("\t", 1) for line in metadata.splitlines())
        if set(directories) != set(names):
            raise ValueError("prepared package directories do not match selected packages")
        # go test -c names output by the last import-path element. Split only
        # collisions, so unrelated packages still share one graph load.
        batches: list[dict[str, str]] = []
        for package in names:
            basename = package.rsplit("/", 1)[-1] + ".test"
            batch = next((batch for batch in batches if basename not in batch), None)
            if batch is None:
                batch = {}
                batches.append(batch)
            batch[basename] = package
        for index, batch in enumerate(batches):
            directory = objects / scope / str(index)
            directory.mkdir(parents=True)
            # go test normally omits DWARF; -c retains it unless disabled.
            subprocess.run(
                ["go", "test", "-race", "-c", "-ldflags=-w", "-o", str(directory.resolve()), *batch.values()],
                cwd=module,
                check=True,
            )
            for basename, package in batch.items():
                binary = directory / basename
                compiled[scope, package] = (binary, str(Path(directories[package]).relative_to(root)), digest(binary))

    tool = objects / "test2json"
    subprocess.run(["go", "build", "-o", str(tool.resolve()), "cmd/test2json"], cwd=root, check=True)
    tool_digest = digest(tool)
    for index, members in enumerate(data["bins"]):
        directory = output / str(index)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "test2json").hardlink_to(tool)
        entries: list[PreparedBinary] = []
        for (scope, package), names in sorted(packages(data, members).items()):
            filename = hashlib.sha256(f"{scope}:{package}".encode()).hexdigest() + ".test"
            binary = directory / filename
            binary.hardlink_to(compiled[scope, package][0])
            entries.append(
                {
                    "scope": scope,
                    "package": package,
                    "names": sorted(names),
                    "binary": filename,
                    "sha256": compiled[scope, package][2],
                    "directory": compiled[scope, package][1],
                }
            )
        manifest: PreparedManifest = {
            "revision": revision,
            "plan_sha256": digest(plan),
            "bin": index,
            "entries": entries,
            "test2json_sha256": tool_digest,
        }
        (directory / "manifest.json").write_text(json.dumps(manifest, sort_keys=True) + "\n")
        (directory / "pr-test-plan.json").write_bytes(plan.read_bytes())


def execute(plan: Path, index: int, directory: Path, *, root: Path = ROOT) -> None:
    data = json.loads(plan.read_text())
    manifest: PreparedManifest = json.loads((directory / "manifest.json").read_text())
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    if (manifest["revision"], manifest["plan_sha256"], manifest["bin"]) != (revision, digest(plan), index):
        raise ValueError("prepared contract source, plan or bin mismatch; rerun preparation")
    expected = {key: sorted(names) for key, names in packages(data, data["bins"][index]).items()}
    actual = {(entry["scope"], entry["package"]): entry["names"] for entry in manifest["entries"]}
    if actual != expected or len(actual) != len(manifest["entries"]):
        raise ValueError("prepared contract inventory does not match the selected tests")
    tool = directory / "test2json"
    if digest(tool) != manifest["test2json_sha256"]:
        raise ValueError("prepared test2json identity mismatch")
    commands = []
    for entry in manifest["entries"]:
        binary = directory / entry["binary"]
        if binary.parent != directory or digest(binary) != entry["sha256"]:
            raise ValueError("prepared contract binary identity mismatch")
        selection = "^(" + "|".join(re.escape(name) for name in entry["names"]) + ")$"
        module = root / entry["directory"]
        if not module.resolve().is_relative_to(root.resolve()) or not module.is_dir():
            raise ValueError("prepared contract package directory is outside the checkout or missing")
        commands.append(
            (
                [
                    str(tool.resolve()),
                    "-t",
                    "-p",
                    entry["package"],
                    str(binary.resolve()),
                    "-test.v=test2json",
                    "-test.parallel=2",
                    "-test.count=1",
                    "-test.paniconexit0",
                    "-test.timeout=" + ("5m" if entry["scope"] == "runtime" else "4m"),
                    "-test.run=" + selection,
                ],
                module,
            )
        )
    started = time.monotonic()

    def run(command: tuple[list[str], Path]) -> None:
        run_tests(command[0], started=started, cwd=command[1], env={**os.environ, "PWD": str(command[1])})

    # Preserve go test's bounded package parallelism; propagate every failure.
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(run, commands))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    prepare(args.plan, args.output)


if __name__ == "__main__":
    main()
