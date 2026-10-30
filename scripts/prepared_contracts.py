#!/usr/bin/env python3
"""Publish selected race test executables, never their compiler dependency cache."""

from __future__ import annotations

import argparse
import hashlib
import json
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

    def compile_package(key: tuple[str, str]) -> tuple[tuple[str, str], Path]:
        scope, package = key
        binary = objects / (hashlib.sha256(f"{scope}:{package}".encode()).hexdigest() + ".test")
        module = root / "sdo/operator" / ("runtime" if scope == "runtime" else "")
        subprocess.run(["go", "test", "-race", "-c", "-o", str(binary.resolve()), package], cwd=module, check=True)
        return key, binary

    tool = objects / "test2json"
    subprocess.run(["go", "build", "-o", str(tool.resolve()), "cmd/test2json"], cwd=root, check=True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        compiled = dict(pool.map(compile_package, sorted(selected)))
    for index, members in enumerate(data["bins"]):
        directory = output / str(index)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "test2json").hardlink_to(tool)
        entries: list[PreparedBinary] = []
        for (scope, package), names in sorted(packages(data, members).items()):
            filename = hashlib.sha256(f"{scope}:{package}".encode()).hexdigest() + ".test"
            binary = directory / filename
            binary.hardlink_to(compiled[scope, package])
            entries.append(
                {
                    "scope": scope,
                    "package": package,
                    "names": sorted(names),
                    "binary": filename,
                    "sha256": digest(binary),
                }
            )
        manifest: PreparedManifest = {
            "revision": revision,
            "plan_sha256": digest(plan),
            "bin": index,
            "entries": entries,
            "test2json_sha256": digest(tool),
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
        module = root / "sdo/operator" / ("runtime" if entry["scope"] == "runtime" else "")
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
        run_tests(command[0], started=started, cwd=command[1])

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
