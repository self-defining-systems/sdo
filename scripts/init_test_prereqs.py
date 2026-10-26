#!/usr/bin/env python3
"""Initialize the committed SREGym test inputs without changing any gitlink."""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

MISSING = "SDO_TEST_PREREQUISITES_MISSING"
REMEDY = "uv run python scripts/init_test_prereqs.py"
FORK_BASE = "https://github.com/vic-lsh"


class PrerequisiteError(RuntimeError):
    """A recorded test input is unavailable or cannot safely be initialized."""


def git(repository: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repository), *args],
        check=check,
        capture_output=True,
        text=True,
        timeout=120,
    )


def initialized(repository: Path) -> bool:
    result = git(repository, "rev-parse", "--show-toplevel", check=False)
    return result.returncode == 0 and Path(result.stdout.strip()).resolve() == repository.resolve()


@dataclass(frozen=True)
class Pin:
    name: str
    path: str
    sha: str


def pins(repository: Path, revision: str = "HEAD") -> list[Pin]:
    modules = git(
        repository, "config", "--blob", f"{revision}:.gitmodules", "--get-regexp", r"^submodule\..*\.path$", check=False
    )
    if modules.returncode:
        # No .gitmodules is normal for leaf repositories; other Git errors are not.
        if git(repository, "cat-file", "-e", f"{revision}:.gitmodules", check=False).returncode:
            return []
        if modules.returncode != 1:
            raise PrerequisiteError(modules.stderr.strip())
    result = []
    for line in modules.stdout.splitlines():
        key, path = line.split(None, 1)
        child = (repository / path).resolve()
        if not child.is_relative_to(repository.resolve()) or child == repository.resolve():
            raise PrerequisiteError(f"submodule path escapes its repository: {path}")
        entry = git(repository, "ls-tree", revision, "--", path).stdout.split()
        if len(entry) != 4 or entry[0:2] != ["160000", "commit"]:
            raise PrerequisiteError(f"no recorded gitlink for {path}")
        result.append(Pin(key.removeprefix("submodule.").removesuffix(".path"), path, entry[2]))
    return result


def required_pin(repository: Path) -> Pin:
    for pin in pins(repository):
        if pin.path == "third_party/sregym":
            return pin
    raise PrerequisiteError("no committed third_party/sregym gitlink")


def check_pin(parent: Path, pin: Pin) -> None:
    child = parent / pin.path
    if not initialized(child):
        raise PrerequisiteError(f"{child}: uninitialized")
    actual = git(child, "rev-parse", "HEAD").stdout.strip()
    if actual != pin.sha:
        raise PrerequisiteError(f"{child}: expected {pin.sha}, found {actual}")
    for nested in pins(child, pin.sha):
        check_pin(child, nested)


def check_prerequisites(repository: Path) -> None:
    """Check the entire pinned input tree; never fall back to the parent Git repo."""
    try:
        check_pin(repository, required_pin(repository))
    except (PrerequisiteError, subprocess.SubprocessError, OSError) as error:
        raise PrerequisiteError(f"{MISSING}: {error}. Run `{REMEDY}`.") from error


def initialize_pin(parent: Path, pin: Pin, fork_base: str) -> None:
    child = parent / pin.path
    print(f"Initializing {child} at {pin.sha}", flush=True)
    git(parent, "submodule", "init", "--", pin.path)
    remote = git(parent, "config", "--get", f"submodule.{pin.name}.url").stdout.strip()
    name = remote.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1].removesuffix(".git")
    fork = f"{fork_base.rstrip('/')}/{name}.git"
    was_initialized = initialized(child)
    if not was_initialized:
        for url in dict.fromkeys((remote, fork)):
            git(
                parent,
                "-c",
                f"submodule.{pin.name}.url={url}",
                "submodule",
                "update",
                "--init",
                "--depth",
                "1",
                "--no-fetch",
                "--",
                pin.path,
                check=False,
            )
            if initialized(child):
                break
        if not initialized(child):
            raise PrerequisiteError(f"cannot initialize {child} from {remote} or {fork}")
    if git(child, "cat-file", "-e", f"{pin.sha}^{{commit}}", check=False).returncode:
        for url in dict.fromkeys((remote, fork)):
            git(child, "fetch", "--depth", "1", url, pin.sha, check=False)
            if git(child, "cat-file", "-e", f"{pin.sha}^{{commit}}", check=False).returncode == 0:
                break
        else:
            raise PrerequisiteError(f"{child}: commit {pin.sha} unavailable from {remote} and {fork}")
    actual = git(child, "rev-parse", "--verify", "HEAD", check=False).stdout.strip()
    if actual != pin.sha:
        if was_initialized and git(child, "status", "--porcelain").stdout.strip():
            raise PrerequisiteError(f"{child}: refusing to overwrite local changes")
        git(child, "checkout", "--detach", pin.sha)
    for nested in pins(child, pin.sha):
        initialize_pin(child, nested, fork_base)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--check", action="store_true", help="only verify exact pinned checkouts")
    parser.add_argument("--fork-base-url", default=FORK_BASE, help="fallback fork namespace")
    args = parser.parse_args()
    try:
        if not args.check:
            initialize_pin(args.repository, required_pin(args.repository), args.fork_base_url)
        check_prerequisites(args.repository)
    except (PrerequisiteError, subprocess.SubprocessError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1
    print("SREGym test prerequisites are initialized at their recorded SHAs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
