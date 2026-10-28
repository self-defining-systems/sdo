#!/usr/bin/env python3
"""Hand a PR's new Go build entries to its consumer jobs without saving the base cache again.

Hosted caches saved on a pull request ref are private to it, and each full prepared build is
about 1.6 GB, so a handful of concurrent PRs pass the 10 GB repository cap and evict each
other. Base-branch pushes save the full archives. A PR run restores the newest base archive,
records what it holds (`snapshot`), compiles, and saves only the entries it added (`export`).
Consumers restore the base archive and the delta, then lay the delta over it (`apply`).

A root is `name=path` (content-addressed: an entry is new when its path is not in the
snapshot) or `name=path@content` (mutable files such as rebuilt binaries: an entry is new
when its path, size or digest differs).
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path


class CacheDeltaError(RuntimeError):
    """A root specification or manifest could not be used."""


@dataclass(frozen=True)
class Root:
    name: str
    path: Path
    by_content: bool

    @classmethod
    def parse(cls, spec: str) -> Root:
        name, separator, location = spec.partition("=")
        if not separator or not name or not location or "/" in name:
            raise CacheDeltaError(f"root must be name=path[@content], got {spec!r}")
        by_content = location.endswith("@content")
        location = location.removesuffix("@content")
        return cls(name, Path(os.path.expanduser(location)), by_content)

    def identity(self, relative: str, file: Path) -> str:
        if not self.by_content:
            return relative
        digest = hashlib.sha256(file.read_bytes()).hexdigest()
        return f"{relative}\t{file.stat().st_size}\t{digest}"

    def entries(self) -> dict[str, Path]:
        """Every regular file under the root, keyed by its identity."""
        found: dict[str, Path] = {}
        if not self.path.is_dir():
            return found
        for directory, _, names in os.walk(self.path):
            for name in names:
                file = Path(directory, name)
                if file.is_symlink() or not file.is_file():
                    continue
                relative = file.relative_to(self.path).as_posix()
                found[self.identity(relative, file)] = file
        return found


def _manifest_lines(roots: list[Root]) -> list[str]:
    return sorted(f"{root.name}\t{identity}" for root in roots for identity in root.entries())


def snapshot(manifest: Path, roots: list[Root]) -> int:
    manifest.parent.mkdir(parents=True, exist_ok=True)
    lines = _manifest_lines(roots)
    manifest.write_text("".join(f"{line}\n" for line in lines))
    return len(lines)


def export(manifest: Path, out: Path, roots: list[Root], max_bytes: int) -> tuple[bool, int, int]:
    """Copy entries absent from the snapshot to `out`; return (saved, files, bytes)."""
    if not manifest.is_file():
        raise CacheDeltaError(f"no snapshot at {manifest}")
    known = set(manifest.read_text().splitlines())
    shutil.rmtree(out, ignore_errors=True)
    added: list[tuple[Root, str, Path]] = []
    size = 0
    for root in roots:
        for identity, file in root.entries().items():
            if f"{root.name}\t{identity}" in known:
                continue
            added.append((root, file.relative_to(root.path).as_posix(), file))
            size += file.stat().st_size
    if size > max_bytes:
        return False, len(added), size
    for root, relative, file in added:
        destination = out / root.name / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file, destination)
    out.mkdir(parents=True, exist_ok=True)
    return True, len(added), size


def apply(delta: Path, roots: list[Root]) -> int:
    """Lay a delta over its roots; an existing file with the same name is replaced."""
    count = 0
    for root in roots:
        source = delta / root.name
        if not source.is_dir():
            continue
        for directory, _, names in os.walk(source):
            for name in names:
                file = Path(directory, name)
                destination = root.path / file.relative_to(source)
                destination.parent.mkdir(parents=True, exist_ok=True)
                if destination.exists() or destination.is_symlink():
                    destination.unlink()
                shutil.copy2(file, destination)
                count += 1
    return count


def _github_output(**values: object) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as handle:
            handle.writelines(f"{key}={value}\n" for key, value in values.items())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("snapshot", "export"):
        command = commands.add_parser(name)
        command.add_argument("--manifest", type=Path, required=True)
        if name == "export":
            command.add_argument("--out", type=Path, required=True)
            command.add_argument("--max-bytes", type=int, required=True)
        command.add_argument("roots", nargs="+")
    command = commands.add_parser("apply")
    command.add_argument("--delta", type=Path, required=True)
    command.add_argument("roots", nargs="+")
    args = parser.parse_args(argv)
    try:
        roots = [Root.parse(spec) for spec in args.roots]
        if args.command == "snapshot":
            print(f"snapshot: {snapshot(args.manifest, roots)} entries")
        elif args.command == "export":
            saved, files, size = export(args.manifest, args.out, roots, args.max_bytes)
            print(f"delta: {files} new entries, {size / 1e6:.1f} MB, {'saved' if saved else 'over the cap; not saved'}")
            _github_output(saved=str(saved).lower())
        else:
            print(f"applied {apply(args.delta, roots)} entries")
    except CacheDeltaError as error:
        print(f"go_ci_cache_delta: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
