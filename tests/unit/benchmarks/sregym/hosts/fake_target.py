"""An in-memory host for planning tests: a file map plus canned command answers. No root, no processes."""

from __future__ import annotations

import posixpath
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from benchmarks.sregym.hosts.target import Result

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass
class FakeTarget:
    name: str = "fake"
    files: dict[str, bytes] = field(default_factory=dict)
    fstype: dict[str, str] = field(default_factory=dict)  # mount point -> filesystem type
    sudo_calls: list[list[str]] = field(default_factory=list)
    commands: dict[tuple[str, ...], str] = field(default_factory=dict)
    dirs: set[str] = field(default_factory=set)

    def run(self, argv: Sequence[str], *, stdin: bytes | None = None, timeout: float = 60) -> Result:
        argv = list(argv)
        if argv[:2] == ["test", "-e"]:
            return Result(0 if self._exists(argv[2]) else 1)
        if argv[:2] == ["test", "-d"]:
            return Result(0 if argv[2] in self.dirs else 1)
        if argv[:2] == ["test", "-s"]:
            return Result(0 if self.files.get(argv[2]) else 1)
        if argv[:2] == ["test", "-w"]:
            return Result(0 if argv[2] in self.dirs else 1)
        if argv[:1] == ["findmnt"]:
            path = argv[-1]
            for mount in sorted(self.fstype, key=len, reverse=True):
                if path == mount or path.startswith(mount.rstrip("/") + "/") or mount == "/":
                    return Result(0, self.fstype[mount].encode() + b"\n")
            return Result(1)
        if argv[:3] == ["sh", "-c", 'mkdir -p "$1" && chmod 700 "$1"']:
            self.dirs.add(argv[-1])
            return Result(0)
        if argv[:3] == ["stat", "-c", "%a"]:
            return Result(0, b"700\n") if argv[3] in self.dirs else Result(1)
        if argv[:1] in (["env"], ["loginctl"]):
            return Result(0, b"inactive\n")
        if argv[:1] == ["mkdir"]:
            self.dirs.add(argv[-1])
            return Result(0)
        key = tuple(argv)
        if key in self.commands:
            return Result(0, self.commands[key].encode())
        return Result(1, b"", "unknown command")

    def _exists(self, path: str) -> bool:
        return path in self.files or path in self.dirs or any(f.startswith(path.rstrip("/") + "/") for f in self.files)

    def read(self, path: str) -> bytes | None:
        return self.files.get(path)

    def write(self, path: str, data: bytes, mode: int = 0o644) -> None:
        self.files[path] = data
        self.dirs.add(posixpath.dirname(path))

    def sudo(self, argv: Sequence[str]) -> Result:
        self.sudo_calls.append(list(argv))
        return Result(0)

    def sudo_install(self, path: str, data: bytes, mode: int = 0o644) -> None:
        self.sudo_calls.append(["install", path])
        self.files[path] = data
