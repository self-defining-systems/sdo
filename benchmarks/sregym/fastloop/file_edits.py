"""Bounded file-edit evidence for replaying a reflection checkout."""

from __future__ import annotations

import base64
import binascii
import json
import stat
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

MAX_FILE_EDITS_BYTES = 48 << 20
# Written next to a checkpoint's result when its file edits could not be recorded.
UNRECORDED_FILE_EDITS = "file_edits_unrecorded.json"


@dataclass(frozen=True)
class FileEdit:
    path: str
    content_base64: str | None
    mode: int = 0o644

    def __post_init__(self) -> None:
        path = PurePosixPath(self.path)
        if (
            not self.path
            or self.path == "."
            or path.is_absolute()
            or ".." in path.parts
            or ".git" in path.parts
            or str(path) != self.path
            or "\\" in self.path
        ):
            raise ValueError(f"invalid recorded file path {self.path!r}")
        if self.mode not in {0o644, 0o755}:
            raise ValueError("recorded file mode must be 0644 or 0755")
        if self.content_base64 is not None:
            try:
                base64.b64decode(self.content_base64, validate=True)
            except (ValueError, binascii.Error) as exc:
                raise ValueError("invalid recorded file content") from exc


def encode_file_edits(edits: Sequence[FileEdit]) -> bytes:
    data = json.dumps({"version": 1, "edits": [asdict(edit) for edit in edits]}, sort_keys=True).encode()
    if len(data) > MAX_FILE_EDITS_BYTES:
        raise ValueError("recorded file edits exceed the 48 MiB evidence limit")
    return data


def decode_file_edits(data: bytes) -> tuple[FileEdit, ...]:
    if len(data) > MAX_FILE_EDITS_BYTES:
        raise ValueError("recorded file edits exceed the 48 MiB evidence limit")
    value = json.loads(data)
    if not isinstance(value, dict) or value.get("version") != 1 or not isinstance(value.get("edits"), list):
        raise ValueError("unsupported recorded file edits")
    edits = tuple(FileEdit(**edit) for edit in value["edits"])
    if len({edit.path for edit in edits}) != len(edits):
        raise ValueError("duplicate recorded file path")
    return edits


def record_file_edits(root: Path, base: str) -> tuple[FileEdit, ...]:
    def git(*args: str) -> list[str]:
        result = subprocess.run(["git", "-C", str(root), *args], capture_output=True, timeout=30, check=True)
        return [name.decode() for name in result.stdout.split(b"\0") if name]

    paths = set(git("diff", "--name-only", "-z", base, "--"))
    paths.update(git("ls-files", "--others", "--exclude-standard", "-z"))
    paths.update(git("ls-files", "--others", "--ignored", "--exclude-standard", "-z", "--", ".sdo"))
    edits = []
    total = 0
    for name in sorted(paths):
        path = root / name
        for parent in (path, *path.parents):
            if parent == root:
                break
            if parent.is_symlink():
                raise ValueError(f"recorded file path traverses a symlink: {name}")
        if path.exists():
            info = path.stat()
            total += info.st_size
            if not stat.S_ISREG(info.st_mode) or total > 32 << 20:
                raise ValueError("recorded files must be regular files totaling at most 32 MiB")
            content = base64.b64encode(path.read_bytes()).decode()
            mode = 0o755 if info.st_mode & 0o111 else 0o644
            edits.append(FileEdit(name, content, mode))
        else:
            edits.append(FileEdit(name, None))
    encode_file_edits(edits)
    return tuple(edits)


def apply_file_edits(root: Path, edits: Sequence[FileEdit]) -> None:
    for edit in edits:
        target = root / edit.path
        for parent in (target, *target.parents):
            if parent == root:
                break
            if parent.is_symlink():
                raise ValueError(f"recorded file path traverses a symlink: {edit.path}")
        if edit.content_base64 is None:
            target.unlink(missing_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(base64.b64decode(edit.content_base64, validate=True))
            target.chmod(edit.mode)
