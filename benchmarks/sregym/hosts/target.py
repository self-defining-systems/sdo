"""Where host setup runs: this machine or another over ssh, with root actions through ``sudo -S``.

The sudo password is read from a file the caller names, kept in memory, written only to sudo's stdin and never
placed in an argument list, a log line or an error message.
"""

from __future__ import annotations

import os
import shlex
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence

_WRITE = 'set -e; mkdir -p "$(dirname "$1")"; t=$(mktemp "$1.XXXXXX"); cat > "$t"; chmod "$2" "$t"; mv -f "$t" "$1"'


class TargetError(RuntimeError):
    """A command on the target failed; the message carries the command's stderr, never its stdin."""


@dataclass(frozen=True)
class Result:
    returncode: int
    data: bytes = b""
    stderr: str = ""

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    @property
    def stdout(self) -> str:
        return self.data.decode(errors="replace")


class Target(Protocol):
    name: str

    def run(self, argv: Sequence[str], *, stdin: bytes | None = None, timeout: float = 60) -> Result: ...

    def read(self, path: str) -> bytes | None: ...

    def write(self, path: str, data: bytes, mode: int = 0o644) -> None: ...

    def sudo(self, argv: Sequence[str]) -> Result: ...

    def sudo_install(self, path: str, data: bytes, mode: int = 0o644) -> None: ...


def read_password(path: str | None) -> str | None:
    """The first line of the password file, or ``None`` when no file is named (``SUDO_PASSWORD_FILE`` fallback)."""
    chosen = path or os.environ.get("SUDO_PASSWORD_FILE")
    if not chosen:
        return None
    try:
        text = Path(chosen).expanduser().read_text()
    except OSError as exc:
        raise TargetError(f"cannot read sudo password file {chosen}: {exc.strerror}") from exc
    line = text.splitlines()[0] if text else ""
    if not line:
        raise TargetError(f"sudo password file {chosen} is empty")
    return line


@dataclass
class CommandTarget:
    """Common file and sudo behaviour over one ``_exec`` that runs an argv on the machine."""

    name: str
    password: str | None = field(default=None, repr=False)
    is_root: bool = False

    def _exec(self, argv: Sequence[str], stdin: bytes | None, timeout: float) -> Result:
        raise NotImplementedError

    def run(self, argv: Sequence[str], *, stdin: bytes | None = None, timeout: float = 60) -> Result:
        return self._exec(argv, stdin, timeout)

    def read(self, path: str) -> bytes | None:
        probe = self._exec(["test", "-e", path], None, 30)
        if not probe.ok:
            return None
        result = self._exec(["cat", "--", path], None, 30)
        if not result.ok:
            raise TargetError(f"{self.name}: cannot read {path}: {result.stderr.strip()}")
        return result.data

    def write(self, path: str, data: bytes, mode: int = 0o644) -> None:
        result = self._exec(["sh", "-c", _WRITE, "sh", path, f"{mode:04o}"], data, 60)
        if not result.ok:
            raise TargetError(f"{self.name}: cannot write {path}: {result.stderr.strip()}")

    def sudo(self, argv: Sequence[str]) -> Result:
        if self.is_root:
            return self._exec(argv, None, 120)
        if self.password is None:
            probe = self._exec(["sudo", "-n", "--", *argv], None, 120)
            if not probe.ok and "password is required" in probe.stderr:
                raise TargetError(f"{self.name}: root step needs sudo; pass --sudo-password-file or SUDO_PASSWORD_FILE")
            return probe
        return self._exec(["sudo", "-S", "-p", "", "--", *argv], (self.password + "\n").encode(), 120)

    def sudo_install(self, path: str, data: bytes, mode: int = 0o644) -> None:
        made = self._exec(["mktemp"], None, 30)
        if not made.ok:
            raise TargetError(f"{self.name}: mktemp failed: {made.stderr.strip()}")
        tmp = made.stdout.strip()
        try:
            self.write(tmp, data, 0o600)
            result = self.sudo(["install", "-D", "-m", f"{mode:04o}", "-o", "root", "-g", "root", tmp, path])
            if not result.ok:
                raise TargetError(f"{self.name}: cannot install {path}: {result.stderr.strip()}")
        finally:
            self._exec(["rm", "-f", "--", tmp], None, 30)


class LocalTarget(CommandTarget):
    def __init__(self, name: str, password: str | None = None) -> None:
        super().__init__(name=name, password=password, is_root=os.geteuid() == 0)

    def _exec(self, argv: Sequence[str], stdin: bytes | None, timeout: float) -> Result:
        try:
            done = subprocess.run(list(argv), input=stdin, capture_output=True, timeout=timeout, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            return Result(127, b"", f"{type(exc).__name__}: {exc}")
        return Result(done.returncode, done.stdout, done.stderr.decode(errors="replace"))


class SshTarget(CommandTarget):
    def __init__(self, name: str, ssh_name: str, password: str | None = None) -> None:
        super().__init__(name=name, password=password)
        self.ssh_name = ssh_name

    def _exec(self, argv: Sequence[str], stdin: bytes | None, timeout: float) -> Result:
        command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", self.ssh_name, "--", shlex.join(argv)]
        try:
            done = subprocess.run(command, input=stdin, capture_output=True, timeout=timeout, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            return Result(255, b"", f"{type(exc).__name__}: {exc}")
        return Result(done.returncode, done.stdout, done.stderr.decode(errors="replace"))
