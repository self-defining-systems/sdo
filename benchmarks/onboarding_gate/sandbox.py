"""A network-less bubblewrap sandbox for replayed agent commands and agent-authored probe code.

The host file system is visible read-only (entry by entry, so sandbox-only paths can be created);
only the given directories are writable. Commands get
a clean environment, so a replayed ``kubectl`` or ``curl`` fails at once instead of reaching a
cluster, and agent-authored probe code that ``onboarding journeys --plan`` runs cannot send anything.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path


class SandboxUnavailable(RuntimeError):
    """bubblewrap is not installed or cannot create namespaces on this host."""


@dataclass(frozen=True)
class Sandbox:
    """Writable binds (host path -> sandbox path) and environment for one family of commands."""

    binds: dict[Path, str]
    env: dict[str, str]
    read_only: dict[Path, str] = field(default_factory=dict)
    chdir: str = "/"
    # isolated: a fresh root with only /usr, a minimal /etc and the given binds (an agent session);
    # otherwise the whole host root, read-only (replayed commands and offline checks).
    isolated: bool = False
    network: bool = False

    def __post_init__(self) -> None:
        for host in [*self.binds, *self.read_only]:
            if not host.is_absolute():
                raise ValueError(f"sandbox bind {host} must be absolute")

    def argv(self, command: list[str]) -> list[str]:
        bwrap = shutil.which("bwrap")
        if bwrap is None:
            raise SandboxUnavailable("bwrap is required to replay agent commands")
        argv = [bwrap, *(self._isolated_root() if self.isolated else self._host_root())]
        for host, inside in self.read_only.items():
            argv += ["--ro-bind", str(host), inside]
        for host, inside in self.binds.items():
            argv += ["--bind", str(host), inside]
        argv += [
            *([] if self.network else ["--unshare-net"]),
            "--unshare-pid",
            "--unshare-ipc",
            "--die-with-parent",
            "--new-session",
        ]
        argv += ["--clearenv", "--chdir", self.chdir]
        for key, value in self.env.items():
            argv += ["--setenv", key, value]
        return [*argv, *command]

    @staticmethod
    def _host_root() -> list[str]:
        # Bind the host root entry by entry, so sandbox-only paths such as /work can be created.
        argv = []
        for entry in sorted(Path("/").iterdir()):
            if entry.name in ("dev", "proc", "run", "tmp"):
                continue
            if entry.is_symlink():
                argv += ["--symlink", os.readlink(entry), str(entry)]
            elif entry.is_dir():
                argv += ["--ro-bind", str(entry), str(entry)]
        return [*argv, "--dev", "/dev", "--proc", "/proc", "--tmpfs", "/run", "--tmpfs", "/tmp"]

    @staticmethod
    def _isolated_root() -> list[str]:
        argv = ["--tmpfs", "/", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp", "--ro-bind", "/usr", "/usr"]
        for link in ("bin", "sbin", "lib", "lib64"):
            if Path(f"/usr/{link}").exists():
                argv += ["--symlink", f"usr/{link}", f"/{link}"]
        for name in (
            "resolv.conf",
            "hosts",
            "nsswitch.conf",
            "ld.so.cache",
            "localtime",
            "ssl",
            "ca-certificates",
            "alternatives",
            "passwd",
            "group",
        ):
            path = Path("/etc") / name
            if path.exists():
                argv += ["--ro-bind-try", str(path.resolve()), str(path)]
        return argv

    def run(self, command: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                self.argv(command),
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
                stdin=subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired as expired:
            stdout = expired.stdout.decode() if isinstance(expired.stdout, bytes) else expired.stdout or ""
            return subprocess.CompletedProcess(command, 124, stdout, "timeout")


def go_environment() -> dict[str, str]:
    """Go settings that build offline against the host's module and build caches."""
    env = {"GOPROXY": "off", "GOTOOLCHAIN": "local", "GOFLAGS": "-modcacherw", "CGO_ENABLED": "0"}
    for key in ("GOCACHE", "GOMODCACHE"):
        value = (
            os.environ.get(key)
            or subprocess.run(["go", "env", key], capture_output=True, text=True, check=False).stdout.strip()
        )
        if value:
            env[key] = value
    return env


def go_binds(env: dict[str, str]) -> dict[Path, str]:
    """Writable binds for the Go caches named in env (concurrent Go builds share them safely)."""
    binds = {}
    for key in ("GOCACHE", "GOMODCACHE"):
        if key in env:
            path = Path(env[key])
            path.mkdir(parents=True, exist_ok=True)
            binds[path] = str(path)
    return binds


def host_path() -> str:
    go = shutil.which("go")
    parts = [str(Path(go).parent)] if go else []
    return ":".join([*parts, "/usr/local/bin", "/usr/bin", "/bin"])
