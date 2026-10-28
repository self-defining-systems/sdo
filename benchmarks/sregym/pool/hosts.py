"""Run lane processes on their own host: locally, or over ssh for a remote host.

Why ssh rather than kind API servers bound to the LAN: every lane step is
host-local anyway (kind, docker caps, image delivery, the host-wide flock
limits under /var/tmp, that host's coord lease and load). Running the lane
process on its host keeps those limits correct without recreating clusters,
changing API certificates or exposing kube APIs on the LAN. Processes start as
transient ``systemd-run --user`` units on that host, so an ssh drop or a
scheduler restart never kills a running pair.
"""

from __future__ import annotations

import shlex
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from pydantic import BaseModel, ConfigDict, Field

from benchmarks.sregym.pool.placement import DEFAULT_MAX_LOAD

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

COORD = Path("/mnt/data/shli/coord/coord")
ENV_SCRIPT = Path("/mnt/data/shli/cache/env.sh")
_ABSENT = 3


class HostConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    #: Lane id prefix, e.g. ``c5``.
    prefix: str = Field(pattern=r"^[a-z][a-z0-9]{0,5}$")
    #: ssh destination; ``None`` runs on the scheduler's host.
    address: str | None = None
    lease: str = Field(min_length=1)
    coord: Path = COORD
    code_root: Path
    python: Path
    #: Host directory for this run's lanes, specs, pair outputs and logs.
    work_root: Path
    max_load: float = Field(default=DEFAULT_MAX_LOAD, gt=0)
    nice: int = Field(default=10, ge=0, le=19)
    env_script: Path | None = ENV_SCRIPT


class Host(Protocol):
    name: str

    def read_text(self, path: Path) -> str | None: ...
    def write_text(self, path: Path, text: str) -> None: ...
    def spawn(self, unit: str, argv: Sequence[str], log: Path) -> None: ...
    def active(self, unit: str) -> bool: ...
    def load1(self) -> float: ...
    def lease_cores(self) -> float: ...
    def gate(self) -> bool: ...


Runner = "Callable[..., subprocess.CompletedProcess[str]]"


class ShellHost:
    def __init__(
        self,
        name: str,
        config: HostConfig,
        runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self.name, self.config, self._runner = name, config, runner

    def command(self, argv: Sequence[str]) -> list[str]:
        if self.config.address is None:
            return list(argv)
        return [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ServerAliveInterval=15",
            self.config.address,
            shlex.join(argv),
        ]

    def run(
        self, argv: Sequence[str], *, stdin: str | None = None, timeout: float = 60
    ) -> subprocess.CompletedProcess[str]:
        return self._runner(
            self.command(argv), input=stdin, capture_output=True, text=True, timeout=timeout, check=False
        )

    def _checked(self, argv: Sequence[str], **kwargs: object) -> str:
        result = self.run(argv, **kwargs)  # type: ignore[arg-type]
        if result.returncode:
            raise RuntimeError(
                f"pool: {self.name}: {shlex.join(argv)} exited {result.returncode}: {result.stderr[-500:]}"
            )
        return result.stdout

    def read_text(self, path: Path) -> str | None:
        result = self.run(["sh", "-c", f'test -e "$1" || exit {_ABSENT}; cat "$1"', "sh", str(path)])
        if result.returncode == _ABSENT:
            return None
        if result.returncode:
            raise RuntimeError(f"pool: {self.name}: cannot read {path}: {result.stderr[-500:]}")
        return result.stdout

    def write_text(self, path: Path, text: str) -> None:
        script = 'mkdir -p "$(dirname "$1")" && cat > "$1.tmp" && mv "$1.tmp" "$1"'
        self._checked(["sh", "-c", script, "sh", str(path)], stdin=text)

    def spawn(self, unit: str, argv: Sequence[str], log: Path) -> None:
        self._checked(["mkdir", "-p", str(log.parent)])
        wrapped = list(argv)
        if self.config.env_script is not None:
            wrapped = ["bash", "-c", 'source "$0" && exec "$@"', str(self.config.env_script), *wrapped]
        self._checked(
            [
                "systemd-run",
                "--user",
                "--collect",
                f"--unit={unit}",
                f"--working-directory={self.config.code_root}",
                f"--property=StandardOutput=append:{log}",
                f"--property=StandardError=append:{log}",
                "--",
                "nice",
                "-n",
                str(self.config.nice),
                *wrapped,
            ]
        )

    def active(self, unit: str) -> bool:
        state = self.run(["systemctl", "--user", "is-active", unit]).stdout.strip()
        return state in {"active", "activating", "reloading", "deactivating"}

    def load1(self) -> float:
        return float(self._checked(["cat", "/proc/loadavg"]).split()[0])

    def lease_cores(self) -> float:
        """Cores of the pool's coord lease on this host; no lease means no room for new lanes."""
        text = self.read_text(self.config.coord.parent / "leases.tsv") or ""
        for line in text.splitlines():
            fields = line.split("\t")
            if len(fields) >= 4 and fields[0] == self.config.lease and float(fields[3]) > time.time():
                return float(fields[2])
        return 0.0

    def gate(self) -> bool:
        argv = [str(self.config.coord), "gate", str(self.config.max_load), "--lease", self.config.lease]
        return self.run(argv).returncode == 0
