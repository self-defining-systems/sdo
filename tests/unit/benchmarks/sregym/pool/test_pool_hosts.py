"""Remote control is plain ssh: a command survives quoting and runs on the lane's host."""

from __future__ import annotations

import subprocess
import time
from pathlib import Path

from benchmarks.sregym.pool.hosts import HostConfig, ShellHost


def config(tmp_path: Path, address: str | None = None) -> HostConfig:
    return HostConfig(
        prefix="c4",
        address=address,
        lease="pool-c4",
        coord=tmp_path / "coord" / "coord",
        code_root=tmp_path,
        python=Path("/usr/bin/python3"),
        work_root=tmp_path / "work",
    )


def ssh_loopback(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    """Stands in for sshd: the remote side receives one string and runs it with a shell."""
    assert argv[:3] == ["ssh", "-o", "BatchMode=yes"]
    assert "192.168.1.107" in argv
    return subprocess.run(["sh", "-c", argv[-1]], **kwargs)  # type: ignore[call-overload]


def test_remote_file_round_trip_with_awkward_paths(tmp_path: Path) -> None:
    host = ShellHost("chelan4", config(tmp_path, "192.168.1.107"), runner=ssh_loopback)
    path = tmp_path / "dir with space" / "spec 'q'.json"
    host.write_text(path, '{"a": "$HOME `x`"}\n')
    assert path.read_text() == '{"a": "$HOME `x`"}\n'
    assert host.read_text(path) == '{"a": "$HOME `x`"}\n'
    assert host.read_text(tmp_path / "absent") is None


def test_local_host_runs_directly(tmp_path: Path) -> None:
    host = ShellHost("chelan5", config(tmp_path))
    assert host.command(["echo", "x"]) == ["echo", "x"]
    assert host.load1() >= 0


def test_lease_cores_come_from_the_live_pool_lease(tmp_path: Path) -> None:
    leases = tmp_path / "coord" / "leases.tsv"
    leases.parent.mkdir()
    future, past = int(time.time()) + 3600, int(time.time()) - 60
    leases.write_text(
        f"other\tx\t40\t{future}\t-\tn\npool-c4\tPOOL1\t16\t{future}\tc4-l0\tn\npool-old\tx\t8\t{past}\t-\tn\n"
    )
    host = ShellHost("chelan4", config(tmp_path, "192.168.1.107"), runner=ssh_loopback)
    assert host.lease_cores() == 16
    expired = ShellHost("chelan4", config(tmp_path).model_copy(update={"lease": "pool-old"}))
    assert expired.lease_cores() == 0
