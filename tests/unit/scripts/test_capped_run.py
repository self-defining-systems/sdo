"""scripts/capped_run.sh: env limits always, a systemd memory scope when it verifiably applies, nice otherwise."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "capped_run.sh"


def _run(tmp_path: Path, args: list[str], *, systemd: str | None, env: dict[str, str] | None = None):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "calls.log"
    # `nice` and `ionice` are stand-ins that log and exec the rest, so the test never lowers real priorities.
    for name in ("nice", "ionice"):
        path = bin_dir / name
        path.write_text(f'#!/bin/sh\necho "{name} $*" >> "{log}"\nshift 2\nexec "$@"\n')
    if systemd is not None:
        (bin_dir / "systemd-run").write_text(systemd.replace("@LOG@", str(log)))
    for path in bin_dir.iterdir():
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    environ = {"PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": str(tmp_path), "GOFLAGS": "-mod=mod", **(env or {})}
    result = subprocess.run(["/bin/bash", str(SCRIPT), *args], capture_output=True, text=True, env=environ, check=False)
    return result, (log.read_text() if log.exists() else "")


PRINT_ENV = ["--", "sh", "-c", 'echo "$GOFLAGS|$GOMAXPROCS"']

# A fake systemd-run whose scope reports a real memory.max for the probe and execs the command otherwise.
SCOPE_OK = """#!/bin/sh
echo "systemd-run $*" >> "@LOG@"
case "$*" in
  *cgroup*) echo 17179869184 ;;
  *) while [ "$1" != "--" ]; do shift; done; shift; exec "$@" ;;
esac
"""
SCOPE_NO_CONTROLLER = """#!/bin/sh
case "$*" in *cgroup*) echo max ;; *) exit 1 ;; esac
"""


def test_limits_are_set_and_nice_ionice_is_the_fallback_without_systemd(tmp_path: Path) -> None:
    result, calls = _run(tmp_path, PRINT_ENV, systemd=None)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "-mod=mod -p=4|8"
    assert "nice -n 10" in calls
    assert "ionice -c 3" in calls
    assert "nice/ionice" in result.stderr


def test_a_scope_that_does_not_apply_memory_max_falls_back(tmp_path: Path) -> None:
    result, calls = _run(tmp_path, PRINT_ENV, systemd=SCOPE_NO_CONTROLLER)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "-mod=mod -p=4|8"
    assert "nice -n 10" in calls


def test_a_working_scope_runs_the_command_under_memory_max_16g_by_default(tmp_path: Path) -> None:
    result, calls = _run(tmp_path, PRINT_ENV, systemd=SCOPE_OK)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "-mod=mod -p=4|8"
    assert "--user --scope -q -p MemoryMax=16G -- sh -c" in calls
    assert "nice" not in calls


def test_memory_is_configurable_by_flag_and_environment(tmp_path: Path) -> None:
    _, calls = _run(tmp_path, ["--memory", "4G", *PRINT_ENV], systemd=SCOPE_OK)
    assert "MemoryMax=4G" in calls
    other = tmp_path / "env"
    other.mkdir()
    _, calls = _run(other, PRINT_ENV, systemd=SCOPE_OK, env={"SDO_CAPPED_MEMORY_MAX": "8G"})
    assert "MemoryMax=8G" in calls


def test_exit_status_and_usage(tmp_path: Path) -> None:
    result, _ = _run(tmp_path, ["--", "sh", "-c", "exit 7"], systemd=None)
    assert result.returncode == 7
    usage = subprocess.run(
        ["/bin/bash", str(SCRIPT)], capture_output=True, text=True, check=False, env=dict(os.environ)
    )
    assert usage.returncode == 2
