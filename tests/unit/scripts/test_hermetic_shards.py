"""The hermetic driver must join every shard and preserve every failure."""

import os
import shlex
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("jobs", [4, 8])
@pytest.mark.parametrize(
    "failure", ["none", "first_runtime", "second_runtime", "reflection", "shared_memory", "python"]
)
def test_hermetic_driver_propagates_each_shard_failure(tmp_path: Path, failure: str, jobs: int) -> None:
    root = Path(__file__).resolve().parents[3]
    commands = tmp_path / "bin"
    commands.mkdir()
    log = tmp_path / "commands.log"
    go = commands / "go"
    go.write_text(
        "#!/bin/bash\n"
        'printf "go workers=%s %s\\n" "$GOMAXPROCS" "$*" >> "$LG_DRIVER_LOG"\n'
        'case "$*" in\n'
        "  *TestHermeticSharedMemoryControllers*) shard=shared_memory ;;\n"
        "  *TestReflectionMongoAuth*) shard=reflection ;;\n"
        "  *TestHermeticBringup*) shard=first_runtime ;;\n"
        "  *TestLearnedOriginCredit*) shard=second_runtime ;;\n"
        "  *) shard=other ;;\n"
        "esac\n"
        'if [[ "$shard" == "$LG_DRIVER_FAILURE" ]]; then exit 7; fi\n'
    )
    go.chmod(0o755)
    uv = commands / "uv"
    uv.write_text(
        '#!/bin/bash\nprintf "uv %s\\n" "$*" >> "$LG_DRIVER_LOG"\n'
        'if [[ "$LG_DRIVER_FAILURE" == "python" ]]; then exit 7; fi\n'
    )
    uv.chmod(0o755)
    env = {
        **os.environ,
        "PATH": str(commands) + os.pathsep + os.environ["PATH"],
        "LG_DRIVER_LOG": str(log),
        "LG_DRIVER_FAILURE": failure,
        "SDO_HERMETIC_JOBS": str(jobs),
    }
    result = subprocess.run(
        ["bash", str(root / "scripts/test_fastloop_hermetic.sh")],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == (0 if failure == "none" else 7), result.stderr
    lines = log.read_text().splitlines()
    assert any("TestReflectionMongoAuth" in line for line in lines)
    assert sum("TestHermeticSharedMemoryControllers" in line for line in lines) == 1
    # Python is a joined parallel shard: it must run even when another shard
    # fails, and its own failure must propagate after every Go shard is joined.
    python_commands = [line for line in lines if line.startswith("uv ")]
    assert len(python_commands) == 1
    assert {
        "tests/unit/benchmarks/sregym/fastloop/test_toy.py",
        "tests/unit/benchmarks/sregym/fastloop/test_scripted_worker.py",
    } <= set(shlex.split(python_commands[0]))
    assert any("TestLearnedOriginCredit" in line for line in lines)
    assert any("TestHermeticControllerClosureReachesNativeOutcomeBroker" in line for line in lines)

    for marker, expected in (
        ("TestReflectionMongoAuth", jobs // 2),
        ("TestHermeticSharedMemoryControllers", jobs // 2),
        ("TestHermeticBringup", jobs // 2 - 1),
        ("TestLearnedOriginCredit", jobs // 2),
    ):
        command = next(line for line in lines if marker in line)
        assert f"workers={expected} " in command, command
