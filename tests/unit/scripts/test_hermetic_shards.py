"""The hermetic driver must join every shard and preserve every failure."""

import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("failure", ["none", "first_runtime", "second_runtime", "reflection", "python"])
def test_hermetic_driver_propagates_each_shard_failure(tmp_path: Path, failure: str) -> None:
    root = Path(__file__).resolve().parents[3]
    commands = tmp_path / "bin"
    commands.mkdir()
    log = tmp_path / "commands.log"
    go = commands / "go"
    go.write_text(
        "#!/bin/bash\n"
        'printf "go %s\\n" "$*" >> "$LG_DRIVER_LOG"\n'
        'case "$*" in\n'
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
        '#!/bin/bash\nprintf "uv\\n" >> "$LG_DRIVER_LOG"\nif [[ "$LG_DRIVER_FAILURE" == "python" ]]; then exit 7; fi\n'
    )
    uv.chmod(0o755)
    env = {
        **os.environ,
        "PATH": str(commands) + os.pathsep + os.environ["PATH"],
        "LG_DRIVER_LOG": str(log),
        "LG_DRIVER_FAILURE": failure,
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
    # Python is a joined parallel shard: it must run even when another shard
    # fails, and its own failure must propagate after every Go shard is joined.
    assert "uv" in lines
    assert any("TestLearnedOriginCredit" in line for line in lines)
    assert any("TestHermeticControllerClosureReachesNativeOutcomeBroker" in line for line in lines)
