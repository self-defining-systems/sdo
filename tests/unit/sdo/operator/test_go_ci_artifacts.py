"""The real artifact producer must feed a consumer with no base cache."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]


@pytest.mark.parametrize("mode", ["normal", "python"])
def test_prepared_artifact_round_trip_without_base_cache(tmp_path: Path, mode: str) -> None:
    source = tmp_path / "repo"
    (source / "scripts").mkdir(parents=True)
    shutil.copy2(ROOT / "scripts/go_ci_artifacts.sh", source / "scripts")
    cache = tmp_path / "producer-cache"
    cache.mkdir()
    (cache / "compiled-a").write_text("compiler archive")
    modules = tmp_path / "modules"
    modules.mkdir(parents=True)
    (modules / "module.zip").write_text("module archive")
    tools = source / ".fastloop/test-bin"
    tools.mkdir(parents=True)
    executable = tools / "setup"
    executable.write_text("executable")
    executable.chmod(0o755)
    artifact = tmp_path / "artifact"
    env = {**os.environ, "GOMODCACHE": str(modules), "GOCACHE": str(cache)}
    command = ["bash", str(source / "scripts/go_ci_artifacts.sh")]
    subprocess.run([*command, "pack", mode, str(artifact)], env=env, check=True)
    shutil.rmtree(cache)
    shutil.rmtree(modules)
    shutil.rmtree(tools)
    subprocess.run([*command, "unpack", mode, str(artifact)], env=env, check=True)
    if mode == "normal":
        assert (cache / "compiled-a").read_text() == "compiler archive"
    else:
        assert not cache.exists()
    if mode in {"normal", "python"}:
        assert (modules / "module.zip").read_text() == "module archive"
        assert executable.read_text() == "executable"
        assert executable.stat().st_mode & 0o777 == 0o755
    else:
        assert not modules.exists()
        assert not tools.exists()
