"""Contract of scripts/build_push_bench_images.sh without building or reaching any host."""

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPT = REPO_ROOT / "scripts" / "build_push_bench_images.sh"


def test_script_is_valid_bash_and_executable() -> None:
    assert SCRIPT.stat().st_mode & 0o111
    assert subprocess.run(["bash", "-n", str(SCRIPT)], check=False).returncode == 0


def test_script_requires_a_revision_and_a_host() -> None:
    result = subprocess.run(["bash", str(SCRIPT), "deadbeef"], capture_output=True, text=True, check=False)
    assert result.returncode == 2
    assert "usage:" in result.stderr


def test_script_limits_parallel_imports_per_host_to_three() -> None:
    result = subprocess.run(
        ["bash", str(SCRIPT), "HEAD", "local"],
        capture_output=True,
        text=True,
        check=False,
        env={"PATH": "/usr/bin:/bin", "SDO_BENCH_PARALLEL": "4"},
    )
    assert result.returncode == 2
    assert "SDO_BENCH_PARALLEL" in result.stderr


def test_script_distributes_by_registry_not_by_image_archive() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    assert "docker push" in text
    assert "docker save" not in text
    assert "docker load" not in text
    # The build script lets the caller run the smoke gates itself.
    assert "SDO_IMAGE_SMOKE" in (REPO_ROOT / "scripts" / "build_sdo_images.sh").read_text(encoding="utf-8")
