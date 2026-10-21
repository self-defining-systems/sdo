"""Generation survives BSR rate limits without losing committed artifacts."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("local_succeeds", [True, False])
def test_generation_falls_back_and_restores_on_failure(tmp_path: Path, local_succeeds: bool) -> None:
    root = Path(__file__).resolve().parents[3]
    shutil.copytree(root / "scripts", tmp_path / "scripts")
    for name in ("buf.gen.yaml", "buf.gen.validate.yaml"):
        shutil.copy(root / name, tmp_path / name)
    generated = [tmp_path / "sdo/operator/contracts/gen", tmp_path / "sdo/contracts/_gen"]
    for directory in generated:
        directory.mkdir(parents=True)
        (directory / "keep.txt").write_text("original")
    cached = generated[1] / "buf/validate"
    cached.mkdir(parents=True)
    (cached / "validate_pb2.py").write_text("cached")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_buf = bin_dir / "buf"
    fake_buf.write_text("""#!/bin/bash
if [[ "$1" == lint ]]; then exit 0; fi
if [[ "$*" == *buf.gen.validate.yaml* ]]; then
    echo 'resource_exhausted: too many requests' >&2; exit 1
fi
if [[ "$*" == *--template* ]]; then
    echo fallback >> calls
    if [[ "$LOCAL_SUCCEEDS" == True ]]; then exit 0; fi
    echo 'local generation failed' >&2; exit 1
fi
rm -rf sdo/operator/contracts/gen sdo/contracts/_gen
echo 'resource_exhausted: too many requests' >&2
exit 1
""")
    fake_buf.chmod(0o755)
    for name, output in (("protoc", "libprotoc 33.4"), ("protoc-gen-go", "")):
        executable = bin_dir / name
        executable.write_text(f"#!/bin/sh\necho '{output}'\n")
        executable.chmod(0o755)
    result = subprocess.run(
        ["bash", "scripts/gen_proto.sh"],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "GOBIN": str(bin_dir),
            "LOCAL_SUCCEEDS": str(local_succeeds),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) == local_succeeds, result.stderr
    assert (tmp_path / "calls").read_text() == "fallback\n"
    for directory in generated:
        assert (directory / "keep.txt").read_text() == "original"
    assert (cached / "validate_pb2.py").read_text() == "cached"
