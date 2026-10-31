"""The proto drift gate must not rewrite files being imported or compiled."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


def test_proto_drift_check_generates_outside_the_source_tree(tmp_path: Path) -> None:
    repository = Path(__file__).resolve().parents[2]
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copy(repository / "scripts/check_proto.sh", scripts / "check_proto.sh")
    for name in ("buf.yaml", "buf.lock", "buf.gen.yaml", "buf.gen.validate.yaml"):
        (tmp_path / name).write_text("")
    (tmp_path / "sdo/contracts/proto").mkdir(parents=True)
    source = tmp_path / "sdo/operator/contracts/gen"
    source.mkdir(parents=True)
    generated = source / "fixture.go"
    generated.write_text("accepted")
    (tmp_path / "sdo/contracts/_gen").mkdir(parents=True)
    (scripts / "gen_proto.sh").write_text(
        '#!/bin/bash\ncd "$(dirname "$0")/.."\nprintf changed > sdo/operator/contracts/gen/fixture.go\n'
    )
    (scripts / "gen_proto.sh").chmod(0o755)
    tools = tmp_path / "bin"
    tools.mkdir()
    (tools / "buf").write_text("#!/bin/bash\nexit 0\n")
    (tools / "buf").chmod(0o755)
    result = subprocess.run(
        ["bash", str(scripts / "check_proto.sh")],
        env={**os.environ, "GOBIN": str(tools), "PATH": f"{tools}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "Generated proto code is out of date" in result.stdout + result.stderr
    assert generated.read_text() == "accepted", "drift checks must never replace live compiler/import inputs"
