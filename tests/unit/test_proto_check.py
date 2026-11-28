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


def _check_go_errors_copy(tmp_path: Path, check_proto: str) -> Path:
    repository = Path(__file__).resolve().parents[2]
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copy(repository / "scripts/check_go_errors.sh", scripts / "check_go_errors.sh")
    (scripts / "check_proto.sh").write_text(check_proto)
    for name in (
        "check_actions.sh",
        "gofmt.sh",
        "gen_skill_references.sh",
        "gen_config_docs.sh",
        "gen_kube_read_clients.sh",
    ):
        (scripts / name).write_text("#!/bin/bash\nexit 0\n")
    (scripts / "go_quality_checks.py").write_text("")
    for name in (
        "check_go_determinism.sh",
        "check_go_core.sh",
        "check_go_mains.sh",
        "check_lanes_layering.sh",
        "check_contracts_fast.sh",
        "test_go_ci.sh",
    ):
        (scripts / name).write_text("#!/bin/bash\necho ran-$(basename $0)\n")
    for script in scripts.glob("*.sh"):
        script.chmod(0o755)
    tools = tmp_path / "bin"
    tools.mkdir()
    (tools / "buf").write_text("#!/bin/bash\nexit 0\n")
    (tools / "buf").chmod(0o755)
    return scripts / "check_go_errors.sh"


def _run_go_errors(script: Path, tmp_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(script)],
        env={**os.environ, "GOBIN": str(tmp_path / "bin"), "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        check=False,
    )


def test_an_unavailable_proto_toolchain_is_a_loud_skip_and_later_stages_run(tmp_path: Path) -> None:
    script = _check_go_errors_copy(tmp_path, "#!/bin/bash\nexit 75\n")
    result = _run_go_errors(script, tmp_path)
    assert result.returncode == 0
    assert "SKIPPED: proto contracts" in result.stderr
    assert "ran-check_go_mains.sh" in result.stdout, "stages after the proto stage must still run"
    assert "ran-check_lanes_layering.sh" in result.stdout, "the layer guard is part of the gate"
    assert "ran-test_go_ci.sh" in result.stdout, "the whole-tree guards are part of the gate"
    assert "SKIPPED stages" in result.stderr
    assert "proto contracts: buf/BSR" in result.stderr


def test_a_real_proto_failure_still_fails_the_gate(tmp_path: Path) -> None:
    script = _check_go_errors_copy(tmp_path, "#!/bin/bash\nexit 1\n")
    result = _run_go_errors(script, tmp_path)
    assert result.returncode == 1
    assert "ran-check_go_mains.sh" not in result.stdout
