"""Python jobs consume exact prepared executables instead of compiling adapters."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]


def test_every_python_shard_requires_the_current_runs_tools_before_tests() -> None:
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    producer = next(
        step for step in jobs["prepare-go"]["steps"] if step.get("name") == "Publish prepared Python Go tools"
    )
    assert "if" not in producer
    assert producer["with"]["if-no-files-found"] == "error"
    steps = jobs["python"]["steps"]
    download = next(step for step in steps if step.get("name") == "Download prepared Python Go tools")
    assert download["with"]["name"] == producer["with"]["name"]
    assert "run-id" not in download["with"]
    configure = next(step for step in steps if "python_go_tools.sh env" in step.get("run", ""))
    assert "if" not in configure
    assert steps.index(download) < steps.index(configure)
    assert steps.index(configure) < steps.index(next(step for step in steps if step.get("name") == "Run Python shard"))
    assert not any("operator_test_binary.py" in step.get("run", "") for step in steps)


@pytest.mark.parametrize("missing", ["sdo-operator", "sdo-native-receipt", "sdo-submission-guard"])
def test_missing_prepared_tool_fails_before_exporting_any_environment(tmp_path: Path, missing: str) -> None:
    for name in ("sdo-operator", "sdo-native-receipt", "sdo-submission-guard"):
        if name != missing:
            tool = tmp_path / name
            tool.write_text("#!/bin/sh\nexit 0\n")
            tool.chmod(0o755)
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/python_go_tools.sh"), "env", str(tmp_path)],
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "missing prepared Python Go tool" in result.stderr
    assert result.stdout == ""


@pytest.mark.go_integration
def test_real_producer_archive_runs_in_consumer_and_rejects_stale_source(tmp_path: Path) -> None:
    root = tmp_path / "repository"
    scripts = root / "scripts"
    scripts.mkdir(parents=True)
    for name in ("python_go_tools.sh", "operator_test_binary.py"):
        shutil.copy2(ROOT / "scripts" / name, scripts / name)
    programs = {
        "sdo/operator": ("sdo-operator", "var sourceHash string\nfunc main() { fmt.Println(sourceHash) }"),
        "benchmarks/sregym/protocol/native_receipt_go": (
            "sdo-native-receipt",
            'func main() { fmt.Println("receipt") }',
        ),
        "benchmarks/sregym/protocol/submission_go": ("sdo-submission-guard", 'func main() { fmt.Println("guard") }'),
    }
    for relative, (name, source) in programs.items():
        module = root / relative
        command = module / "cmd" / name
        command.mkdir(parents=True)
        (module / "go.mod").write_text(f"module fixture.test/{name}\n\ngo 1.24.0\n")
        (command / "main.go").write_text('package main\nimport "fmt"\n' + source + "\n")
    producer = tmp_path / "producer"
    subprocess.run(["bash", str(scripts / "python_go_tools.sh"), "prepare", str(producer)], check=True)
    consumer = tmp_path / "consumer"
    consumer.mkdir()
    subprocess.run(["tar", "-xf", str(producer) + ".tar", "-C", str(consumer)], check=True)
    result = subprocess.run(
        ["bash", str(scripts / "python_go_tools.sh"), "env", str(consumer)],
        capture_output=True,
        text=True,
        check=True,
    )
    selected = dict(line.split("=", 1) for line in result.stdout.splitlines())
    assert set(selected) == {"SDO_OPERATOR_BIN", "SDO_NATIVE_RECEIPT_BIN", "SDO_SUBMISSION_GUARD_BIN"}
    for key, expected in (("SDO_NATIVE_RECEIPT_BIN", "receipt"), ("SDO_SUBMISSION_GUARD_BIN", "guard")):
        assert subprocess.check_output([selected[key]], text=True).strip() == expected
    main = root / "sdo/operator/cmd/sdo-operator/main.go"
    main.write_text(main.read_text() + "// changed core source\n")
    stale = subprocess.run(
        ["bash", str(scripts / "python_go_tools.sh"), "env", str(consumer)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert stale.returncode != 0
    assert "source identity mismatch" in stale.stderr
    assert stale.stdout == ""
