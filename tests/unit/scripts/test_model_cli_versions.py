"""The shipped model defaults must be usable by the CLIs installed in the image."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
DOCKERFILE = ROOT / "sdo/operator/Dockerfile.runtime"
CHECKER = ROOT / "scripts/check_model_cli_versions.py"
REQUIREMENTS = ROOT / "sdo/operator/model_cli_requirements.json"


def _pins() -> dict[str, str]:
    return dict(re.findall(r"^ARG (\w+)=(\S+)$", DOCKERFILE.read_text(), re.MULTILINE))


def test_shipped_claude_pin_supports_default_opus() -> None:
    assert tuple(map(int, _pins()["CLAUDE_CODE_VERSION"].split("."))) >= (2, 1, 280)


def _check(
    tmp_path: Path,
    claude: str = "2.1.280 (Claude Code)",
    codex: str = "codex-cli 0.163.0",
    *,
    claude_exit: int = 0,
    codex_exit: int = 0,
    minimum: str | None = None,
    claude_pin: str | None = None,
) -> subprocess.CompletedProcess[str]:
    # Only the external executables are fixtures: the real build checker reads
    # the real requirements and writes the same metadata as the Dockerfile RUN.
    for name, output, code in (("claude", claude, claude_exit), ("codex", codex, codex_exit)):
        executable = tmp_path / name
        executable.write_text(
            f"#!{sys.executable}\nimport sys\n"
            "assert sys.argv[1:] == ['--version']\n"
            f"print({output!r})\nsys.exit({code})\n"
        )
        executable.chmod(0o755)
    requirements = REQUIREMENTS
    if minimum is not None:
        values = json.loads(REQUIREMENTS.read_text())
        values["minimum_cli_versions"]["claude"] = minimum
        requirements = tmp_path / "requirements.json"
        requirements.write_text(json.dumps(values))
    return subprocess.run(
        [
            sys.executable,
            str(CHECKER),
            "--requirements",
            str(requirements),
            "--claude-code-version",
            claude_pin or _pins()["CLAUDE_CODE_VERSION"],
            "--codex-version",
            _pins()["CODEX_VERSION"],
            "--metadata",
            str(tmp_path / "model-cli-versions.json"),
        ],
        env={**os.environ, "PATH": str(tmp_path)},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def test_build_records_both_measured_cli_versions_and_model_requirements(tmp_path: Path) -> None:
    result = _check(tmp_path)
    assert result.returncode == 0, result.stderr
    metadata = json.loads((tmp_path / "model-cli-versions.json").read_text())
    assert metadata["cli_versions"] == {"claude": "2.1.280", "codex": "0.163.0"}
    assert metadata["minimum_cli_versions"]["claude"] == "2.1.280"
    assert "claude-opus-5-5" in metadata["default_models"]["claude"]
    assert json.loads(result.stdout) == metadata


@pytest.mark.parametrize("version", ["2.1.258", "2.1.279", "2.0.999", "1.99.999"])
def test_build_rejects_old_claude_before_publishing_metadata(tmp_path: Path, version: str) -> None:
    result = _check(tmp_path, claude=f"{version} (Claude Code)")
    assert result.returncode == 1
    assert f"Claude Code {version}" in result.stderr
    assert "requires >=2.1.280" in result.stderr
    assert "claude-opus-5-5" in result.stderr
    assert not (tmp_path / "model-cli-versions.json").exists()


@pytest.mark.parametrize("version", ["2.1.280", "2.1.999", "2.2.0", "2.10.0", "3.0.0"])
def test_newer_stable_pins_pass_numeric_version_comparison(tmp_path: Path, version: str) -> None:
    result = _check(tmp_path, claude=f"{version} (Claude Code)", claude_pin=version)
    assert result.returncode == 0, result.stderr
    assert json.loads((tmp_path / "model-cli-versions.json").read_text())["cli_versions"]["claude"] == version


def test_raising_model_minimum_rejects_previously_accepted_cli(tmp_path: Path) -> None:
    result = _check(tmp_path, minimum="2.1.281")
    assert result.returncode == 1
    assert "requires >=2.1.281" in result.stderr


@pytest.mark.parametrize(
    ("claude", "codex", "claude_exit", "codex_exit"),
    [
        ("unknown", "codex-cli 0.163.0", 0, 0),
        ("2.1.280-beta (Claude Code)", "codex-cli 0.163.0", 0, 0),
        ("2.1.280 (Claude Code)", "unknown", 0, 0),
        ("2.1.280 (Claude Code)", "codex-cli 0.163.0", 1, 0),
        ("2.1.280 (Claude Code)", "codex-cli 0.163.0", 0, 1),
        ("2.1.281 (Claude Code)", "codex-cli 0.163.0", 0, 0),
        ("2.1.280 (Claude Code)", "codex-cli 0.164.0", 0, 0),
    ],
)
def test_unmeasurable_or_unpinned_cli_fails_closed(
    tmp_path: Path, claude: str, codex: str, claude_exit: int, codex_exit: int
) -> None:
    result = _check(tmp_path, claude, codex, claude_exit=claude_exit, codex_exit=codex_exit)
    assert result.returncode == 1
    assert not (tmp_path / "model-cli-versions.json").exists()


def test_model_stage_runs_checker_and_preserves_version_metadata() -> None:
    stage = DOCKERFILE.read_text().split(" AS model-deps\n", 1)[1].split("\nFROM ", 1)[0]
    assert "COPY scripts/check_model_cli_versions.py /usr/local/lib/sdo/check_model_cli_versions.py" in stage
    assert "COPY sdo/operator/model_cli_requirements.json /usr/local/lib/sdo/model_cli_requirements.json" in stage
    assert "python3 /usr/local/lib/sdo/check_model_cli_versions.py" in stage
    assert '--claude-code-version "${CLAUDE_CODE_VERSION}"' in stage
    assert '--codex-version "${CODEX_VERSION}"' in stage
    assert "--metadata /opt/sdo/model-cli-versions.json" in stage
    assert 'dev.sdo.claude-code.version="${CLAUDE_CODE_VERSION}"' in stage
    assert 'dev.sdo.codex.version="${CODEX_VERSION}"' in stage
