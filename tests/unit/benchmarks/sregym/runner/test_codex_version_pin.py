"""The stock Codex baseline runs the same Codex CLI version as the SDO images."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from benchmarks.sregym.runner.experiment import ExperimentConfig, config_to_env

ROOT = Path(__file__).resolve().parents[5]


def _registry() -> dict[str, dict[str, object]]:
    document = yaml.safe_load((ROOT / "benchmarks" / "sregym" / "registry.yaml").read_text(encoding="utf-8"))
    return {entry["name"]: entry for entry in document["agents"]}


def _sdo_image_codex_version() -> str:
    dockerfile = (ROOT / "sdo" / "operator" / "Dockerfile.runtime").read_text(encoding="utf-8")
    match = re.search(r"^ARG CODEX_VERSION=(\S+)$", dockerfile, flags=re.MULTILINE)
    assert match is not None
    return match.group(1)


def test_the_codex_baseline_pins_the_codex_version_the_sdo_images_use() -> None:
    codex = _registry()["codex"]

    assert codex["agent_version"] == _sdo_image_codex_version()
    assert codex["install_script"] == "install-codex.sh"
    assert codex["kickoff_command"] == "python -m clients.codex.driver"


def test_the_codex_baseline_launches_through_the_first_party_registry(tmp_path: Path) -> None:
    env = config_to_env(ExperimentConfig(agent="codex"), ROOT)

    assert env["SREGYM_AGENT_REGISTRY"] == str(ROOT / "benchmarks" / "sregym" / "registry.yaml")
