"""Both Codex arms run the same Codex CLI.

The SDO runtime image pins ``CODEX_VERSION``. The stock Codex baseline installs
its CLI at container start from SREGym's agent registry; an unpinned entry
installs npm ``latest``, which changes between runs and has shipped releases
whose platform package 404s (0.158.0, 2026-09-28).
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[5]


def _sdo_codex_version() -> str:
    dockerfile = (ROOT / "sdo" / "operator" / "Dockerfile.runtime").read_text(encoding="utf-8")
    match = re.search(r"^ARG CODEX_VERSION=(\S+)$", dockerfile, re.MULTILINE)
    assert match is not None
    return match.group(1)


def test_stock_codex_agent_installs_the_sdo_image_codex_cli_version() -> None:
    registry = yaml.safe_load((ROOT / "third_party" / "sregym" / "agents.yaml").read_text(encoding="utf-8"))
    codex = next(agent for agent in registry["agents"] if agent["name"] == "codex")

    assert codex["install_script"] == "install-codex.sh"
    assert str(codex["agent_version"]) == _sdo_codex_version()
