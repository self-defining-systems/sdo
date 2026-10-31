from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop.cli import build_parser

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("option", [["--agent", "live"], ["--agent", "scripted"], ["--live-on-cache-miss"]])
def test_replay_has_no_alternate_model_path(option: list[str]) -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["replay", "--run-dir", "/tmp/run", "--incident", "0", "--stage", "reflection", *option]
        )


def test_replay_accepts_an_explicit_agent_script(tmp_path: Path) -> None:
    script = tmp_path / "script.json"
    args = build_parser().parse_args(
        ["replay", "--run-dir", str(tmp_path), "--incident", "0", "--stage", "reflection", "--script", str(script)]
    )
    assert args.script == script
