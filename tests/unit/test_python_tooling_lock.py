"""Repository formatters and linters resolve to the locked version."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Tools that scripts/format_code.sh, scripts/check_errors.sh and pre-commit run
# without an `--extra`. A tool outside the default sync set makes `uv run`
# fall back to whatever copy is on PATH, whose formatting can differ from CI.
DEFAULT_ENV_TOOLS = ("ruff", "pyright")


def _requirement_names(requirements: list[str]) -> set[str]:
    return {re.split(r"[\s<>=!~;\[]", requirement, maxsplit=1)[0].lower() for requirement in requirements}


def test_check_tools_are_in_the_default_uv_environment() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    default_groups = pyproject.get("tool", {}).get("uv", {}).get("default-groups", ["dev"])
    installed = _requirement_names(pyproject["project"]["dependencies"])
    for group in default_groups:
        installed |= _requirement_names(pyproject["dependency-groups"][group])

    missing = [tool for tool in DEFAULT_ENV_TOOLS if tool not in installed]

    assert not missing, (
        f"{missing} must be in project dependencies or a default uv dependency group "
        f"({default_groups}) so `uv run` uses the uv.lock version, not a PATH copy"
    )
