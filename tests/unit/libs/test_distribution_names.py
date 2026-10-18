from pathlib import Path

import pytest
import tomllib

LIBS_ROOT = Path(__file__).parents[3] / "libs"


@pytest.mark.parametrize(
    ("library", "distribution"),
    [
        ("agent_cli", "sdo-agent-cli"),
    ],
)
def test_internal_distribution_uses_sdo_name(library: str, distribution: str) -> None:
    with (LIBS_ROOT / library / "pyproject.toml").open("rb") as file:
        project = tomllib.load(file)["project"]

    assert project["name"] == distribution
