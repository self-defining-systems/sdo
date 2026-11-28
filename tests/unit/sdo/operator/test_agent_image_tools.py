"""Agent images carry the shell tools an operator expects next to kubectl."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]

# An agent stops what it started in the background (`pkill`, `ps`): procps.
AGENT_TOOL_PACKAGES = {"bash", "curl", "jq", "tar", "procps"}


def _apt_packages(source: str, stage: str) -> set[str]:
    sections = re.split(r"^FROM .* AS (\S+)\n", source, flags=re.MULTILINE)
    for name, body in zip(sections[1::2], sections[2::2], strict=True):
        if name == stage:
            match = re.search(r"apt-get install --yes --no-install-recommends ([^\\&]+)", body)
            assert match, f"{stage} installs no packages"
            return set(match.group(1).split())
    raise AssertionError(f"no stage {stage}")


def test_responder_and_script_tool_images_carry_the_agent_tool_floor() -> None:
    source = (ROOT / "sdo/operator/Dockerfile.runtime").read_text()
    for stage in ("runtime-base", "script-tools-base"):
        missing = AGENT_TOOL_PACKAGES - _apt_packages(source, stage)
        assert not missing, f"{stage} lacks {sorted(missing)}"
