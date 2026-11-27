"""Pull requests never run SREGym's own uv environment.

Syncing it takes ~30 s on a runner and was charged to whichever tests ran first. A test may only run it when
marked ``sregym_env`` (tests/conftest.py fails the others), and a file with that mark is nightly-only.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

CI_WHOLE_TREE = True

ROOT = Path(__file__).resolve().parents[2]
HARNESS = ROOT / "third_party" / "sregym"


def test_files_that_run_the_sregym_environment_are_nightly_only() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    ignored = set(re.findall(r"--ignore=(tests/\S+\.py)", workflow))
    mark = re.compile(r"^\s*(?:@|pytestmark\s*=.*)pytest\.mark\.sregym_env\b", re.MULTILINE)
    marked = {p.relative_to(ROOT).as_posix() for p in (ROOT / "tests").rglob("test_*.py") if mark.search(p.read_text())}
    assert marked, "the sregym_env marker moved"
    assert marked <= ignored, f"pull requests would sync SREGym's environment: {sorted(marked - ignored)}"
    # No background sync either: nothing in the pull request tier uses it, and it competes with the tests.
    assert "third_party/sregym" not in (ROOT / "scripts/test_python.sh").read_text()


def test_an_unmarked_test_cannot_run_the_sregym_environment() -> None:
    reply = subprocess.run(
        ["uv", "run", "--directory", str(HARNESS), "python", "-c", "pass"], capture_output=True, text=True, check=False
    )
    assert reply.returncode == 97
    assert "pytest.mark.sregym_env" in reply.stderr


def test_other_uv_commands_still_run() -> None:
    reply = subprocess.run(["uv", "--version"], capture_output=True, text=True, check=False)
    assert reply.returncode == 0
    assert reply.stdout.startswith("uv ")
