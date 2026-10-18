"""The deterministic source worker must change each accepted checkout."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from tests.fixtures.sdo.native_smoke.worker import write_source_marker

if TYPE_CHECKING:
    from pathlib import Path


def test_source_marker_changes_after_previous_proposal_is_accepted(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "config", "user.name", "fixture"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "config", "user.email", "fixture@invalid"], check=True)
    (tmp_path / "README.md").write_text("seed\n")
    subprocess.run(["git", "-C", str(tmp_path), "add", "README.md"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "commit", "-qm", "seed"], check=True)

    first_base = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    write_source_marker(tmp_path, first_base)
    first = (tmp_path / "source-smoke.txt").read_text()
    subprocess.run(["git", "-C", str(tmp_path), "add", "source-smoke.txt"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "commit", "-qm", "accepted source"], check=True)

    second_base = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    assert second_base != first_base
    write_source_marker(tmp_path, second_base)
    assert (tmp_path / "source-smoke.txt").read_text() != first
    status = subprocess.run(
        ["git", "-C", str(tmp_path), "status", "--short"], capture_output=True, text=True, check=True
    )
    assert status.stdout
