"""A worker image can commit its first repair without host Git configuration."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path


def test_worker_image_can_commit_with_no_git_identity_config(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[4]
    image = (root / "sdo/operator/Dockerfile.runtime").read_text()
    identity = dict(re.findall(r'\b(GIT_(?:AUTHOR|COMMITTER)_(?:NAME|EMAIL))="([^"]+)"', image))
    assert len(identity) == 4
    environment = {"PATH": os.environ["PATH"], "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull, **identity}
    subprocess.run(["git", "init", str(tmp_path)], env=environment, check=True, capture_output=True)
    (tmp_path / "repair.txt").write_text("verified source repair\n")
    subprocess.run(["git", "add", "repair.txt"], cwd=tmp_path, env=environment, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "repair"], cwd=tmp_path, env=environment, check=True, capture_output=True)
    actual = subprocess.check_output(
        ["git", "show", "-s", "--format=%an|%ae|%cn|%ce"], cwd=tmp_path, env=environment, text=True
    ).strip()
    assert actual == "|".join(
        identity[key] for key in ("GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_NAME", "GIT_COMMITTER_EMAIL")
    )
