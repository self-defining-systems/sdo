"""Shared fixtures for fastloop source preflight tests."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[5]
REAL_RUN = subprocess.run


@dataclass(frozen=True)
class ImageRepo:
    """A git repository holding the real image build files and fixture sources."""

    path: Path

    def git(self, *args: str) -> str:
        return REAL_RUN(
            ["git", "-C", str(self.path), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.com", *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    @property
    def head(self) -> str:
        return self.git("rev-parse", "HEAD")

    def commit(self, files: dict[str, str], message: str) -> str:
        for name, text in files.items():
            (self.path / name).parent.mkdir(parents=True, exist_ok=True)
            (self.path / name).write_text(text)
        self.git("add", ".")
        self.git("commit", "-qm", message)
        return self.head


@pytest.fixture
def image_repo(tmp_path: Path) -> ImageRepo:
    repo = ImageRepo(tmp_path / "repo")
    repo.path.mkdir()
    repo.git("init", "-q")
    repo.commit(
        {
            "scripts/sdo-images.hcl": (ROOT / "scripts/sdo-images.hcl").read_text(),
            "sdo/operator/Dockerfile.runtime": (ROOT / "sdo/operator/Dockerfile.runtime").read_text(),
            ".dockerignore": (ROOT / ".dockerignore").read_text(),
            "sdo/operator/go.mod": "module fixture\n",
            "sdo/operator/main.go": "package main\n",
            "sdo/agents/registrations/responder.json": "{}\n",
            "benchmarks/sregym/fastloop/admission.py": "BUDGET = 1\n",
        },
        "image inputs",
    )
    return repo
