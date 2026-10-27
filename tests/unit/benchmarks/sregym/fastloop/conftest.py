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
            "sdo/operator/main_test.go": "package main\n",
            "sdo/operator/testdata/case.json": "{}\n",
            "sdo/operator/README.md": "# operator\n",
            "sdo/agents/registrations/responder.json": "{}\n",
            "benchmarks/sregym/fastloop/admission.py": "BUDGET = 1\n",
        },
        "image inputs",
    )
    return repo


@pytest.fixture(autouse=True)
def bridge_lane_guards(monkeypatch: pytest.MonkeyPatch):
    """Worker-bridge tests run against stub SREGym modules; the idempotent-Helm guard would patch a stub ``Helm``."""
    from benchmarks.sregym.fastloop import worker_bridge

    monkeypatch.setattr(worker_bridge, "_make_deploy_idempotent", lambda: None)


@pytest.fixture(autouse=True)
def row_start_wipe_guards(monkeypatch: pytest.MonkeyPatch):
    """CLI tests run on synthetic kubeconfigs; the row-start wipe and lane inventory would call kubectl on them.

    test_wipe.py exercises the wipe itself against a fake lane.
    """
    from benchmarks.sregym.fastloop import cli, wipe

    monkeypatch.setattr(cli, "_wipe_sdo_and_harness", lambda *_args: wipe.Tier1Receipt(0.0, (), False, ()))
    monkeypatch.setattr(cli, "_application_needs_redeploy", lambda *_args: None)
    monkeypatch.setattr(wipe.KubectlLaneCluster, "list_objects", lambda self: frozenset())
