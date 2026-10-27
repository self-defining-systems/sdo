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


class _EmptyLaneInventory:
    def list_objects(self) -> frozenset[str]:
        return frozenset()

    def delete(self, identity: str) -> None:
        raise AssertionError(f"unexpected lane delete {identity}")


@pytest.fixture(autouse=True)
def bridge_lane_guards(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Worker-bridge tests run against stub SREGym modules and a synthetic kubeconfig.

    The bridge's two lane guards would otherwise patch a stub ``Helm`` and write a setup ledger next to
    a relative kubeconfig while calling kubectl. test_setup_ledger.py overrides this fixture to run them.
    """
    from benchmarks.sregym.fastloop import worker_bridge

    monkeypatch.setattr(worker_bridge, "_make_deploy_idempotent", lambda: None)
    monkeypatch.setattr(worker_bridge, "KubectlClusterInventory", lambda kubeconfig: _EmptyLaneInventory())
    monkeypatch.setattr(worker_bridge, "ledger_path", lambda kubeconfig: tmp_path / "bridge-setup-ledger.json")
