"""Real CLI and accepted repository setup for reflection provider contract tests."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def memory_binary(tmp_path_factory: pytest.TempPathFactory, operator_binary: Path) -> Path:
    binary = tmp_path_factory.mktemp("memory-cli") / "sdo-operator"
    binary.symlink_to(operator_binary)
    return binary


@pytest.fixture
def prepare_reflection_memory(memory_binary: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[..., str]:
    def prepare(root: Path, incident_id: str = "incident", *, outcome_record: dict[str, object] | None = None) -> str:
        root.mkdir(parents=True, exist_ok=True)

        def git(*args: str) -> str:
            return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()

        git("init", "-q")
        git("config", "user.name", "Fixture")
        git("config", "user.email", "fixture@example.test")
        (root / "app.txt").write_text("synthetic source\n")
        git("add", "-f", ".")
        git("commit", "-qm", "accepted source")
        outcome = root / ".sdo/outcomes" / f"{hashlib.sha256(incident_id.encode()).hexdigest()}.json"
        outcome.parent.mkdir(parents=True, exist_ok=True)
        outcome.write_text(
            json.dumps(
                outcome_record
                if outcome_record is not None
                else {"schema_version": 1, "incident_id": incident_id, "classification": "success"}
            )
        )
        git("add", "-f", ".sdo")
        git("commit", "-qm", f"accepted outcome\n\nSDO-Incident: {incident_id}\nSDO-Phase: outcome")
        base = git("rev-parse", "HEAD")
        monkeypatch.setenv("PATH", f"{memory_binary.parent}{os.pathsep}{os.environ['PATH']}")
        for key, value in {
            "SDO_ACTOR": "responder",
            "SDO_PHASE": "reflection",
            "SDO_BASE_COMMIT": base,
            "SDO_INCIDENT_ID": incident_id,
            "SDO_OPERATOR_ROOT": str(ROOT / "sdo/operator"),
            "SDO_HEALTHY_BASELINE": "",
        }.items():
            monkeypatch.setenv(key, value)
        return base

    return prepare
