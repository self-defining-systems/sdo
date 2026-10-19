"""The agent image carries the SDO skills that each worker installs into its CLI home."""

from __future__ import annotations

from pathlib import Path

from sdo.agents.responder.credentials import AGENT_SKILLS

REPO_ROOT = Path(__file__).resolve().parents[4]
DOCKERFILE = (REPO_ROOT / "sdo" / "operator" / "Dockerfile.runtime").read_text(encoding="utf-8")


def test_the_worker_image_copies_the_skills_tree_and_checks_it() -> None:
    worker_stage = DOCKERFILE.split("AS worker-runtime", 1)[1].split("FROM worker-runtime", 1)[0]

    assert "COPY sdo /opt/sdo/sdo" in worker_stage
    for name in {skill for skills in AGENT_SKILLS.values() for skill in skills}:
        assert f"/opt/sdo/sdo/skills/{name}/SKILL.md" in worker_stage
        assert (REPO_ROOT / "sdo" / "skills" / name / "SKILL.md").is_file()
