"""Generate the scripted health refresh for an already accepted, hand-authored seed.

The judge only retains the seed's judge-owned health detectors and adds the
explicit objective detector the native judge publication requires, by reusing the
seed's own readiness policy. A seed without judge-owned health detectors needs a
real judge, so this raises instead of weakening the accepted bundle.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from benchmarks.sregym.agent_script.model import AgentScript, ScriptTurn, Write
from sdo.contracts import HealthJudgeRequest

HEALTH = Path(".sdo/diagnostics/detectors/health")
OBJECTIVE_DETECTOR = "health-objective"


def judge_script(*, checkout: Path, request: HealthJudgeRequest) -> AgentScript:
    """Read the accepted checkout; the generated script performs every edit."""
    existing: dict[str, Path] = {}
    for manifest_path in sorted((checkout / HEALTH).glob("*/manifest.yaml")):
        manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict) or manifest.get("class") != "health" or manifest.get("owner") != "health_judge":
            raise ValueError(f"scripted health refresh requires judge-owned health detectors ({manifest_path})")
        existing[str(manifest["id"])] = manifest_path.parent
    if not existing:
        raise ValueError("scripted health refresh needs an accepted seed with judge-owned health detectors")
    steps: list[Write] = []
    if OBJECTIVE_DETECTOR not in existing:
        source = existing[sorted(existing)[0]]
        for name in ("manifest.yaml", "detector.go"):
            text = (source / name).read_text(encoding="utf-8").replace(source.name, OBJECTIVE_DETECTOR)
            steps.append(Write(kind="write", path=str(HEALTH / OBJECTIVE_DETECTOR / name), text=text))
    ids = sorted({*existing, OBJECTIVE_DETECTOR})
    return AgentScript(
        version=1,
        turns=[
            ScriptTurn(
                role="health-judge",
                steps=steps,
                result={
                    "authored": True,
                    "summary": "Retained the accepted health detectors and added the explicit objective detector",
                    "agent_session_id": "scripted-health-" + request.accepted_source_commit[:12],
                    "detector_ids": ids,
                },
            )
        ],
    )
