"""Synthetic hermetic judge example used by the native broker contract test."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from sdo.agents.health_judge.worker import run_judge_task
from sdo.contracts import AgentTask, HealthJudgeRequest, to_canonical_json

from .cli import launch


class Publisher:
    def publish_payload(self, name: str, key: str, payload: str) -> None:
        print(payload)


def main() -> int:
    if sys.argv[1] == "worker":
        run_judge_task(Path(sys.argv[2]), "synthetic", Publisher())
        return 0
    checkout, script = map(Path, sys.argv[1:])
    base = subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    plan = json.loads(script.read_text())
    plan["turns"][0]["result"]["objective_digest"] = hashlib.sha256(b"stay healthy").hexdigest()
    script.write_text(json.dumps(plan))
    with tempfile.TemporaryDirectory(prefix="sdo-script-judge-") as temporary:
        task = AgentTask(
            schema_version="sdo.dev/v1alpha1",
            task_id="synthetic-judge",
            agent_id="sdo-health-judge",
            task_type="health-judgment-direct-v1",
            workspace={"proposal_id": "script-judge", "checkout_path": str(checkout), "base_commit": base},
        )
        task.request.Pack(
            HealthJudgeRequest(
                application="synthetic",
                namespace="demo",
                health_objective="stay healthy",
                accepted_source_commit=base,
                architecture_summary_markdown="Synthetic application for judge authoring.",
                source_topology_fingerprint="a" * 64,
                attempt=1,
            )
        )
        task_path = Path(temporary) / "task.json"
        task_path.write_text(to_canonical_json(task))
        os.environ["SDO_AGENT_SCRIPT"] = str(script)
        os.environ["SDO_TASK_PATH"] = str(task_path)
        return launch([sys.executable, "-m", "benchmarks.sregym.agent_script.judge_example", "worker", str(task_path)])


if __name__ == "__main__":
    raise SystemExit(main())
