"""Run one recorded reflection inside the responder image, then check and replay its edits.

The runner mounts the scenario where the live reflection Job had it (session at
``/sdo/session``, checkout at the recorded proposal path, task at
``/sdo/request/agent-task.json``) and the evaluated skills at the image's skills
root. This entry does what the live reflection worker does, publishes the result
to ``/skilleval/out`` instead of a ConfigMap, then runs an independent
``sdo-operator memory check`` and replays the incident's captured corpus cases.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from sdo.agents.responder import AGENT_SKILLS, prepare_codex_home, run_reflection_task

OUT = Path("/skilleval/out")
TASK = Path("/sdo/request/agent-task.json")
REPLAY_BINARY = Path("/skilleval/bin/sdo-replay")
OPERATOR_ROOT = Path("/opt/sdo/sdo/operator")


class FilePublisher:
    def publish_payload(self, name: str, key: str, payload: str) -> None:
        (OUT / key).write_text(payload, encoding="utf-8")


def _skills() -> tuple[str, ...]:
    names = [*AGENT_SKILLS["responder"], *AGENT_SKILLS["reflector"]]
    names += [name for name in os.environ.get("SKILLEVAL_EXTRA_SKILLS", "").split(",") if name]
    return tuple(dict.fromkeys(names))


def _run(argv: list[str], *, cwd: Path, timeout: float, env: dict[str, str] | None = None) -> dict[str, object]:
    started = time.monotonic()
    try:
        completed = subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=timeout, check=False, env=env)
    except subprocess.TimeoutExpired as exc:
        return {"argv": argv, "exit_code": None, "timed_out": True, "stdout": str(exc.stdout or "")[-20000:]}
    return {
        "argv": argv,
        "exit_code": completed.returncode,
        "timed_out": False,
        "seconds": round(time.monotonic() - started, 1),
        "stdout": completed.stdout[-60000:],
        "stderr": completed.stderr[-20000:],
    }


def _incident_cases(checkout: Path, incident_id: str, destination: Path) -> list[str]:
    """Copy the corpus cases captured from this incident into a replay fixtures directory."""

    copied: list[str] = []
    for labels in sorted((checkout / ".sdo" / "corpus").glob("*/labels.json")):
        try:
            provenance = json.loads(labels.read_text(encoding="utf-8")).get("provenance") or {}
        except (OSError, ValueError):
            continue
        if provenance.get("incident_id") == incident_id:
            shutil.copytree(labels.parent, destination / labels.parent.name)
            copied.append(labels.parent.name)
    return copied


def _reflect() -> dict[str, object]:
    codex_home = Path(os.environ["CODEX_HOME"])
    shutil.rmtree(codex_home / "skills", ignore_errors=True)
    # The evaluated skills are mounted over the image's skills root, which this installs from.
    prepare_codex_home(skills=_skills())
    started = time.monotonic()
    result = run_reflection_task(TASK, "skilleval", FilePublisher())
    return {
        "status": result.status,
        "failure_reason": result.failure_reason,
        "error_summary": result.error_summary,
        "reflection_seconds": round(time.monotonic() - started, 1),
    }


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    task = json.loads(TASK.read_text(encoding="utf-8"))
    checkout = Path(task["workspace"]["checkout_path"])
    previous = OUT / "entry-summary.json"
    if os.environ.get("SKILLEVAL_POST_ONLY") == "1":
        # Re-measure a finished run without another model turn.
        summary: dict[str, object] = json.loads(previous.read_text(encoding="utf-8"))
    else:
        summary = _reflect()
    payload = json.loads((OUT / "agent-result.json").read_text(encoding="utf-8"))
    decision = dict(payload.get("response") or {})
    decision.pop("@type", None)
    if decision:
        decision_path = OUT / "decision.json"
        decision_path.write_text(json.dumps(decision), encoding="utf-8")
        # The same environment the worker's own pre-submission check runs with.
        environment = {**os.environ, "SDO_INCIDENT_ID": task["request"]["outcome"]["incident_id"]}
        summary["memory_check"] = _run(
            ["sdo-operator", "memory", "check", "--json", "--decision", str(decision_path)],
            cwd=checkout,
            timeout=600,
            env=environment,
        )
    fixtures = OUT / "replay-fixtures"
    shutil.rmtree(fixtures, ignore_errors=True)
    fixtures.mkdir()
    cases = _incident_cases(checkout, task["request"]["outcome"]["incident_id"], fixtures)
    summary["replay_cases"] = cases
    if cases and REPLAY_BINARY.is_file():
        summary["replay"] = _run(
            [
                str(REPLAY_BINARY),
                "-sdo",
                str(checkout / ".sdo"),
                "-operator",
                str(OPERATOR_ROOT),
                "-fixtures",
                str(fixtures),
            ],
            cwd=checkout,
            timeout=900,
        )
    (OUT / "entry-summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in summary.items() if key in ("status", "failure_reason")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
