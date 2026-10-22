"""Replay one fresh-session reflection turn offline from a saved pipeline workspace.

A stage's ``application_workspace`` keeps the operational-memory repository,
and the stage's strict receipts (``runs/**/sdo_production_receipt_strict.json``)
carry each incident's ``native_ledger``: the verified closure and the
authoritative outcome commit. This tool clones
the repository at that outcome commit and runs the production reflection code
path (``SessionReflector`` with a fresh-session brief) under a chosen guidance,
so prompt wording can be compared without a cluster. The clone is a scratch
copy; the saved workspace is never modified.

    uv run python -m benchmarks.sregym.analysis.reflection_replay \\
        --workspace STAGE/application_workspace --incident-suffix 35102207 \\
        --guidance generalize --out /tmp/replay
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from google.protobuf import json_format

from benchmarks.sregym.analysis.incident_cost import RECEIPT_NAME
from benchmarks.sregym.memory_cli import read_outcomes
from libs.agent_cli.structured import run_structured_turn, turn_usage
from sdo.agents.responder import (
    INCIDENT_REASONING_EFFORT,
    REFLECTION_GUIDANCE_MODES,
    CodexSessionBackend,
    ReflectionTurn,
    SessionReflector,
    session_output_schema,
    unwrap_branch,
)
from sdo.contracts import IncidentClosure


class _ScopedBackend(CodexSessionBackend):
    """Reflection backend confined to the scratch clone (workspace-write) instead of full access."""

    def _turn(self, *, worktree: Path, prompt: str, idempotency_key: str, session_id: str | None) -> ReflectionTurn:
        turn = run_structured_turn(
            self.provider,
            f"Idempotency key: {idempotency_key}\n\n{prompt}",
            output_schema=session_output_schema(),
            cwd=worktree,
            access="workspace-write",
            model=self.model,
            reasoning_effort=self.reasoning_effort,
            timeout_seconds=self.timeout_seconds,
            resume_session_id=session_id,
            executor=self.executor,
        )
        return ReflectionTurn.model_validate(unwrap_branch(turn.output_json, "reflection")).with_usage(turn_usage(turn))


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True).stdout


def find_ledger(stage: Path, suffix: str) -> dict[str, Any]:
    """The ``native_ledger`` of the incident ending in *suffix*, from the stage's strict receipts."""

    for path in sorted((stage / "runs").rglob(RECEIPT_NAME)):
        receipt = json.loads(path.read_text(encoding="utf-8"))
        ledger = receipt.get("native_ledger") if isinstance(receipt, dict) else None
        if isinstance(ledger, dict) and str(ledger.get("incident_id", "")).endswith(suffix):
            return ledger
    raise SystemExit(f"no native ledger ending in {suffix!r} under {stage}")


def memory_delta(clone: Path) -> dict[str, list[str]]:
    """Incident detectors and playbooks the reflection added or modified, relative to the outcome commit."""

    added: list[str] = []
    modified: list[str] = []
    for line in _git(clone, "status", "--porcelain", "--untracked-files=all").splitlines():
        status, path = line[:2].strip(), line[3:]
        (added if status in {"??", "A"} else modified).append(path)

    def kinds(paths: list[str], marker: str) -> list[str]:
        return sorted({p.split(marker, 1)[1].split("/", 1)[0] for p in paths if marker in p})

    return {
        "new_detectors": kinds(added, "detectors/incidents/"),
        "modified_detectors": kinds(modified, "detectors/incidents/"),
        "new_playbooks": kinds(added, ".sdo/playbooks/"),
        "modified_playbooks": kinds(modified, ".sdo/playbooks/"),
        "changed_paths": sorted(added + modified),
    }


def replay(workspace: Path, suffix: str, guidance: str, out: Path, *, model: str, timeout: int) -> dict[str, Any]:
    ledger = find_ledger(workspace.parent, suffix)
    out.mkdir(parents=True, exist_ok=True)
    clone = out / f"clone-{guidance}"
    if clone.exists():
        raise SystemExit(f"{clone} already exists")
    _git(out, "clone", "--quiet", "--no-hardlinks", str(workspace), str(clone))
    _git(clone, "checkout", "--quiet", "--detach", ledger["outcome_commit"])
    records = read_outcomes(clone)
    outcome = next(r for r in reversed(records) if r.incident_id == ledger["incident_id"])
    closure = json_format.ParseDict(ledger["closure"], IncidentClosure(), ignore_unknown_fields=True)
    reflector = SessionReflector(
        _ScopedBackend(model=model, reasoning_effort=INCIDENT_REASONING_EFFORT, timeout_seconds=timeout),
        guidance=guidance,  # type: ignore[arg-type]
    )
    turn = reflector.resume(
        session_id=closure.result.responder_session_id if closure.HasField("result") else "",
        incident_id=outcome.incident_id,
        worktree=clone,
        outcome=outcome,
        history=records,
        outcome_commit=ledger.outcome_commit,
        session_mode="fresh",
        closure=closure,
    )
    result = {
        "guidance": guidance,
        "incident_id": outcome.incident_id,
        "learning_decision": turn.learning_decision,
        "summary": turn.summary,
        "no_change_reason": turn.no_change_reason,
        "playbook_ids": turn.playbook_ids,
        "usage": turn.usage,
        "delta": memory_delta(clone),
    }
    (out / f"result-{guidance}.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    (out / f"diff-{guidance}.patch").write_text(_git(clone, "diff", "HEAD"), encoding="utf-8")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--incident-suffix", required=True)
    parser.add_argument("--guidance", choices=REFLECTION_GUIDANCE_MODES, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--model", default="gpt-6-luna")
    parser.add_argument("--timeout", type=int, default=900)
    args = parser.parse_args(argv)
    result = replay(
        args.workspace, args.incident_suffix, args.guidance, args.out, model=args.model, timeout=args.timeout
    )
    json.dump(result, sys.stdout, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
