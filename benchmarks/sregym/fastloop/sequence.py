"""Thin, restartable row journal around the existing guarded fastloop runner.

A durable native result can be adopted after interruption. An incomplete result
is held for native recovery; restarting this command never injects it again.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.parallel_memory import memory_ancestry, memory_head
from benchmarks.sregym.fastloop.records import load_records

if TYPE_CHECKING:
    from collections.abc import Callable


class SequenceRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    slot: int = Field(ge=0)
    problem_id: str = Field(min_length=1)
    encounter: Literal["first", "repeat"]


class SequencePlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$")
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    code_root: Path
    environment_dir: Path
    journal_dir: Path
    lane: str = Field(min_length=1)
    rows: list[SequenceRow] = Field(min_length=2)
    lease: str = "le-hu"
    coord: str = "/mnt/data/shli/coord/coord"
    timeout_seconds: int = Field(default=int(wait_for(WaitKind.RESPONSE)), gt=0)
    # Retained as experiment provenance. Automatic program trust gating was
    # removed; this value grants no execution or publication authority.
    program_trust_threshold: Literal[3] = 3

    @model_validator(mode="after")
    def pairs(self) -> Self:
        if len(self.rows) % 2:
            raise ValueError("each problem requires adjacent first + exact repeat rows")
        seen = set()
        for first, repeat in zip(self.rows[::2], self.rows[1::2], strict=True):
            if (
                (first.encounter, repeat.encounter) != ("first", "repeat")
                or (first.slot, first.problem_id) != (repeat.slot, repeat.problem_id)
                or first.problem_id in seen
            ):
                raise ValueError("each unique problem requires adjacent first + exact repeat rows")
            seen.add(first.problem_id)
        return self


def _save(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as handle:
        handle.write(json.dumps(value, indent=2) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def execute_row(plan: SequencePlan, index: int, results: Path) -> None:
    """Use fastloop's production admission, model, learning and cleanup paths."""
    environment = FastloopEnvironment.load(plan.environment_dir)
    command = [
        sys.executable,
        "-m",
        "benchmarks.sregym.fastloop.lane_guard",
        "--cluster",
        environment.cluster,
        "--kubeconfig",
        str(environment.kubeconfig),
        "--lease",
        plan.lease,
        "--coord",
        plan.coord,
        "--",
        "benchmarks.sregym.fastloop",
        "run",
        "--run-dir",
        str(plan.environment_dir),
        "--run-id",
        results.parent.name,
        "--stream",
        str(plan.journal_dir / f"{index:03d}" / "stream.json"),
        "--slot-offset",
        str(index),
        "--agent",
        "sdo",
        "--provider",
        "codex",
        "--model",
        "gpt-6-luna",
        "--repair-policy",
        "recorded-actions",
        "--reflection-session",
        "resume",
        "--timeout",
        str(plan.timeout_seconds),
        "--detection-timeout",
        "300",
        "--healthy-baseline",
        "--inject-before-resume",
        "--require-fault-tracking",
        "--require-full-cleanup",
        "--cleanup-passes",
        "2",
        "--continue-on-error",
    ]
    directory = plan.journal_dir / f"{index:03d}"
    _save(directory / "command.json", command)
    # Keep transient load admission from turning an unstarted row into an
    # uncertain interruption. Lease loss still stops immediately.
    subprocess.run([plan.coord, "check", environment.cluster, "--lease", plan.lease], check=True)
    while os.getloadavg()[0] >= 40:
        subprocess.run([plan.coord, "check", environment.cluster, "--lease", plan.lease], check=True)
        time.sleep(wait_for(WaitKind.PROBE))
    subprocess.run([plan.coord, "gate", "--lease", plan.lease], check=True)
    subprocess.run([plan.coord, "check", environment.cluster, "--lease", plan.lease], check=True)
    with (directory / "stdout.log").open("a") as out, (directory / "stderr.log").open("a") as error:
        result = subprocess.run(command, cwd=plan.code_root, stdout=out, stderr=error, check=False)
    _save(directory / "process-exit.json", {"returncode": result.returncode, "finished_at": _now()})
    if result.returncode:
        if results.exists():
            _terminal(plan, index, results)  # Propagate a proven never-injected retry.
        raise RuntimeError(f"eval: row {index} exited {result.returncode}; adopt its native evidence before resuming")


class NeverInjectedRetry(RuntimeError):
    """Durable pre-injection evidence permits a fresh row attempt, never a hold."""


def _terminal(plan: SequencePlan, index: int, results: Path) -> dict[str, object]:
    if not results.exists():
        raise RuntimeError(f"eval: row {index} incomplete; recover native work, never reinject an uncertain row")
    records = load_records(results)
    if len(records) != 1:
        raise RuntimeError(f"eval: row {index} requires exactly one correlated native result")
    record = records[0]
    if (record.run_id, record.index, record.problem_id, record.agent, record.model) != (
        results.parent.name,
        index,
        plan.rows[index].problem_id,
        "sdo",
        "gpt-6-luna",
    ):
        raise RuntimeError(f"eval: row {index} native result identity differs")
    if (
        record.injection.state == "not_injected"
        and record.injection.stage == "pre_injection"
        and record.infra_error == "never_injected_retry"
    ):
        raise NeverInjectedRetry(f"eval: row {index} never_injected_retry: baseline observation unavailable")
    if (
        record.injection.state != "injected"
        or not record.incident_id
        or record.infra_error
        or (
            record.cleanup_skipped_reason != "row_start_tier2"
            and (
                record.recovery_error
                or not record.recovery_finished_at
                or not record.recovery_oracle
                or not record.recovery_oracle.success
                or not record.full_cleanup_verified
                or record.cleanup_passes_verified != 2
            )
        )
        or record.reflection_finished_at is None
        or "learning failed:" in (record.error or "")
    ):
        raise RuntimeError(f"eval: row {index} did not pass recovery and learning; hold before the next injection")
    receipt_path = Path(record.artifacts_dir or "") / "sdo_production_receipt_strict.json"
    receipt = json.loads(receipt_path.read_text())
    ledger = receipt.get("native_ledger", {})
    if (
        receipt.get("incident_id") != record.incident_id
        or ledger.get("incident_id") != record.incident_id
        or ledger.get("acknowledged") is not True
        or not ledger.get("outcome_commit")
    ):
        raise RuntimeError(f"eval: row {index} lacks an acknowledged correlated native outcome")
    return {
        "incident_id": record.incident_id,
        "native_result": str(results),
        "receipt": str(receipt_path),
        "outcome_commit": ledger["outcome_commit"],
        "oracle": record.oracle.model_dump() if record.oracle else None,
    }


def run_sequence(plan: SequencePlan, *, execute: Callable[[SequencePlan, int, Path], None] = execute_row) -> None:
    """Skip finished rows and adopt native results left before a journal commit."""
    plan.journal_dir.mkdir(parents=True, exist_ok=True)
    with (plan.journal_dir / ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        identity = plan.model_dump(mode="json")
        saved = plan.journal_dir / "sequence.json"
        if saved.exists() and json.loads(saved.read_text()) != identity:
            raise ValueError("eval: sequence identity changed; use a new run directory")
        head = memory_head(plan.code_root)
        if head != plan.code_sha:
            raise ValueError(f"eval: code HEAD {head} differs from frozen {plan.code_sha}")
        _save(saved, identity)
        environment = FastloopEnvironment.load(plan.environment_dir)
        if environment.shared_accepted_id:
            raise ValueError("eval: this sequence requires private per-lane memory")
        for index, row in enumerate(plan.rows):
            directory = plan.journal_dir / f"{index:03d}"
            finished = directory / "finished.json"
            if finished.exists():
                old = json.loads(finished.read_text())
                memory_ancestry(environment.workspace, old["accepted_after"], memory_head(environment.workspace))
                continue
            manifest = directory / "manifest.json"
            retry_path = directory / "retry.json"
            attempt = json.loads(retry_path.read_text())["next_attempt"] if retry_path.exists() else 0
            suffix = f"-retry-{attempt}" if attempt else ""
            results = plan.environment_dir / "results" / f"{plan.run_id}-{index:03d}{suffix}" / "incidents.jsonl"
            attempt_manifest = directory / f"attempt-{attempt}.json"
            execute_needed = not manifest.exists() or (attempt > 0 and not attempt_manifest.exists())
            if not manifest.exists():
                if results.parent.exists():
                    raise RuntimeError(f"eval: row {index} has unjournaled results; never overwrite native evidence")
                if memory_head(plan.code_root) != plan.code_sha:
                    raise ValueError("eval: source changed during sequence")
                _save(
                    directory / "stream.json",
                    {
                        "incidents": [row.problem_id],
                        "diagnosis_grading": "deferred",
                        "judge_model_id": "codex-gpt-6-luna",
                        "capability_mode": "permissive",
                        "permissive_responder": True,
                    },
                )
                _save(
                    manifest,
                    {
                        **row.model_dump(),
                        "index": index,
                        "lane": plan.lane,
                        "code_sha": plan.code_sha,
                        "seed_commit": environment.seed_commit,
                        "accepted_before": memory_head(environment.workspace),
                        "started_at": _now(),
                        "program_trust_threshold": plan.program_trust_threshold,
                        "program_trust_threshold_status": "retired; provenance only",
                    },
                )
            try:
                if execute_needed:
                    _save(attempt_manifest, {"attempt": attempt, "native_result": str(results), "started_at": _now()})
                    execute(plan, index, results)
                terminal = _terminal(plan, index, results)
            except NeverInjectedRetry:
                _save(
                    directory / f"never-injected-{attempt}.json",
                    {
                        "status": "never_injected_retry",
                        "native_result": str(results),
                        "attempt": attempt,
                    },
                )
                _save(retry_path, {"next_attempt": attempt + 1})
                raise
            start = json.loads(manifest.read_text())
            after = memory_head(environment.workspace)
            lineage = memory_ancestry(environment.workspace, start["accepted_before"], after)
            _save(
                finished,
                {**start, **terminal, "accepted_after": after, "memory_ancestry": lineage, "finished_at": _now()},
            )
        _save(
            plan.journal_dir / "complete.json",
            {"rows": len(plan.rows), "code_sha": plan.code_sha, "accepted_after": memory_head(environment.workspace)},
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    args = parser.parse_args()
    run_sequence(SequencePlan.model_validate_json(args.plan.read_text()))


if __name__ == "__main__":
    main()
