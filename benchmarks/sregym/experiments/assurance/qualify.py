"""No-agent fast-loop qualification of the assurance problems (the assurance plan section (e), first row).

For each problem and trial: the app is healthy, inject, the mitigation oracle fails,
recover, the app is healthy and the oracle passes. For a composite, after the
injection one fault component is recovered alone (a different one each trial);
the composite's oracle must still fail and every other fault component's oracle
must still fail. Whether the recovered component's own oracle passes is recorded
but not gated (a coupled component oracle can need the other fix too).

No LLM, no Codex, no controller: only the SREGym fast-loop worker on a warm lane
created by ``fastloop up``. Run from the repository root::

    PYTHONPATH=. uv run python benchmarks/sregym/experiments/assurance/qualify.py \\
        --run-dir <fastloop run dir> --trials 3 --out qualification.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.fault_driver import WORKER_OP_TIMEOUT_SECONDS, SregymFaultDriver
from benchmarks.sregym.fastloop.wipe import LaneInjectionState
from benchmarks.sregym.fastloop.worker_client import SregymWorker, worker_argv

PHASE_ONE = (
    "missing_configmap_hotel_reservation",
    "wrong_service_selector_hotel_reservation",
    "network_policy_block",
    "composite_policy_and_rate_configmap_hotel_reservation",
    "composite_frontend_selector_and_readiness_hotel_reservation",
)


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _components(verdict: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"name": part.get("name"), "success": bool(part.get("success"))}
        for part in verdict.get("details", {}).get("oracles", [])
    ]


def _wait_healthy(worker: SregymWorker, namespace: str, timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        health = worker.request("health", namespace=namespace, timeout=60)
        if health.get("healthy") or time.monotonic() >= deadline:
            return health
        time.sleep(2.0)


def qualify(
    worker: SregymWorker, driver: SregymFaultDriver, namespace: str, problem_id: str, trial: int
) -> dict[str, Any]:
    record: dict[str, Any] = {"problem_id": problem_id, "trial": trial, "started_at": _now(), "checks": {}}
    checks: dict[str, bool] = record["checks"]
    injected = False
    try:
        before = _wait_healthy(worker, namespace, 300)
        checks["healthy before inject"] = bool(before.get("healthy"))
        began = time.monotonic()
        reply = worker.request("inject", problem_id=problem_id, timeout=WORKER_OP_TIMEOUT_SECONDS)
        injected = True
        record["inject_seconds"] = round(time.monotonic() - began, 1)
        faults = list(reply.get("faults", [reply.get("fault", 0)]))
        record["faults"] = faults
        on_fault = driver.oracle().model_dump()
        record["oracle_on_fault"] = {"success": on_fault["success"], "components": _components(on_fault)}
        checks["oracle fails on the fault"] = on_fault["success"] is False
        if len(faults) > 1:
            first = faults[(trial - 1) % len(faults)]
            worker.request("recover", fault=first, timeout=WORKER_OP_TIMEOUT_SECONDS)
            whole = driver.oracle().model_dump()
            parts = {fault: driver.oracle(fault=fault).success for fault in faults}
            record["partial"] = {
                "recovered_fault": first,
                "composite_success": whole["success"],
                "components": _components(whole),
                "fault_oracles": parts,
            }
            checks["composite oracle fails after one component is recovered"] = whole["success"] is False
            # Recorded, not gated: K2's selector oracle probes the Service's endpoints, which stay
            # empty while the readiness probe is still broken, so its components are coupled.
            record["partial"]["recovered_component_passes"] = parts[first] is True
            checks["every other component's oracle still fails"] = all(
                success is False for fault, success in parts.items() if fault != first
            )
        record["recover_seconds"] = round(driver.recover(), 1)
        injected = False
        after = driver.oracle().model_dump()
        record["oracle_after_recovery"] = {"success": after["success"], "components": _components(after)}
        checks["oracle passes after recovery"] = after["success"] is True
    except Exception as error:  # recorded; the next trial still runs
        record["error"] = f"{type(error).__name__}: {error}"
        if injected:
            try:
                driver.recover()
            except Exception as recover_error:
                record["error"] += f"; recovery failed: {recover_error}"
    record["finished_at"] = _now()
    record["passed"] = "error" not in record and all(checks.values())
    return record


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--problem", action="append", help="problem id (repeat); default: the phase-1 set")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--out", type=Path, required=True, help="JSON lines, one record per trial (appended)")
    args = parser.parse_args(argv)
    if args.trials < 1:
        raise SystemExit("--trials must be positive")
    environment = FastloopEnvironment.load(args.run_dir.resolve())
    os.environ["KUBECONFIG"] = str(environment.kubeconfig)
    problems = args.problem or list(PHASE_ONE)
    worker = SregymWorker(
        worker_argv(environment.sregym_dir, private_tmp=environment.private_tmp),
        env=environment.worker_process_env(),
        log_path=environment.run_dir / "qualify-worker.log",
    )
    failed = 0
    with worker:
        worker.request(
            "deploy",
            problem_id=environment.deploy_problem,
            baseline_path=str(environment.baseline_path),
            timeout=2400,
        )
        driver = SregymFaultDriver(
            worker,
            namespace=environment.namespace,
            health_timeout_seconds=600,
            injection_state=LaneInjectionState(environment.kubeconfig, environment.namespace),
        )
        for problem_id in problems:
            for trial in range(1, args.trials + 1):
                record = qualify(worker, driver, environment.namespace, problem_id, trial)
                with args.out.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(record) + "\n")
                failed += not record["passed"]
                verdict = "PASS" if record["passed"] else "FAIL"
                print(f"[{verdict}] {problem_id} #{trial} {record.get('error', '')}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
