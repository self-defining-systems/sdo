"""Run one RH3 slot through the existing stream runner, after refreshed seeds are ready.

--dry-run prints the directive and runner arguments without contacting a cluster.
Real runs are Phase B only. They require the responder-lanes lease and shared
lane lock, gate host load, reset to seed, and preserve evidence before cleanup.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import time
from pathlib import Path

from benchmarks.sregym.adapter import SCRIPTED_REPLAY_SLOTS as SLOTS
from benchmarks.sregym.adapter import control_namespace_for
from benchmarks.sregym.assurance.cli import require_scripted
from benchmarks.sregym.assurance.scripted_codex.mixed10 import directive_for
from benchmarks.sregym.fastloop import cli
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.records import load_records
from benchmarks.sregym.protocol.native_receipt import validate_native_production_receipt

ROOT = Path(__file__).resolve().parents[3]


def runner_arguments(slot: int, run_dir: Path, output: Path) -> list[str]:
    directive_for(slot)  # Validate even for dry runs.
    arguments = [
        "run",
        "--run-dir",
        str(run_dir),
        "--agent",
        "sdo",
        "--stream",
        str(output / "stream.json"),
        "--agent-registrations",
        str(output / "registrations"),
        "--slot-offset",
        str(slot),
        "--run-id",
        output.name,
        "--program-fast-path",
        "--program-trust-threshold",
        "1",
        "--repair-policy",
        "recorded-actions",
        "--inject-before-resume",
        "--require-full-cleanup",
        "--cleanup-passes",
        "2" if slot in (1, 4) else "1",
        "--require-faulty-oracle",
        "--healthy-baseline",
        "--timeout",
        "3600",
        "--composite-deadline",
        "3600",
        "--health-timeout",
        "600",
        "--diagnosis-grading",
        "off",
    ]

    if slot == 6:
        arguments.extend(["--require-faulty-component", "fault_1:ValkeyMemoryDisruption"])
    return arguments


def prepare(slot: int, output: Path) -> None:
    directive_for(slot)
    output.mkdir(parents=True, exist_ok=False)
    (output / "directive.json").write_text(directive_for(slot).to_json() + "\n", encoding="utf-8")
    (output / "stream.json").write_text(
        json.dumps(
            {
                "incidents": [SLOTS[slot].problem_id],
                "diagnosis_grading": "off",
                "capability_mode": "permissive",
                "permissive_responder": True,
            }
        ),
        encoding="utf-8",
    )
    registrations = output / "registrations"
    registrations.mkdir()
    for role in ("responder", "reflector", "health-judge"):
        payload = json.loads((ROOT / "benchmarks/sregym/adapter/scripted_registrations" / f"{role}.json").read_text())
        payload["kubernetes_job"]["command"].insert(1, f"SDO_SCRIPTED_REPLAY_SLOT={slot}")
        (registrations / f"{role}.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def check_lease(path: Path, cluster: str) -> None:
    for line in path.read_text(encoding="utf-8").splitlines():
        fields = line.split("\t")
        if (
            len(fields) >= 5
            and fields[0] == "responder-lanes"
            and int(fields[3]) > time.time()
            and cluster in fields[4].split(",")
        ):
            return
    raise ValueError(f"active responder-lanes lease does not cover {cluster}")


def prepare_scripted_control(environment: FastloopEnvironment) -> None:
    """Create fresh-lane prerequisites for already-validated model-free images.

    Existing Secrets are left untouched. Creation needs no Secret data read and
    an empty Secret satisfies the runtime's required credentials volume.
    """
    namespace = control_namespace_for(environment.namespace)
    resources = [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}},
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "Opaque",
            "metadata": {"name": "sdo-agent-credentials", "namespace": namespace},
        },
    ]
    for resource in resources:
        result = subprocess.run(
            ["kubectl", "--kubeconfig", str(environment.kubeconfig), "create", "-f", "-"],
            input=json.dumps(resource),
            text=True,
            capture_output=True,
            check=False,
            timeout=60,
        )
        if result.returncode and "(AlreadyExists)" not in result.stderr:
            raise RuntimeError(f"cannot prepare scripted {resource['kind']}/{resource['metadata']['name']}")


def verify_results(output: Path, environment: FastloopEnvironment) -> None:
    records = load_records(output / "incidents.jsonl")
    if len(records) != 1 or records[0].error or not records[0].full_cleanup_verified:
        raise ValueError("slot failed or full seed recovery was not verified; inspect incidents.jsonl")
    if records[0].index in (1, 4) and records[0].cleanup_passes_verified != 2:
        raise ValueError("credential slots require two verified full cleanup passes")
    if records[0].oracle is None or not records[0].oracle.success:
        raise ValueError("official mitigation oracle did not pass")
    receipts = list(output.rglob("sdo_production_receipt_strict.json"))
    if not receipts:
        raise ValueError("no strict native incident receipt was produced")
    for path in receipts:
        receipt = json.loads(path.read_text())
        validate_native_production_receipt(receipt)
        ledger = receipt.get("native_ledger") or {}
        if (
            not receipt.get("proposal_commit")
            or not receipt.get("outcome_commit")
            or not ledger.get("outcome_document")
        ):
            raise ValueError("strict receipt has no broker outcome")
        ancestry = subprocess.run(
            ["git", "merge-base", "--is-ancestor", receipt["proposal_commit"], "HEAD"],
            cwd=environment.workspace,
            capture_output=True,
            check=False,
        )
        if ancestry.returncode:
            raise ValueError("local accepted checkout does not contain the broker source repair")
    lock = json.loads((environment.workspace / ".sdo/rendered/lock.json").read_text())
    if not lock.get("objects") or any(item.get("error") or not item.get("file") for item in lock["objects"]):
        raise ValueError("accepted rendered lock has evidence gaps after source repair")
    source_commit = lock.get("source_commit")
    if not isinstance(source_commit, str) or not source_commit:
        raise ValueError("rendered lock is not bound to source repair evidence")
    continuity = subprocess.run(
        ["git", "diff", "--quiet", source_commit, "HEAD", "--", ".", ":(exclude).sdo"],
        cwd=environment.workspace,
        capture_output=True,
        check=False,
    )
    if continuity.returncode:
        raise ValueError("rendered lock predates the accepted source repair")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slot", required=True, type=int, choices=range(10))
    parser.add_argument("--run-dir", required=True, type=Path, help="refreshed seed fastloop environment")
    parser.add_argument("--lane-lock", required=True, type=Path, help="shared lane52.lock or lane53.lock")
    parser.add_argument("--coord", type=Path, default=Path("/mnt/data/shli/coord/coord"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    run_dir = args.run_dir.resolve()
    output = run_dir / "results" / f"rh3-slot-{args.slot}-{time.time_ns()}"
    runner = runner_arguments(args.slot, run_dir, output)
    if args.dry_run:
        print(json.dumps({"directive": json.loads(directive_for(args.slot).to_json()), "runner": runner}, indent=2))
        return 0
    environment = FastloopEnvironment.load(run_dir)
    slot = SLOTS[args.slot]
    expected_cluster = "luna-pilot-52" if slot.namespace == "astronomy-shop" else "luna-pilot-53"
    if environment.namespace != slot.namespace or environment.cluster != expected_cluster:
        raise ValueError("slot must use its refreshed seed namespace and designated lane")
    require_scripted(environment.images)
    with args.lane_lock.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        check_lease(args.coord.parent / "leases.tsv", environment.cluster)
        subprocess.run([str(args.coord), "gate"], check=True)
        os.environ["KUBECONFIG"] = str(environment.kubeconfig)
        prepare(args.slot, output)
        # Reset stops the old controller, so this slot installs the registrations
        # above. The final reset preserves results while restoring the seed.
        if cli.main(["reset", "--run-dir", str(run_dir)]):
            raise RuntimeError("initial reset to refreshed seed failed")
        try:
            prepare_scripted_control(environment)
            result = cli.main(runner)
            if result:
                return result
            verify_results(output, environment)
        finally:
            if cli.main(["reset", "--run-dir", str(run_dir)]):
                raise RuntimeError("final reset to refreshed seed failed")
    print(f"RH3 slot {args.slot} passed; evidence: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
