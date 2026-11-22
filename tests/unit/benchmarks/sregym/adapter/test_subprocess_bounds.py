"""No command of the benchmark code waits for ever: the count of unbounded subprocess calls only goes down.

A ``subprocess.run`` / ``check_output`` / ``check_call`` / ``call`` with no ``timeout`` waits as long as the command
does: a ``kubectl`` against a lane API server that stopped answering, a ``helm`` or ``docker`` call to a registry that
rate-limits, a ``git`` over ssh. On an EKS lane such a call holds a step to the platform's limit (hours) with nothing in
the log. Each file below has a ceiling (its count today); a new call needs ``timeout=wait_for(...)``, a file not listed
has none, and a ceiling that no longer matches is lowered in the change that fixed it.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
ROOTS = ("benchmarks/sregym", "benchmarks/lanes", "benchmarks/fuzzops")
CALLS = {"run", "check_output", "check_call", "call"}

#: file -> its unbounded calls. Only ever lowered. Most are kind- or toy-only or local ``git`` (see the EKS sweep).
CEILINGS: dict[str, int] = {
    "benchmarks/fuzzops/cellevidence.py": 1,
    "benchmarks/fuzzops/eks_faults.py": 1,
    "benchmarks/fuzzops/hooks.py": 3,
    "benchmarks/fuzzops/install.py": 6,
    "benchmarks/fuzzops/pack.py": 3,
    "benchmarks/fuzzops/sink.py": 2,
    "benchmarks/fuzzops/workload.py": 2,
    "benchmarks/sregym/agent_script/cli.py": 2,
    "benchmarks/sregym/agent_script/judge_example.py": 1,
    "benchmarks/sregym/agent_script/synthetic_health_judge.py": 1,
    "benchmarks/sregym/analysis/composite_sequence.py": 1,
    "benchmarks/sregym/analysis/reflection_artifacts.py": 1,
    "benchmarks/sregym/analysis/stream_curve.py": 2,
    "benchmarks/sregym/assurance/chaos.py": 2,
    "benchmarks/sregym/assurance/cli.py": 4,
    "benchmarks/sregym/assurance/harness.py": 1,
    "benchmarks/sregym/assurance/rehearse_batch.py": 1,
    "benchmarks/sregym/assurance/rehearse_harness.py": 3,
    "benchmarks/sregym/assurance/rehearse_real.py": 1,
    "benchmarks/sregym/detector_manifests.py": 2,
    "benchmarks/sregym/eval/fastloop_row.py": 2,
    "benchmarks/sregym/eval/snapshots.py": 4,
    "benchmarks/sregym/fastloop/admission.py": 1,
    "benchmarks/sregym/fastloop/app_retirement.py": 1,
    "benchmarks/sregym/fastloop/claude_worker_probe.py": 3,
    "benchmarks/sregym/fastloop/cli.py": 12,
    "benchmarks/sregym/fastloop/deployed_application.py": 2,
    "benchmarks/sregym/fastloop/dispatch.py": 3,
    "benchmarks/sregym/fastloop/gitlab_lane.py": 7,
    "benchmarks/sregym/fastloop/gitlab_rehearsal.py": 1,
    "benchmarks/sregym/fastloop/hotel_latent.py": 3,
    "benchmarks/sregym/fastloop/image_delivery.py": 6,
    "benchmarks/sregym/fastloop/parallel.py": 1,
    "benchmarks/sregym/fastloop/remote_memory.py": 5,
    "benchmarks/sregym/fastloop/reset_provenance.py": 1,
    "benchmarks/sregym/fastloop/sequence.py": 5,
    "benchmarks/sregym/fastloop/shared_memory.py": 1,
    "benchmarks/sregym/fastloop/toy.py": 10,
    "benchmarks/sregym/fastloop/toy_cni.py": 2,
    "benchmarks/sregym/fastloop/toy_images.py": 8,
    "benchmarks/sregym/fastloop/toy_reset.py": 1,
    "benchmarks/sregym/fastloop/toy_upstream.py": 4,
    "benchmarks/sregym/fastloop/wipe.py": 1,
    "benchmarks/sregym/pool/worker.py": 2,
    "benchmarks/sregym/runner/runner.py": 4,
    "benchmarks/sregym/skilleval/runner.py": 7,
    "benchmarks/sregym/skilleval/scenario.py": 1,
}


def unbounded(path: Path) -> list[int]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [
        node.lineno
        for node in ast.walk(tree)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "subprocess"
            and node.func.attr in CALLS
            and "timeout" not in {keyword.arg for keyword in node.keywords}
        )
    ]


def counts() -> dict[str, list[int]]:
    found: dict[str, list[int]] = {}
    for root in ROOTS:
        for path in sorted((ROOT / root).rglob("*.py")):
            if path.name.startswith("test_"):
                continue
            if lines := unbounded(path):
                found[path.relative_to(ROOT).as_posix()] = lines
    return found


def test_no_new_subprocess_call_waits_without_a_timeout() -> None:
    found = counts()
    grown = {file: lines for file, lines in found.items() if len(lines) > CEILINGS.get(file, 0)}
    assert not grown, "subprocess call(s) with no timeout (use timeout=wait_for(WaitKind...)):\n" + "\n".join(
        f"{file}: lines {lines}" for file, lines in sorted(grown.items())
    )


def test_a_fixed_call_lowers_its_ceiling() -> None:
    found = counts()
    stale = {
        file: (ceiling, len(found.get(file, [])))
        for file, ceiling in CEILINGS.items()
        if len(found.get(file, [])) < ceiling
    }
    assert not stale, f"lower these ceilings (file: ceiling, now): {stale}"
