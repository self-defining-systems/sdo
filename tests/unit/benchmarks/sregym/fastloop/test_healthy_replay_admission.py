"""The production controller's capture prerequisite feeds the real admission predicate."""

import json
import subprocess
from pathlib import Path

from benchmarks.sregym.adapter.eval_guards import controller_proof


def test_real_controller_capture_and_restart_gate_admission(tmp_path):
    root = Path(__file__).resolve().parents[5]
    binary = tmp_path / "readiness"
    subprocess.run(
        ["go", "build", "-o", str(binary), str(root / "tests/fixtures/sregym/replay_readiness/main.go")],
        cwd=root / "sdo/operator/runtime",
        check=True,
        capture_output=True,
        text=True,
    )
    result = subprocess.run([str(binary)], check=True, capture_output=True, text=True, timeout=5)
    states = json.loads(result.stdout)
    for phase, state in states.items():
        proof = controller_proof(state, {"health"}, "2026-01-01T00:00:00Z")
        assert proof["passed"] == (phase in {"healthy", "requalified"}), (phase, proof)
    # Clear observations survive a restart, but the process-local capture does not.
    assert states["restart"]["history"] == states["healthy"]["history"]
    for value in (None, False, "true", 1):
        state = dict(states["healthy"], healthy_replay_ready=value)
        assert not controller_proof(state, {"health"}, "2026-01-01T00:00:00Z")["passed"]
