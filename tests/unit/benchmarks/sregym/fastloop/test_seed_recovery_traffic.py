from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from benchmarks.sregym.fastloop import seed_recovery


@pytest.mark.parametrize(
    ("phase", "ok", "complete"),
    [
        ("Succeeded", True, True),
        ("Succeeded", False, True),
        ("Failed", True, True),
        ("Pending", True, True),
        ("Succeeded", True, False),
    ],
)
def test_seed_recovery_requires_complete_successful_independent_traffic(monkeypatch, phase, ok, complete):
    runner = Mock()
    runner.run_pod.return_value = phase, "synthetic probe log"
    entry = object()
    grading = SimpleNamespace(
        ENTRY_POINTS={"Example": entry},
        probe_pod_manifest=Mock(return_value={"metadata": {"name": "probe"}}),
        probe_deadline_s=Mock(return_value=90),
        parse_probe_log=Mock(
            return_value=SimpleNamespace(ok=ok, complete=complete, detail=lambda: "synthetic HTTP counts")
        ),
    )
    cluster = SimpleNamespace(KubeClusterIO=lambda: runner)
    monkeypatch.setattr(
        seed_recovery.importlib, "import_module", lambda name: grading if name.endswith(".grading") else cluster
    )
    problem = SimpleNamespace(app=SimpleNamespace(name="Example"), namespace="sample")
    original = {"kind": "sregym-mitigation-oracle", "success": True, "details": {"seed": True}}
    result = seed_recovery.verify_baseline_traffic(problem, original)
    assert result["success"] is (phase == "Succeeded" and ok and complete)
    assert result["kind"] == "health-check"
    assert result["details"]["baseline_oracle"] is original
    runner.run_pod.assert_called_once_with({"metadata": {"name": "probe"}}, timeout_s=120)


def test_failed_baseline_never_probes_or_credits_cleanup():
    original = {"kind": "health-check", "success": False, "details": {"unready": ["api"]}}
    assert seed_recovery.verify_baseline_traffic(object(), original) is original
