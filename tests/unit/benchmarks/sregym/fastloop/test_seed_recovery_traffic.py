from __future__ import annotations

import json
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
    from benchmarks.sregym import adapter

    api = Mock(return_value=SimpleNamespace(stdout=""))
    monkeypatch.setattr(adapter, "kubectl", api)
    monkeypatch.setenv("KUBECONFIG", "/fixture/kubeconfig")
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
    assert json.loads(api.call_args.kwargs["input_text"])["metadata"]["name"] == "sregym"


def test_failed_baseline_never_probes_or_credits_cleanup():
    original = {"kind": "health-check", "success": False, "details": {"unready": ["api"]}}
    assert seed_recovery.verify_baseline_traffic(object(), original) is original


@pytest.mark.parametrize("settled", [True, False])
def test_hotel_client_refresh_is_scoped_and_reports_bounded_rollout(monkeypatch, settled):
    kubectl = Mock()
    runner = Mock()
    runner.wait_for_rollouts.return_value = SimpleNamespace(
        settled=settled, waited_s=3, reason="" if settled else "still rolling"
    )
    monkeypatch.setattr(
        seed_recovery.importlib, "import_module", lambda _name: SimpleNamespace(KubeClusterIO=lambda: runner)
    )
    problem = SimpleNamespace(
        app=SimpleNamespace(name="Hotel Reservation"), namespace="hotel-reservation", kubectl=kubectl
    )
    from benchmarks.sregym import adapter

    items = []
    for name in ("consul", *seed_recovery.RESTORE_CLIENTS["hotel-reservation"]):
        items.extend(
            [
                {
                    "kind": "Deployment",
                    "metadata": {"name": name},
                    "spec": {"selector": {"matchLabels": {"app": name}}},
                },
                {
                    "kind": "Pod",
                    "metadata": {
                        "name": name + "-pod",
                        "labels": {"app": name},
                        "creationTimestamp": "2026-10-25T20:00:00Z" if name == "consul" else "2026-10-25T20:01:00Z",
                    },
                },
            ]
        )
    monkeypatch.setattr(adapter, "kubectl", lambda *a, **kw: SimpleNamespace(stdout=json.dumps({"items": items})))
    monkeypatch.setenv("KUBECONFIG", "/fixture/kubeconfig")
    result = seed_recovery.refresh_baseline_clients(problem)
    assert result["success"] is settled
    command = kubectl.exec_command.call_args.args[0]
    assert "deployment/geo" in command
    assert "deployment/search" in command
    assert "deployment/consul" not in command
    assert "deployment/mongodb" not in command
    assert "secret" not in command
    assert "delete" not in command
    runner.wait_for_rollouts.assert_called_once_with("hotel-reservation", timeout_s=90)


def test_unknown_application_cannot_claim_a_client_refresh():
    problem = SimpleNamespace(app=SimpleNamespace(name="Unknown"), namespace="sample", kubectl=Mock())
    result = seed_recovery.refresh_baseline_clients(problem)
    assert result["success"] is False
    problem.kubectl.exec_command.assert_not_called()


@pytest.mark.parametrize("namespace", ["astronomy-shop", "social-network"])
def test_registration_refresh_does_not_roll_apps_without_startup_dependencies(monkeypatch, namespace):
    from benchmarks.sregym import adapter

    items = [{"kind": "Deployment", "metadata": {"name": name}} for name in seed_recovery.RESTORE_CLIENTS[namespace]]
    monkeypatch.setattr(adapter, "kubectl", lambda *a, **kw: SimpleNamespace(stdout=json.dumps({"items": items})))
    monkeypatch.setenv("KUBECONFIG", "/fixture/kubeconfig")
    problem = SimpleNamespace(namespace=namespace, kubectl=Mock())
    result = seed_recovery.refresh_baseline_clients(problem, dependencies_only=True)
    assert result["success"] is True
    assert result["deployments"] == []
    problem.kubectl.exec_command.assert_not_called()
